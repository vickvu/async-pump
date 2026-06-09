/**
 * Does nothing. Handy as a `.then(noop, noop)` argument to deliberately
 * swallow a settled promise without an empty arrow showing up at each call site.
 */
export function noop() {
    // intentionally empty
}

/**
 * Resolves on the next macrotask, after the current microtask queue has drained.
 *
 * Used to break out of an otherwise-unbounded `await` (see the abort dance in
 * {@link AsyncIteratorWriter}): racing a pending promise against this one gives
 * any already-queued microtask continuations a chance to settle, then yields
 * control rather than hanging forever.
 *
 * `setImmediate` exists in Node.js but not in browsers, so we fall back to
 * `setTimeout(0)` where it is missing. `typeof` guards an undeclared global
 * safely (it evaluates to `'undefined'` rather than throwing), keeping this
 * usable in both runtimes.
 */
export function nextMacroTask(): Promise<void> {
    return new Promise<void>((resolve) => {
        if (typeof setImmediate === 'function') {
            setImmediate(resolve);
        } else {
            setTimeout(resolve, 0);
        }
    });
}

/**
 * Adapts an {@link AbortSignal} into a promise that rejects with the signal's
 * `reason` when (and if) it aborts, plus a `cleanup` to detach the listener.
 *
 * The returned promise never resolves — it only ever rejects on abort — so it
 * is meant to be `Promise.race`d against real work. Always call `cleanup()`
 * once the work settles to avoid leaking the abort listener.
 */
export function getAbortPromise(signal: AbortSignal): { promise: Promise<never>; cleanup: () => void } {
    let rejectOnAbort: (reason?: unknown) => void;
    const onAbort = () => {
        rejectOnAbort(signal.reason);
    };
    const abortPromise = new Promise<never>((_, reject) => {
        rejectOnAbort = reject;
    });
    signal.addEventListener('abort', onAbort, { once: true });
    return {
        promise: abortPromise,
        cleanup: function () {
            signal.removeEventListener('abort', onAbort);
        },
    };
}
