import { nextMacroTask, noop, getAbortPromise } from '#SRC/utils.ts';

/**
 * A transform applied to each item of a source iterable. Returning `null` or
 * `undefined` — directly or via the resolved promise — drops the item from the
 * output, so a single function can both map and filter.
 *
 * The current {@link AbortSignal} (if one was supplied) is passed as the second
 * argument so the transform can wire its own async work (a `fetch`, a timeout)
 * to the same cancellation.
 */
export type AsyncIteratorTransformFn<SRC = Uint8Array, DST = SRC> = (src: SRC, signal: AbortSignal | undefined) => DST | null | undefined | Promise<DST | null | undefined>;

export interface AsyncIteratorTransformerOptions<SRC = Uint8Array, DST = SRC> {
    /** The async iterable to transform. */
    source: AsyncIterable<SRC>;
    /**
     * Maps each source item to an output item, or `null`/`undefined` to skip it.
     * Omit it for an identity pass-through, which turns the transformer into an
     * abortable wrapper around the source.
     */
    transform?: AsyncIteratorTransformFn<SRC, DST>;
    /** Cancels the transform; pulls and transforms in flight reject with the abort reason. */
    signal?: AbortSignal;
}

/**
 * Lazily transforms an `AsyncIterable<SRC>` into an `AsyncIterable<DST>`, applying
 * a {@link AsyncIteratorTransformFn} to every item and skipping the ones it maps
 * to `null`/`undefined` (so it both maps and filters).
 *
 * It is lazy and backpressure-friendly — it pulls the next source item only when
 * the consumer asks — and supports full cancellation via an {@link AbortSignal}:
 * the signal interrupts a pull or a transform that is in flight, is forwarded to
 * the transform function, and the source iterator's `return()` is called on the
 * way out. Early termination by the consumer (`break` in `for await`) likewise
 * tears the source down.
 *
 * Omitting `transform` yields an identity pass-through — a handy way to wrap any
 * `AsyncIterable` with abort support.
 *
 * @typeParam SRC - the source item type (defaults to `Uint8Array`).
 * @typeParam DST - the transformed item type (defaults to `SRC`).
 */
export class AsyncIteratorTransformer<SRC = Uint8Array, DST = SRC> implements AsyncIterable<DST> {
    #source: AsyncIterable<SRC>;
    #transform: AsyncIteratorTransformFn<SRC, DST>;
    #signal?: AbortSignal;

    constructor(opts: AsyncIteratorTransformerOptions<SRC, DST>) {
        this.#source = opts.source;
        // No transform = identity pass-through. DST defaults to SRC, so the cast is
        // sound for the common case; it only matters when a caller pins DST != SRC
        // without supplying a transform, which is a misuse.
        this.#transform = opts.transform ?? ((src) => src as unknown as DST);
        this.#signal = opts.signal;
    }

    async *[Symbol.asyncIterator](): AsyncGenerator<DST> {
        const signal = this.#signal;
        // Bail before pulling anything if already aborted.
        signal?.throwIfAborted();

        // A never-resolving promise that rejects on abort, raced against each await
        // so a cancellation interrupts an in-flight pull or transform.
        let abortPromise: Promise<never> | undefined;
        let abortCleanup: (() => void) | undefined;
        if (signal != null) {
            const abort = getAbortPromise(signal);
            abortPromise = abort.promise;
            abortCleanup = abort.cleanup;
        }

        const iterator = this.#source[Symbol.asyncIterator]();
        try {
            let result: IteratorResult<SRC>;
            do {
                // Pull the next source item, racing the abort signal when present.
                result = abortPromise == null ? await iterator.next() : await Promise.race([iterator.next(), abortPromise]);
                if (!result.done) {
                    // Transform it, forwarding the signal and again honoring the abort.
                    const pending = Promise.resolve(this.#transform(result.value, signal));
                    const transformed = abortPromise == null ? await pending : await Promise.race([pending, abortPromise]);
                    // Skip nullish results — this is the "filter" half of the transform.
                    if (transformed != null) {
                        yield transformed;
                    }
                }
            } while (!result.done);
        } finally {
            abortCleanup?.();
            if (typeof iterator.return === 'function') {
                // Best-effort close of the source. If the abort signal won a race above we abandoned
                // an in-flight next(); return() may be chained behind it and never settle, so we race
                // it against a macrotask and move on rather than hang.
                await Promise.race([Promise.resolve().then(iterator.return.bind(iterator)), nextMacroTask()]).then(noop, noop);
            }
        }
    }
}
