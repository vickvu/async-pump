// Default high-water marks, chosen lazily once we see the first chunk and know
// whether we are dealing with raw bytes or arbitrary objects.
// 64kb for raw data
const DEFAULT_BYTE_HIGHWATER_MARK = 64 * 1024;
// 16 for anything else
const DEFAULT_OBJECT_HIGHWATER_MARK = 16;

/**
 * Lifecycle states of the queue. The queue starts `Ready` and moves to exactly
 * one terminal state, after which it never changes again:
 * - `Finished`       — the producer closed it gracefully via {@link AsyncIteratorQueue.end}.
 * - `Aborted`        — the producer (or an abort signal) cancelled it via {@link AsyncIteratorQueue.abort}.
 * - `ConsumerFailed` — the consumer threw into the iterator (`it.throw(reason)`).
 */
const AsyncIteratorQueueState = Object.freeze({
    Ready: 'ready',
    Finished: 'finished',
    Aborted: 'aborted',
    ConsumerFailed: 'consumer-failed',
} as const);

type AsyncIteratorQueueState = (typeof AsyncIteratorQueueState)[keyof typeof AsyncIteratorQueueState];

export interface AsyncIteratorQueueOptions<T = Uint8Array> {
    /**
     * Buffer threshold (in units returned by {@link AsyncIteratorQueueOptions.size})
     * above which {@link AsyncIteratorQueue.write} starts applying backpressure.
     * Defaults lazily to 64 KiB for byte streams or 16 items for object streams.
     */
    highWaterMark?: number;
    /** When aborted, the queue is cancelled with the signal's `reason`. */
    signal?: AbortSignal;
    /** Per-chunk size accounting. Defaults to `byteLength` for `Uint8Array`, else `1`. */
    size?: (chunk: T) => number;
}

/**
 * A single-producer / single-consumer, backpressured, abortable queue that is
 * itself an `AsyncIterable<T>`.
 *
 * The **producer** pushes chunks with {@link AsyncIteratorQueue.write} (which
 * resolves immediately while under the high-water mark, otherwise once the
 * consumer drains), and terminates the stream with {@link AsyncIteratorQueue.end}
 * (graceful) or {@link AsyncIteratorQueue.abort} (cancel).
 *
 * The **consumer** drives it with `for await` (or the raw iterator returned by
 * `[Symbol.asyncIterator]()`). Only one consumer is allowed.
 *
 * @typeParam T - the chunk type.
 * @typeParam F - the type of the optional final value delivered by `end(final)`.
 */
export class AsyncIteratorQueue<T = Uint8Array, F = void> implements AsyncIterable<T> {
    #highWaterMark: number;
    #size: ((chunk: T) => number) | undefined;
    #signal?: AbortSignal;
    #abortListener: () => void;
    #state: AsyncIteratorQueueState;
    // Guards the single-consumer invariant: flipped true the first time the queue is iterated.
    #iterated: boolean;
    // Consumers parked in next() waiting for a chunk that has not been written yet.
    #pendingReads: { resolve: (result: IteratorResult<T>) => void; reject: (err: unknown) => void }[];
    // Producers parked in write() waiting for the buffer to fall back under the high-water mark.
    #pendingDrains: { resolve: () => void; reject: (err: unknown) => void }[];
    // Buffered chunks waiting to be consumed, with their pre-computed size.
    #queue: { chunk: T; size: number }[];
    #queuedSize: number;
    #final: F | undefined;
    // The abort error, stored so a read that arrives *after* the abort can still observe it.
    #error: unknown;
    // Ensures an abort error is surfaced to the consumer exactly once (see next()).
    #errorDelivered: boolean;

    constructor(opts: AsyncIteratorQueueOptions<T> = {}) {
        // 0 mean delay calculating the highwater mark until
        // we see the data since we don't know if the data is Uint8Array or object
        this.#highWaterMark = opts.highWaterMark ?? 0;
        this.#signal = opts.signal;
        // delay assign the default size until we see the data
        this.#size = opts.size;
        this.#abortListener = () => {
            this.abort(this.#signal?.reason);
        };
        if (this.#signal != null) {
            this.#signal.addEventListener('abort', this.#abortListener, { once: true });
        }
        this.#state = AsyncIteratorQueueState.Ready;
        this.#iterated = false;
        this.#pendingReads = [];
        this.#pendingDrains = [];
        this.#queue = [];
        this.#queuedSize = 0;
        this.#errorDelivered = false;
    }

    /** Drop all buffered chunks and reset the running size. */
    #cleanupQueue() {
        this.#queue = [];
        this.#queuedSize = 0;
    }

    /** Detach the abort-signal listener so the queue can be garbage-collected. */
    #cleanupAbort() {
        this.#signal?.removeEventListener('abort', this.#abortListener);
    }

    /** Resolve every parked producer, unblocking their `write()` promises. */
    #releaseDrains() {
        let drains = this.#pendingDrains.shift();
        while (drains != null) {
            drains.resolve();
            drains = this.#pendingDrains.shift();
        }
    }

    /** Reject every parked producer (used when the queue is torn down). */
    #rejectDrains(err: unknown) {
        let drains = this.#pendingDrains.shift();
        while (drains != null) {
            drains.reject(err);
            drains = this.#pendingDrains.shift();
        }
    }

    /** Resolve every parked consumer with the terminal `{ value: final, done: true }`. */
    #deliverFinal() {
        let read = this.#pendingReads.shift();
        while (read != null) {
            read.resolve({ value: this.#final, done: true });
            read = this.#pendingReads.shift();
        }
    }

    /**
     * Reject every parked consumer with `err`. If at least one read was waiting,
     * the error counts as delivered so a later `next()` won't re-report it
     * (the once-then-done contract; see next()).
     */
    #deliverReject(err: unknown) {
        let atLeastOneDelivered = false;
        let read = this.#pendingReads.shift();
        while (read != null) {
            read.reject(err);
            atLeastOneDelivered = true;
            read = this.#pendingReads.shift();
        }
        if (atLeastOneDelivered) {
            this.#errorDelivered = true;
        }
    }

    /** Hand `value` straight to a parked consumer, bypassing the buffer. Returns whether one was waiting. */
    #deliverNext(value: T) {
        const read = this.#pendingReads.shift();
        if (read) {
            read.resolve({ value, done: false });
            return true;
        }
        return false;
    }

    /**
     * Consumer-side termination, shared by the iterator's `return()` and `throw()`.
     * Idempotent: a no-op (resolving to `{ done: true }`) once already terminated.
     */
    #stopConsumer({ final, error, state }: { final?: F; error?: unknown; state: AsyncIteratorQueueState }): Promise<IteratorResult<T, F | undefined>> {
        if (this.#state !== AsyncIteratorQueueState.Ready) {
            return Promise.resolve({ value: this.#final, done: true });
        }
        this.#final = final;
        this.#state = state;
        // Free the queue and remove abort event
        this.#cleanupQueue();
        this.#cleanupAbort();
        // Finish pending reads
        this.#deliverFinal();
        // Reject pending drains
        this.#rejectDrains(error ?? new Error('The queue has been stopped'));
        if (state === AsyncIteratorQueueState.ConsumerFailed) {
            // Mirror a generator's `it.throw()`: the error propagates to *this* caller.
            /* eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors */
            return Promise.reject(error);
        }
        return Promise.resolve({ value: this.#final, done: true });
    }

    /** True once {@link AsyncIteratorQueue.end} closed the queue gracefully. */
    isFinished() {
        return this.#state === AsyncIteratorQueueState.Finished;
    }

    /** True once {@link AsyncIteratorQueue.abort} (or an abort signal) cancelled the queue. */
    isAborted() {
        return this.#state === AsyncIteratorQueueState.Aborted;
    }

    /** True once the consumer threw into the iterator via `it.throw(reason)`. */
    isConsumerFailed() {
        return this.#state === AsyncIteratorQueueState.ConsumerFailed;
    }

    /** True once the queue has reached any terminal state. */
    isDone() {
        return this.#state === AsyncIteratorQueueState.Finished || this.#state === AsyncIteratorQueueState.Aborted || this.#state === AsyncIteratorQueueState.ConsumerFailed;
    }

    /** True while the queue is still open for writes. */
    isReady() {
        return this.#state === AsyncIteratorQueueState.Ready;
    }

    /** The value passed to {@link AsyncIteratorQueue.end}, if any. */
    get finalValue() {
        return this.#final;
    }

    /**
     * Push a chunk onto the queue.
     *
     * @returns a promise that resolves immediately while the buffer is under the
     * high-water mark, or once the consumer has drained enough below it (this is
     * how backpressure flows back to the producer). Rejects if the queue is
     * already closed.
     */
    write(value: T): Promise<void> {
        if (this.#state !== AsyncIteratorQueueState.Ready) {
            return Promise.reject(new Error('Cannot write after the queue is closed'));
        }
        // Fast path: if a consumer is already parked and nothing is buffered, hand the
        // chunk straight to it. No buffering means no backpressure to apply.
        if (this.#queue.length === 0 && this.#deliverNext(value)) {
            return Promise.resolve();
        }
        if (this.#highWaterMark <= 0) {
            // First chunk: now that we can tell bytes from objects, pick the default mark.
            this.#highWaterMark = value instanceof Uint8Array ? DEFAULT_BYTE_HIGHWATER_MARK : DEFAULT_OBJECT_HIGHWATER_MARK;
        }
        let size: number;
        if (this.#size == null) {
            size = value instanceof Uint8Array ? value.byteLength : 1;
        } else {
            size = this.#size(value);
        }
        this.#queue.push({ chunk: value, size });
        this.#queuedSize += size;
        if (this.#queuedSize > this.#highWaterMark) {
            // Over the mark: park the producer until next() drains us back under it.
            return new Promise<void>((resolve, reject) => {
                this.#pendingDrains.push({ resolve, reject });
            });
        }
        return Promise.resolve();
    }

    /**
     * Close the queue gracefully. Already-buffered chunks are **not** discarded —
     * the consumer drains them first and only then observes `{ value: final, done: true }`.
     * No-op if the queue is already closed.
     */
    end(final?: F): void {
        if (this.#state !== AsyncIteratorQueueState.Ready) {
            return;
        }
        this.#state = AsyncIteratorQueueState.Finished;
        this.#final = final;
        this.#cleanupAbort();
    }

    /**
     * Cancel the queue immediately. Buffered chunks are dropped, parked producers
     * and consumers reject with `error`, and `error` is surfaced to the consumer's
     * next read exactly once (then `{ done: true }` thereafter). No-op if already closed.
     */
    abort(error: unknown = new Error('Aborted')): void {
        if (this.#state !== AsyncIteratorQueueState.Ready) {
            return;
        }
        this.#state = AsyncIteratorQueueState.Aborted;
        this.#error = error;
        // Reject pending reads
        this.#deliverReject(error);
        // Reject pending drains
        this.#rejectDrains(error);
        // Free the queue up
        this.#cleanupQueue();
        this.#cleanupAbort();
    }

    [Symbol.asyncIterator](): AsyncIterator<T, F | undefined, undefined> {
        if (this.#iterated) {
            throw new Error('AsyncIteratorQueue supports only a single consumer');
        }
        this.#iterated = true;
        return {
            next: () => {
                // 1. Buffered data takes priority, even after the queue is closed,
                //    so a graceful end() still drains everything that was written.
                const item = this.#queue.shift();
                if (item) {
                    this.#queuedSize -= item.size;
                    // Dropping under the mark unblocks any producer parked in write().
                    if (this.#queuedSize < this.#highWaterMark) {
                        this.#releaseDrains();
                    }
                    return Promise.resolve({ value: item.chunk, done: false });
                }
                // 2. Phase A — an abort happened and no read was parked to catch it:
                //    surface the stored error to this read exactly once.
                if (this.#state === AsyncIteratorQueueState.Aborted && this.#error != null && !this.#errorDelivered) {
                    this.#errorDelivered = true;
                    /* eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors */
                    return Promise.reject(this.#error); // phase A: deliver once
                }
                // 3. Phase B — terminated and the buffer (and any error) are drained: done.
                if (this.#state !== AsyncIteratorQueueState.Ready) {
                    this.#deliverFinal();
                    return Promise.resolve({ value: this.#final, done: true });
                }
                // 4. Still open but empty: park until the producer writes or closes.
                return new Promise<IteratorResult<T, F | undefined>>((resolve, reject) => {
                    this.#pendingReads.push({ resolve, reject });
                });
            },

            // Consumer left early (e.g. `break` out of `for await`): close gracefully.
            return: (final?: F) => {
                return this.#stopConsumer({ final, state: AsyncIteratorQueueState.Finished });
            },

            // Consumer injected an error into the iterator: propagate it to the caller.
            throw: (reason?: unknown) => {
                return this.#stopConsumer({ error: reason, state: AsyncIteratorQueueState.ConsumerFailed });
            },
        };
    }
}
