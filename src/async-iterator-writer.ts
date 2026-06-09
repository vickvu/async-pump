import { nextMacroTask, noop, getAbortPromise } from '#SRC/utils.ts';
import { NodeWritableWrapper, isNodeWritableLike, type NodeWritableLike } from '#SRC/node.ts';
import { WebWritableWrapper, isWebWritableLike } from '#SRC/web-api.ts';

export interface AsyncIteratorWriterOptions<T = Uint8Array> {
    /** The async iterable to drain. */
    source: AsyncIterable<T>;
    /** Where to pump chunks: a Web `WritableStream` or a Node.js writable. Auto-detected. */
    destination: WritableStream<T> | NodeWritableLike<T>;
    /** Close/end the destination once the source is exhausted. Defaults to `true`. */
    shouldEnd?: boolean;
    /** Aborts the pump; the source's `return()` is called and the destination torn down. */
    signal?: AbortSignal;
}

/**
 * Pumps an {@link AsyncIterable} into a Web or Node.js writable, propagating
 * backpressure (it only pulls the next chunk once the destination accepted the
 * previous one) and supporting cancellation via an {@link AbortSignal}.
 *
 * Construct once, then call {@link AsyncIteratorWriter.write} to run the pump to
 * completion.
 */
export class AsyncIteratorWriter<T = Uint8Array> {
    #src: AsyncIterable<T>;
    #dstNode?: NodeWritableLike<T>;
    #dstWeb?: WritableStream<T>;
    #signal?: AbortSignal;
    #shouldEnd: boolean;

    constructor(opts: AsyncIteratorWriterOptions<T>) {
        if (isWebWritableLike<T>(opts.destination)) {
            this.#dstWeb = opts.destination;
        } else if (isNodeWritableLike<T>(opts.destination)) {
            this.#dstNode = opts.destination;
        } else {
            throw new TypeError('Unsupported destination: expected Node writable or Web API WritableStream');
        }
        this.#src = opts.source;
        this.#signal = opts.signal;
        this.#shouldEnd = opts.shouldEnd ?? true;
    }

    /**
     * Run the pump to completion: pull from the source and write to the
     * destination one chunk at a time until the source ends, an error is thrown,
     * or the signal aborts.
     *
     * @throws the source/destination error, or the signal's abort reason.
     */
    async write() {
        // Bail out before doing any work if already aborted.
        this.#signal?.throwIfAborted();
        // A never-resolving promise that rejects on abort. We race it against each
        // await so an abort interrupts a pull/write that would otherwise block.
        let abortPromise: Promise<never> | undefined;
        let abortCleanup: (() => void) | undefined;
        if (this.#signal != null) {
            const abortPromiseResult = getAbortPromise(this.#signal);
            abortPromise = abortPromiseResult.promise;
            abortCleanup = abortPromiseResult.cleanup;
        }
        /* eslint-disable-next-line @typescript-eslint/no-non-null-assertion */
        const wrapper = this.#dstWeb != null ? new WebWritableWrapper<T>(this.#dstWeb, this.#shouldEnd) : new NodeWritableWrapper<T>(this.#dstNode!, this.#shouldEnd);
        const iterator = this.#src[Symbol.asyncIterator]();
        let error: unknown;
        try {
            let result: IteratorResult<T>;
            do {
                // Pull the next chunk, racing the abort signal when present.
                result = abortPromise == null ? await iterator.next() : await Promise.race([iterator.next(), abortPromise]);
                if (!result.done) {
                    const chunk = result.value;
                    // Write it, again honoring backpressure and the abort signal.
                    if (abortPromise == null) {
                        await wrapper.write(chunk);
                    } else {
                        await Promise.race([wrapper.write(chunk), abortPromise]);
                    }
                }
            } while (!result.done);
            await wrapper.end();
        } catch (err) {
            error = err;
        } finally {
            // Tear down the destination (destroy/abort it on the error path) and detach the abort listener.
            await wrapper.cleanup(error != null);
            abortCleanup?.();
            if (typeof iterator.return === 'function') {
                // Best-effort close of the source. When the abort signal won the race above we
                // abandoned an in-flight next(); return() may be chained behind it and never settle,
                // so we race it against a macrotask and move on rather than hang.
                await Promise.race([Promise.resolve().then(iterator.return.bind(iterator)), nextMacroTask()]).then(noop, noop);
            }
        }
        if (error != null) {
            throw error as Error;
        }
    }
}
