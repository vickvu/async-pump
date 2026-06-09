import { nextMacroTask, noop, getAbortPromise } from '#SRC/utils.ts';
import { NodeWritableWrapper, isNodeWritableLike, type NodeWritableLike } from '#SRC/node.ts';
import { WebWritableWrapper, isWebWritableLike } from '#SRC/web-api.ts';
import { type AsyncIteratorTransformFn } from '#SRC/async-iterator-transform.ts';

export interface AsyncIteratorWriterOptions<SRC = Uint8Array, DST = SRC> {
    /** The async iterable to drain. */
    source: AsyncIterable<SRC>;
    /** Where to pump chunks: a Web `WritableStream` or a Node.js writable. Auto-detected. */
    destination: WritableStream<DST> | NodeWritableLike<DST>;
    /**
     * Optional map applied to each item before it is written, skipping the ones
     * it maps to `null`/`undefined`. The active signal is forwarded to it. Omit it
     * to write source items through unchanged (`DST` defaults to `SRC`). This is a
     * convenience for the common "transform then write to a stream" case; for a
     * transformed `AsyncIterable` with no writable destination use
     * {@link AsyncIteratorTransformer} directly.
     */
    transform?: AsyncIteratorTransformFn<SRC, DST>;
    /** Close/end the destination once the source is exhausted. Defaults to `true`. */
    shouldEnd?: boolean;
    /** Aborts the pump; the source's `return()` is called and the destination torn down. */
    signal?: AbortSignal;
}

/**
 * Pumps an {@link AsyncIterable} into a Web or Node.js writable, propagating
 * backpressure (it only pulls the next item once the destination accepted the
 * previous chunk) and supporting cancellation via an {@link AbortSignal}.
 *
 * An optional `transform` maps each source item to an output chunk (skipping
 * nullish results) on the way through, so a source can be reshaped without
 * wiring up a separate {@link AsyncIteratorTransformer}.
 *
 * Construct once, then call {@link AsyncIteratorWriter.write} to run the pump to
 * completion.
 *
 * @typeParam SRC - the source item type (defaults to `Uint8Array`).
 * @typeParam DST - the chunk type written to the destination (defaults to `SRC`).
 */
export class AsyncIteratorWriter<SRC = Uint8Array, DST = SRC> {
    #src: AsyncIterable<SRC>;
    #dstNode?: NodeWritableLike<DST>;
    #dstWeb?: WritableStream<DST>;
    #transform: AsyncIteratorTransformFn<SRC, DST>;
    #signal?: AbortSignal;
    #shouldEnd: boolean;

    constructor(opts: AsyncIteratorWriterOptions<SRC, DST>) {
        if (isWebWritableLike<DST>(opts.destination)) {
            this.#dstWeb = opts.destination;
        } else if (isNodeWritableLike<DST>(opts.destination)) {
            this.#dstNode = opts.destination;
        } else {
            throw new TypeError('Unsupported destination: expected Node writable or Web API WritableStream');
        }
        this.#src = opts.source;
        // No transform = identity pass-through. DST defaults to SRC, so the cast is
        // sound for the common case; it only matters when a caller pins DST != SRC
        // without supplying a transform, which is a misuse.
        this.#transform = opts.transform ?? ((src) => src as unknown as DST);
        this.#signal = opts.signal;
        this.#shouldEnd = opts.shouldEnd ?? true;
    }

    /**
     * Run the pump to completion: pull from the source, transform each item, and
     * write it to the destination one chunk at a time until the source ends, an
     * error is thrown, or the signal aborts.
     *
     * @throws the source/transform/destination error, or the signal's abort reason.
     */
    async write() {
        // Bail out before doing any work if already aborted.
        this.#signal?.throwIfAborted();
        // A never-resolving promise that rejects on abort. We race it against each
        // await so an abort interrupts a pull/transform/write that would otherwise block.
        let abortPromise: Promise<never> | undefined;
        let abortCleanup: (() => void) | undefined;
        if (this.#signal != null) {
            const abortPromiseResult = getAbortPromise(this.#signal);
            abortPromise = abortPromiseResult.promise;
            abortCleanup = abortPromiseResult.cleanup;
        }
        /* eslint-disable-next-line @typescript-eslint/no-non-null-assertion */
        const wrapper = this.#dstWeb != null ? new WebWritableWrapper<DST>(this.#dstWeb, this.#shouldEnd) : new NodeWritableWrapper<DST>(this.#dstNode!, this.#shouldEnd);
        const iterator = this.#src[Symbol.asyncIterator]();
        let error: unknown;
        try {
            let result: IteratorResult<SRC>;
            do {
                // Pull the next source item, racing the abort signal when present.
                result = abortPromise == null ? await iterator.next() : await Promise.race([iterator.next(), abortPromise]);
                if (!result.done) {
                    // Map it to an output chunk, forwarding the signal; nullish results are skipped.
                    const pending = Promise.resolve(this.#transform(result.value, this.#signal));
                    const chunk = abortPromise == null ? await pending : await Promise.race([pending, abortPromise]);
                    if (chunk != null) {
                        // Write it, again honoring backpressure and the abort signal.
                        if (abortPromise == null) {
                            await wrapper.write(chunk);
                        } else {
                            await Promise.race([wrapper.write(chunk), abortPromise]);
                        }
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
