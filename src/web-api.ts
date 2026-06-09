/** Duck-types a value as a Web `WritableStream`: it exposes `getWriter`. */
export function isWebWritableLike<T = Uint8Array>(value: unknown): value is WritableStream<T> {
    return !!value && typeof (value as WritableStream<T>).getWriter === 'function';
}

/**
 * Adapts a Web API `WritableStream` to the same promise-based interface the
 * writer pump expects ({@link WebWritableWrapper.write}/`end`/`cleanup`).
 * Acquires an exclusive writer for the stream's lifetime; backpressure comes
 * from the writer's `ready` promise.
 */
export class WebWritableWrapper<T = Uint8Array> {
    #shouldEnd: boolean;
    #writer: WritableStreamDefaultWriter<T>;

    constructor(writable: WritableStream<T>, shouldEnd: boolean) {
        this.#shouldEnd = shouldEnd;
        this.#writer = writable.getWriter();
    }

    /**
     * Release the writer lock. On the error path, abort the stream *first*
     * (while we still hold the lock) so downstream sees the failure rather than
     * a silently truncated stream.
     */
    async cleanup(withError: boolean): Promise<void> {
        if (withError) {
            // Abort must happen before releaseLock(): a released writer can no longer act on the stream.
            await this.#writer.abort();
        }
        this.#writer.releaseLock();
    }

    /** Close the stream, unless the caller opted out via `shouldEnd: false`. */
    end(): Promise<void> {
        if (this.#shouldEnd) {
            return this.#writer.close();
        }
        return Promise.resolve();
    }

    /** Write a chunk, then await `ready` so the pump backpressures on the stream's queue. */
    async write(chunk: T): Promise<void> {
        await this.#writer.write(chunk);
        await this.#writer.ready;
    }
}
