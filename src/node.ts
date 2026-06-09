/**
 * Minimal structural type so this works without importing Node types.
 * It matches the parts of a Node.js Writable stream that we need.
 */
export interface NodeWritableLike<T = Uint8Array> {
    write(chunk: T): boolean;
    end(): void;
    destroy?(error?: unknown): void;

    on(event: string, listener: (...args: unknown[]) => void): unknown;
    off(event: string, listener: (...args: unknown[]) => void): unknown;
}

/** Duck-types a value as a Node.js writable: it has both `write` and `end`. */
export function isNodeWritableLike<T = Uint8Array>(value: unknown): value is NodeWritableLike<T> {
    return !!value && typeof (value as NodeWritableLike<T>).write === 'function' && typeof (value as NodeWritableLike<T>).end === 'function';
}

/**
 * Adapts a Node.js writable to the small promise-based interface the writer
 * pump expects ({@link NodeWritableWrapper.write}/`end`/`cleanup`), translating
 * the stream's event-based backpressure (`drain`) and lifecycle (`close`,
 * `finish`, `error`) into resolved/rejected promises.
 */
export class NodeWritableWrapper<T = Uint8Array> {
    #writable: NodeWritableLike<T>;
    #shouldEnd: boolean;
    #drainPromiseResolve: (() => void) | undefined;
    #drainPromiseReject: ((err: unknown) => void) | undefined;
    #onDrain: () => void;
    #onClose: () => void;
    #onFinish: () => void;
    #onError: (err: unknown) => void;

    constructor(writable: NodeWritableLike<T>, shouldEnd: boolean) {
        this.#writable = writable;
        this.#shouldEnd = shouldEnd;
        // Arrow functions so `this` stays bound to the wrapper when the stream
        // invokes them as event listeners (a plain `function` would see the emitter).
        // `drain` (backpressure relieved) and `close`/`finish` (stream ended) all
        // mean "stop waiting" — resolve whoever is parked in write().
        this.#onDrain = () => {
            this.#drainPromiseResolve?.();
            this.#drainPromiseResolve = undefined;
        };
        this.#onClose = () => {
            this.#drainPromiseResolve?.();
            this.#drainPromiseResolve = undefined;
        };
        this.#onFinish = () => {
            this.#drainPromiseResolve?.();
            this.#drainPromiseResolve = undefined;
        };
        // A stream error fails the parked write() instead of resolving it.
        this.#onError = (err: unknown) => {
            this.#drainPromiseReject?.(err);
            this.#drainPromiseResolve = undefined;
        };
        this.#writable.on('drain', this.#onDrain);
        this.#writable.on('close', this.#onClose);
        this.#writable.on('finish', this.#onFinish);
        this.#writable.on('error', this.#onError);
    }

    /**
     * Detach all listeners. When `withError` is set, also destroy the stream:
     * this unblocks a parked write so the abandoned source iterator can settle.
     */
    cleanup(withError: boolean): Promise<void> {
        this.#writable.off('drain', this.#onDrain);
        this.#writable.off('close', this.#onClose);
        this.#writable.off('finish', this.#onFinish);
        this.#writable.off('error', this.#onError);
        if (withError) {
            // This is important to close the stream when we abort
            // This is especially important for current Node.js version
            // where destroy() unblocks a parked read() so a subsequent iterator.return() can settle
            // instead of haging behind next() what we may abandoned when the abort signal won
            // Note that this break the implied contract of shouldEnd because we may end up destroying a stream that we were not supposed to end,
            // but in practice it is better to break this contract than hanging the process on abort.
            this.#writable.destroy?.();
        }
        return Promise.resolve();
    }

    /** End the underlying stream, unless the caller opted out via `shouldEnd: false`. */
    end(): Promise<void> {
        if (this.#shouldEnd) {
            this.#writable.end();
        }
        return Promise.resolve();
    }

    /**
     * Write a chunk. Resolves synchronously when the stream still has room;
     * otherwise returns a promise that settles on the next `drain` (resolve) or
     * `error` (reject), which is how this backpressures the pump.
     */
    write(chunk: T): Promise<void> {
        try {
            if (!this.#writable.write(chunk)) {
                const drainPromise = new Promise<void>((resolve, reject) => {
                    this.#drainPromiseResolve = resolve;
                    this.#drainPromiseReject = reject;
                });
                return drainPromise;
            }
        } catch (err: unknown) {
            /* eslint-disable-next-line @typescript-eslint/prefer-promise-reject-errors */
            return Promise.reject(err);
        }
        return Promise.resolve();
    }
}
