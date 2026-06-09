// Shared test utilities. Not a spec file, so it is exempt from the mocha lint rules.

/** The outcome of a settled promise, captured without letting a rejection escape. */
export type Settled<T> = { status: 'resolved'; value: T } | { status: 'rejected'; reason: unknown };

/** Await a promise and report how it settled, never re-throwing. */
export async function settle<T>(promise: Promise<T>): Promise<Settled<T>> {
    try {
        return { status: 'resolved', value: await promise };
    } catch (reason) {
        return { status: 'rejected', reason };
    }
}

/** Resolves on a later macrotask, after the current microtask queue has drained. */
export function macrotask(): Promise<void> {
    return new Promise<void>((resolve) => setTimeout(resolve, 0));
}

/**
 * Resolves `true` if `promise` is still pending after a macrotask, `false` if it
 * settled first. Attaches handlers either way, so a later rejection won't leak.
 */
export function isPending(promise: Promise<unknown>): Promise<boolean> {
    return Promise.race([
        promise.then(
            () => false,
            () => false,
        ),
        macrotask().then(() => true),
    ]);
}

/** Drain an async iterable into an array. */
export async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
    const out: T[] = [];
    for await (const item of iterable) {
        out.push(item);
    }
    return out;
}

/** An async generator that yields each item in turn. */
export async function* fromArray<T>(items: readonly T[]): AsyncGenerator<T> {
    await Promise.resolve();
    for (const item of items) {
        yield item;
    }
}
