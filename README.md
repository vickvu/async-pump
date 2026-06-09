# async-pump

> Zero-dependency primitives for bridging `AsyncIterable` with Node.js and Web streams, with backpressure and abort support.

`async-pump` provides two small, cross-runtime building blocks for streaming pipelines:

- **`AsyncIteratorQueue`** — a backpressured, abortable `AsyncIterable<T>` queue. Push chunks with a promise-returning `write()` that respects a high-water mark, and consume them with `for await`.
- **`AsyncIteratorWriter`** — pumps any `AsyncIterable<T>` into a Node.js `Writable` **or** a Web API `WritableStream`, handling backpressure, lifecycle, and cancellation for you.

It works identically in Node.js (with native `Buffer`/`Uint8Array` support) and the browser, and ships with zero runtime dependencies. Both raw-byte and object streams are supported.

## Install

```sh
npm install async-pump
```

Requires Node.js >= 18 (or any runtime with `AsyncIterable` and, for the Web path, `WritableStream`).

## Quick start

### `AsyncIteratorQueue`

A single-producer / single-consumer queue that is itself an `AsyncIterable`. The producer calls `write()`; the consumer iterates.

```ts
import { AsyncIteratorQueue } from 'async-pump';

const queue = new AsyncIteratorQueue<Uint8Array>();

// Producer
async function produce() {
    await queue.write(new TextEncoder().encode('hello '));
    await queue.write(new TextEncoder().encode('world'));
    queue.end(); // close the queue (no more data)
}

// Consumer
async function consume() {
    for await (const chunk of queue) {
        process.stdout.write(chunk);
    }
}

await Promise.all([produce(), consume()]);
```

`write()` resolves immediately while the internal buffer is below the high-water mark. Once the buffered size exceeds it, the returned promise stays pending until the consumer drains enough data — this is how backpressure propagates back to the producer.

#### Options

```ts
new AsyncIteratorQueue<T, F>({
    highWaterMark, // number — buffer threshold before write() applies backpressure
    signal, // AbortSignal — aborts the queue when triggered
    size, // (chunk: T) => number — custom per-chunk size accounting
});
```

- **`highWaterMark`** — defaults are resolved lazily from the first chunk: `64 * 1024` (64 KiB) for `Uint8Array` byte streams, `16` for object streams.
- **`size`** — by default a `Uint8Array` chunk counts as its `byteLength` and any other value counts as `1`. Provide `size` to override (e.g. measure object streams by a field).
- **`signal`** — when aborted, the queue calls `abort(signal.reason)`: buffered data is dropped, and pending reads / `write()` promises reject.

#### Producer methods

The two type parameters are `AsyncIteratorQueue<T, F>`, where `T` is the chunk type and `F` is the type of the optional _final value_ delivered when the queue completes normally.

| Call                       | Meaning                                                                         | State      |
| -------------------------- | ------------------------------------------------------------------------------- | ---------- |
| `await queue.write(chunk)` | Push a chunk; resolves immediately under the high-water mark, else on drain     | `ready`    |
| `queue.end(finalValue?)`   | Graceful close: buffered chunks are still drained, then `finalValue` is emitted | `finished` |
| `queue.abort(error?)`      | Cancel immediately, dropping buffered data; the next read rejects with `error`  | `aborted`  |

After completion the consumer's `for await` loop ends. Inspect the outcome via:

```ts
queue.isReady(); // still open
queue.isFinished(); // closed via end()
queue.isAborted(); // cancelled via abort() / signal
queue.isConsumerFailed(); // the consumer threw into the iterator (it.throw())
queue.isDone(); // finished || aborted || consumer-failed
queue.finalValue; // the value passed to end()
```

A graceful `end()` never discards already-buffered data — the consumer drains every queued chunk before seeing `{ done: true }`. An `abort(error)` drops the buffer and surfaces `error` to the consumer's next read exactly once (then `{ done: true }` thereafter), mirroring how a generator or a Node `Readable` reports a failure.

> **Single consumer:** the queue allows only one iterator. Calling `[Symbol.asyncIterator]()` (e.g. starting a second `for await`) a second time throws.

### `AsyncIteratorWriter`

Pump an async iterable into a writable destination. The destination is auto-detected: a Web `WritableStream` (anything with `getWriter`) or a Node-style writable (anything with `write`/`end`).

```ts
import { AsyncIteratorWriter } from 'async-pump';
import { createWriteStream } from 'node:fs';

async function* source() {
    yield Buffer.from('line 1\n');
    yield Buffer.from('line 2\n');
}

const writer = new AsyncIteratorWriter({
    source: source(),
    destination: createWriteStream('out.txt'),
});

await writer.write(); // resolves when the source is fully drained into the destination
```

Web stream destination:

```ts
const writer = new AsyncIteratorWriter({
    source: source(),
    destination: someWritableStream, // a Web API WritableStream
});
await writer.write();
```

#### Options

```ts
new AsyncIteratorWriter<T>({
    source, // AsyncIterable<T> — the data to pump
    destination, // WritableStream<T> | Node writable
    shouldEnd, // boolean (default true) — close/end the destination when the source is exhausted
    signal, // AbortSignal — cancel the pump
});
```

- **`shouldEnd`** — when `true` (default) the destination is closed/ended after the last chunk. Set to `false` to leave the destination open (e.g. when writing several sources to the same sink).
- **`signal`** — aborting interrupts the pump. The source iterator's `return()` is invoked, and on the error path the destination is destroyed/closed to avoid hanging.

Backpressure is respected throughout: for Web streams the writer awaits `writer.ready`; for Node streams it waits for the `drain` event whenever `write()` returns `false`.

## Composing the two

`AsyncIteratorQueue` is an `AsyncIterable`, so it plugs straight into `AsyncIteratorWriter` as a `source` — letting an imperative producer feed a stream destination with end-to-end backpressure:

```ts
const queue = new AsyncIteratorQueue<Uint8Array>();

const writer = new AsyncIteratorWriter({
    source: queue,
    destination: createWriteStream('out.bin'),
});

const pump = writer.write();

await queue.write(chunk1);
await queue.write(chunk2);
queue.end();

await pump;
```

## API summary

```ts
import { AsyncIteratorQueue, type AsyncIteratorQueueOptions, AsyncIteratorWriter, type AsyncIteratorWriterOptions } from 'async-pump';
```

## License

MIT
