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
new AsyncIteratorWriter<SRC = Uint8Array, DST = SRC>({
    source, // AsyncIterable<SRC> — the data to pump
    destination, // WritableStream<DST> | Node writable
    transform, // optional — (src: SRC, signal?: AbortSignal) => DST | null | undefined | Promise<…>
    shouldEnd, // boolean (default true) — close/end the destination when the source is exhausted
    signal, // AbortSignal — cancel the pump
});
```

- **`transform`** _(optional)_ — maps each source item to a chunk before writing; a `null`/`undefined` result (sync or async) skips it, and the `signal` is forwarded to it. This folds the common "transform then write to a stream" case into the writer, so you don't have to wire up a separate [`AsyncIteratorTransformer`](#asynciteratortransformer). Omit it to write items through unchanged.
- **`shouldEnd`** — when `true` (default) the destination is closed/ended after the last chunk. Set to `false` to leave the destination open (e.g. when writing several sources to the same sink).
- **`signal`** — aborting interrupts the pump. The source iterator's `return()` is invoked, and on the error path the destination is destroyed/closed to avoid hanging.

```ts
// Pump a stream of records straight into a file, serializing inline.
const encoder = new TextEncoder();
await new AsyncIteratorWriter<LogRecord, Uint8Array>({
    source: records,
    destination: createWriteStream('out.log'),
    transform: (r) => (r.level === 'debug' ? null : encoder.encode(`${r.level}: ${r.msg}\n`)),
}).write();
```

Backpressure is respected throughout: for Web streams the writer awaits `writer.ready`; for Node streams it waits for the `drain` event whenever `write()` returns `false`.

### `AsyncIteratorTransformer`

Lazily map an `AsyncIterable<SRC>` into an `AsyncIterable<DST>`. The transform may be sync or async, and returning `null`/`undefined` **drops** the item — so one function both maps and filters.

```ts
import { AsyncIteratorTransformer } from 'async-pump';

async function* numbers() {
    yield 1;
    yield 2;
    yield 3;
    yield 4;
}

// Keep evens, square them; everything else is skipped.
const squaredEvens = new AsyncIteratorTransformer({
    source: numbers(),
    transform: (n) => (n % 2 === 0 ? n * n : null),
});

for await (const value of squaredEvens) {
    console.log(value); // 4, then 16
}
```

#### Options

```ts
new AsyncIteratorTransformer<SRC = Uint8Array, DST = SRC>({
    source, // AsyncIterable<SRC> — the items to transform
    transform, // optional — (src: SRC, signal?: AbortSignal) => DST | null | undefined | Promise<DST | null | undefined>
    signal, // AbortSignal — cancels the transform
});
```

- **`transform`** _(optional)_ — maps each item; a `null`/`undefined` result (sync or async) skips it. The active `signal` is passed as the second argument so the callback can wire its own async work (a `fetch`, a timeout) to the same cancellation. **Omit it** for an identity pass-through — a handy way to wrap any `AsyncIterable` with abort support.
- **`signal`** — aborting interrupts a pull or a transform in flight (the iteration rejects with the abort reason) and tears the source down.

The type parameters default to `SRC = Uint8Array` and `DST = SRC`, so a byte pass-through is just `new AsyncIteratorTransformer({ source, signal })`.

The result is lazy and backpressure-friendly — it pulls the next source item only when the consumer asks — and forwards early termination (a `break` in `for await`) to the source iterator's `return()`.

## Composing a pipeline

All three primitives are `AsyncIterable`-shaped, so they chain into a single backpressured pipeline:

```
your code → AsyncIteratorQueue → AsyncIteratorTransformer → AsyncIteratorWriter → stream
```

An imperative producer pushes records into the queue, the transformer maps/filters them on the way through, and the writer pumps the result into a stream — with backpressure flowing all the way back: when the stream is full the writer stops pulling, the transformer stops pulling, and `queue.write()` parks.

```ts
import { AsyncIteratorQueue, AsyncIteratorTransformer, AsyncIteratorWriter } from 'async-pump';
import { createWriteStream } from 'node:fs';

interface LogRecord {
    level: string;
    msg: string;
}

// 1. Imperative producer.
const records = new AsyncIteratorQueue<LogRecord>();

// 2. Serialize each record to a line of bytes — and drop debug records.
const encoder = new TextEncoder();
const lines = new AsyncIteratorTransformer({
    source: records,
    transform: (record) => (record.level === 'debug' ? null : encoder.encode(`${record.level}: ${record.msg}\n`)),
});

// 3. Pump the bytes into a writable stream.
const writer = new AsyncIteratorWriter({
    source: lines,
    destination: createWriteStream('out.log'),
});

const pump = writer.write();

await records.write({ level: 'info', msg: 'started' });
await records.write({ level: 'debug', msg: 'noisy' }); // skipped by the transform
await records.write({ level: 'error', msg: 'boom' });
records.end();

await pump; // out.log now contains "info: started\nerror: boom\n"
```

Pass one shared `AbortSignal` to the queue, the transformer, and the writer to cancel the whole pipeline at once.

## API summary

```ts
import {
    AsyncIteratorQueue,
    type AsyncIteratorQueueOptions,
    AsyncIteratorWriter,
    type AsyncIteratorWriterOptions,
    AsyncIteratorTransformer,
    type AsyncIteratorTransformerOptions,
    type AsyncIteratorTransformFn,
} from 'async-pump';
```

## License

MIT
