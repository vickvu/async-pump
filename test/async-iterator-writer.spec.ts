import { expect } from 'chai';
import { Writable } from 'node:stream';
import { AsyncIteratorWriter } from '#SRC/async-iterator-writer.ts';
import { AsyncIteratorQueue } from '#SRC/async-iterator-queue.ts';
import { settle, macrotask, fromArray } from './helpers.ts';

interface NodeSink {
    writable: Writable;
    chunks: Buffer[];
}

function makeNodeSink(): NodeSink {
    const chunks: Buffer[] = [];
    const writable = new Writable({
        write(chunk: Buffer, _enc, cb) {
            chunks.push(Buffer.from(chunk));
            cb();
        },
    });
    return { writable, chunks };
}

interface WebSink<T> {
    stream: WritableStream<T>;
    chunks: T[];
    closed: () => boolean;
    aborted: () => boolean;
}

function makeWebSink<T>(): WebSink<T> {
    const chunks: T[] = [];
    let closed = false;
    let aborted = false;
    const stream = new WritableStream<T>({
        write(chunk) {
            chunks.push(chunk);
        },
        close() {
            closed = true;
        },
        abort() {
            aborted = true;
        },
    });
    return { stream, chunks, closed: () => closed, aborted: () => aborted };
}

describe('AsyncIteratorWriter', function () {
    describe('construction', function () {
        it('throws on an unsupported destination', function () {
            expect(() => new AsyncIteratorWriter({ source: fromArray([1]), destination: {} as WritableStream<number> })).to.throw(TypeError);
        });
    });

    describe('Node destination', function () {
        it('pumps every chunk through and ends the stream by default', async function () {
            const sink = makeNodeSink();
            const writer = new AsyncIteratorWriter<Buffer>({
                source: fromArray([Buffer.from('foo'), Buffer.from('bar')]),
                destination: sink.writable,
            });

            await writer.write();

            expect(Buffer.concat(sink.chunks).toString()).to.equal('foobar');
            expect(sink.writable.writableEnded).to.equal(true);
        });

        it('leaves the stream open when shouldEnd is false', async function () {
            const sink = makeNodeSink();
            const writer = new AsyncIteratorWriter<Buffer>({
                source: fromArray([Buffer.from('foo')]),
                destination: sink.writable,
                shouldEnd: false,
            });

            await writer.write();

            expect(sink.writable.writableEnded).to.equal(false);
        });

        it('propagates a source error and destroys the stream', async function () {
            const sink = makeNodeSink();
            const reason = new Error('source boom');
            async function* boom(): AsyncGenerator<Buffer> {
                yield Buffer.from('foo');
                await Promise.resolve();
                throw reason;
            }
            const writer = new AsyncIteratorWriter<Buffer>({ source: boom(), destination: sink.writable });

            const result = await settle(writer.write());

            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
            expect(sink.writable.destroyed).to.equal(true);
            expect(Buffer.concat(sink.chunks).toString()).to.equal('foo');
        });
    });

    describe('Web destination', function () {
        it('pumps every chunk through and closes the stream by default', async function () {
            const sink = makeWebSink<string>();
            const writer = new AsyncIteratorWriter<string>({
                source: fromArray(['foo', 'bar']),
                destination: sink.stream,
            });

            await writer.write();

            expect(sink.chunks).to.deep.equal(['foo', 'bar']);
            expect(sink.closed()).to.equal(true);
        });

        it('aborts the stream on a source error', async function () {
            const sink = makeWebSink<string>();
            async function* boom(): AsyncGenerator<string> {
                yield 'foo';
                await Promise.resolve();
                throw new Error('source boom');
            }
            const writer = new AsyncIteratorWriter<string>({ source: boom(), destination: sink.stream });

            const result = await settle(writer.write());

            expect(result.status).to.equal('rejected');
            expect(sink.aborted()).to.equal(true);
        });
    });

    describe('abort signal', function () {
        it('rejects immediately when the signal is already aborted', async function () {
            const sink = makeNodeSink();
            const controller = new AbortController();
            controller.abort(new Error('pre-aborted'));
            const writer = new AsyncIteratorWriter<Buffer>({
                source: fromArray([Buffer.from('foo')]),
                destination: sink.writable,
                signal: controller.signal,
            });

            const result = await settle(writer.write());

            expect(result.status).to.equal('rejected');
            expect(sink.chunks).to.deep.equal([]);
        });

        it('interrupts an in-flight pump and tears the destination down', async function () {
            const sink = makeNodeSink();
            const controller = new AbortController();
            const reason = new Error('cancelled');
            async function* slow(): AsyncGenerator<Buffer> {
                yield Buffer.from('foo');
                await new Promise<void>(() => undefined); // never settles
                yield Buffer.from('bar');
            }
            const writer = new AsyncIteratorWriter<Buffer>({
                source: slow(),
                destination: sink.writable,
                signal: controller.signal,
            });

            const pump = settle(writer.write());
            await macrotask();
            controller.abort(reason);

            const result = await pump;
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
            expect(sink.writable.destroyed).to.equal(true);
        });
    });

    describe('integration with AsyncIteratorQueue', function () {
        it('pumps a queue into a Node writable with end-to-end flow', async function () {
            const sink = makeNodeSink();
            const queue = new AsyncIteratorQueue<Buffer>();
            const writer = new AsyncIteratorWriter<Buffer>({ source: queue, destination: sink.writable });

            const pump = writer.write();
            await queue.write(Buffer.from('hello '));
            await queue.write(Buffer.from('world'));
            queue.end();
            await pump;

            expect(Buffer.concat(sink.chunks).toString()).to.equal('hello world');
            expect(sink.writable.writableEnded).to.equal(true);
        });
    });
});
