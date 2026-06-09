import { expect } from 'chai';
import { AsyncIteratorQueue } from '#SRC/async-iterator-queue.ts';
import { settle, isPending, collect } from './helpers.ts';

describe('AsyncIteratorQueue', function () {
    describe('basic production and consumption', function () {
        it('delivers written chunks to a for-await consumer in order, then ends', async function () {
            const queue = new AsyncIteratorQueue<number>();

            const producer = (async function () {
                await queue.write(1);
                await queue.write(2);
                await queue.write(3);
                queue.end();
            })();

            const items = await collect(queue);
            await producer;

            expect(items).to.deep.equal([1, 2, 3]);
        });

        it('hands a chunk straight to a parked consumer (direct handoff)', async function () {
            const queue = new AsyncIteratorQueue<number>();
            const iterator = queue[Symbol.asyncIterator]();

            const read = settle(iterator.next());
            await queue.write(42);

            const result = await read;
            expect(result).to.deep.equal({ status: 'resolved', value: { value: 42, done: false } });
        });

        it('reports lifecycle state through the predicates', function () {
            const queue = new AsyncIteratorQueue<number>();
            expect(queue.isReady()).to.equal(true);
            expect(queue.isDone()).to.equal(false);

            queue.end();
            expect(queue.isReady()).to.equal(false);
            expect(queue.isFinished()).to.equal(true);
            expect(queue.isDone()).to.equal(true);
            expect(queue.isAborted()).to.equal(false);
            expect(queue.isConsumerFailed()).to.equal(false);
        });
    });

    describe('backpressure', function () {
        it('resolves write() immediately while under the high-water mark', async function () {
            const queue = new AsyncIteratorQueue<number>({ highWaterMark: 15, size: () => 10 });

            expect(await isPending(queue.write(1))).to.equal(false);
        });

        it('parks write() once over the high-water mark and releases it on read', async function () {
            const queue = new AsyncIteratorQueue<number>({ highWaterMark: 15, size: () => 10 });
            const iterator = queue[Symbol.asyncIterator]();

            await queue.write(1); // queued size 10, under the mark
            const parked = queue.write(2); // queued size 20, over the mark
            expect(await isPending(parked)).to.equal(true);

            // Draining one chunk drops us back under the mark and unblocks the producer.
            await iterator.next();
            expect((await settle(parked)).status).to.equal('resolved');
        });

        it('defaults the object high-water mark to 16 items', async function () {
            const queue = new AsyncIteratorQueue<{ n: number }>();

            for (let i = 0; i < 16; i++) {
                expect(await isPending(queue.write({ n: i }))).to.equal(false);
            }
            // The 17th item tips the buffer over the default mark of 16.
            expect(await isPending(queue.write({ n: 16 }))).to.equal(true);
        });

        it('defaults the byte high-water mark to 64 KiB', async function () {
            const queue = new AsyncIteratorQueue<Uint8Array>();

            // A single chunk at exactly the mark stays under (strictly greater parks).
            expect(await isPending(queue.write(new Uint8Array(64 * 1024)))).to.equal(false);
            expect(await isPending(queue.write(new Uint8Array(1)))).to.equal(true);
        });
    });

    describe('graceful end()', function () {
        it('drains already-buffered chunks before delivering the final value', async function () {
            const queue = new AsyncIteratorQueue<string, string>({ highWaterMark: 1000 });
            const iterator = queue[Symbol.asyncIterator]();

            await queue.write('a');
            await queue.write('b');
            queue.end('FINAL');

            expect(await iterator.next()).to.deep.equal({ value: 'a', done: false });
            expect(await iterator.next()).to.deep.equal({ value: 'b', done: false });
            expect(await iterator.next()).to.deep.equal({ value: 'FINAL', done: true });
            expect(await iterator.next()).to.deep.equal({ value: 'FINAL', done: true });
            expect(queue.finalValue).to.equal('FINAL');
        });

        it('is a no-op when the queue is already closed', function () {
            const queue = new AsyncIteratorQueue<number, string>();
            queue.end('first');
            queue.end('second');
            expect(queue.finalValue).to.equal('first');
        });
    });

    describe('abort()', function () {
        it('surfaces the error to an unparked reader exactly once, then ends', async function () {
            const queue = new AsyncIteratorQueue<number>();
            const iterator = queue[Symbol.asyncIterator]();
            const reason = new Error('boom');

            queue.abort(reason);

            const first = await settle(iterator.next());
            expect(first.status).to.equal('rejected');
            if (first.status === 'rejected') {
                expect(first.reason).to.equal(reason);
            }
            expect(await iterator.next()).to.deep.equal({ value: undefined, done: true });
            expect(await iterator.next()).to.deep.equal({ value: undefined, done: true });
        });

        it('rejects a parked reader once and does not re-deliver the error', async function () {
            const queue = new AsyncIteratorQueue<number>();
            const iterator = queue[Symbol.asyncIterator]();
            const reason = new Error('boom');

            const parked = settle(iterator.next());
            queue.abort(reason);

            const parkedResult = await parked;
            expect(parkedResult.status).to.equal('rejected');
            if (parkedResult.status === 'rejected') {
                expect(parkedResult.reason).to.equal(reason);
            }
            // Already delivered to the parked read, so the next call just ends.
            expect(await iterator.next()).to.deep.equal({ value: undefined, done: true });
        });

        it('drops buffered chunks', async function () {
            const queue = new AsyncIteratorQueue<string>({ highWaterMark: 1000 });
            const iterator = queue[Symbol.asyncIterator]();

            await queue.write('a');
            await queue.write('b');
            queue.abort(new Error('boom'));

            expect((await settle(iterator.next())).status).to.equal('rejected');
            expect(await iterator.next()).to.deep.equal({ value: undefined, done: true });
        });

        it('defaults to an Error reason when none is given', async function () {
            const queue = new AsyncIteratorQueue<number>();
            const iterator = queue[Symbol.asyncIterator]();

            queue.abort();

            const result = await settle(iterator.next());
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.be.instanceOf(Error);
            }
            expect(queue.isAborted()).to.equal(true);
        });

        it('rejects a parked write() when aborted', async function () {
            const queue = new AsyncIteratorQueue<number>({ highWaterMark: 15, size: () => 10 });
            await queue.write(1); // queued size 10, under the mark
            const parked = queue.write(2); // queued size 20, parks over the mark

            queue.abort(new Error('boom'));

            expect((await settle(parked)).status).to.equal('rejected');
        });
    });

    describe('abort via signal', function () {
        it('rejects a parked reader with the signal reason', async function () {
            const controller = new AbortController();
            const queue = new AsyncIteratorQueue<number>({ signal: controller.signal });
            const iterator = queue[Symbol.asyncIterator]();
            const reason = new Error('cancelled');

            const parked = settle(iterator.next());
            controller.abort(reason);

            const result = await parked;
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
            expect(queue.isAborted()).to.equal(true);
        });
    });

    describe('writing after close', function () {
        it('rejects write() after end()', async function () {
            const queue = new AsyncIteratorQueue<number>();
            queue.end();
            expect((await settle(queue.write(1))).status).to.equal('rejected');
        });

        it('rejects write() after abort()', async function () {
            const queue = new AsyncIteratorQueue<number>();
            queue.abort(new Error('boom'));
            expect((await settle(queue.write(1))).status).to.equal('rejected');
        });
    });

    describe('single consumer', function () {
        it('throws when iterated a second time', function () {
            const queue = new AsyncIteratorQueue<number>();
            queue[Symbol.asyncIterator]();
            expect(() => queue[Symbol.asyncIterator]()).to.throw('single consumer');
        });
    });

    describe('consumer-driven termination', function () {
        it('ends the queue and releases a parked producer when the consumer returns early', async function () {
            const queue = new AsyncIteratorQueue<number>({ highWaterMark: 15, size: () => 10 });
            const iterator = queue[Symbol.asyncIterator]();

            await queue.write(1); // queued size 10, under the mark
            const parked = queue.write(2); // queued size 20, parks over the mark

            const returnResult = await (iterator.return?.() ?? Promise.resolve({ value: undefined, done: true as const }));

            expect(returnResult).to.deep.equal({ value: undefined, done: true });
            expect(queue.isFinished()).to.equal(true);
            // The parked producer is released (rejected) rather than hanging forever.
            expect((await settle(parked)).status).to.equal('rejected');
        });

        it('propagates a consumer throw() to the caller and then ends', async function () {
            const queue = new AsyncIteratorQueue<number>();
            const iterator = queue[Symbol.asyncIterator]();
            const reason = new Error('consumer boom');

            const thrown = await settle(iterator.throw?.(reason) ?? Promise.resolve({ value: undefined, done: true as const }));
            expect(thrown.status).to.equal('rejected');
            if (thrown.status === 'rejected') {
                expect(thrown.reason).to.equal(reason);
            }

            expect(queue.isConsumerFailed()).to.equal(true);
            expect(await iterator.next()).to.deep.equal({ value: undefined, done: true });
        });
    });
});
