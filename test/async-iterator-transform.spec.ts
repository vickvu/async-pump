import { expect } from 'chai';
import { AsyncIteratorTransformer } from '#SRC/async-iterator-transform.ts';
import { AsyncIteratorQueue } from '#SRC/async-iterator-queue.ts';
import { settle, collect, fromArray, macrotask } from './helpers.ts';

describe('AsyncIteratorTransformer', function () {
    describe('mapping and filtering', function () {
        it('maps every item with a synchronous transform', async function () {
            const transformer = new AsyncIteratorTransformer({ source: fromArray([1, 2, 3]), transform: (n) => n * 2 });
            expect(await collect(transformer)).to.deep.equal([2, 4, 6]);
        });

        it('maps every item with an asynchronous transform', async function () {
            const transformer = new AsyncIteratorTransformer({ source: fromArray([1, 2, 3]), transform: (n) => Promise.resolve(`#${String(n)}`) });
            expect(await collect(transformer)).to.deep.equal(['#1', '#2', '#3']);
        });

        it('changes the element type', async function () {
            const transformer = new AsyncIteratorTransformer({ source: fromArray([1, 22, 333]), transform: (n) => String(n).length });
            expect(await collect(transformer)).to.deep.equal([1, 2, 3]);
        });

        it('skips items whose transform returns null', async function () {
            const transformer = new AsyncIteratorTransformer({ source: fromArray([1, 2, 3, 4]), transform: (n) => (n % 2 === 0 ? n : null) });
            expect(await collect(transformer)).to.deep.equal([2, 4]);
        });

        it('skips items whose transform returns undefined', async function () {
            const transformer = new AsyncIteratorTransformer({ source: fromArray([1, 2, 3, 4]), transform: (n) => (n % 2 === 0 ? n : undefined) });
            expect(await collect(transformer)).to.deep.equal([2, 4]);
        });

        it('skips items whose async transform resolves to null', async function () {
            const transformer = new AsyncIteratorTransformer({ source: fromArray([1, 2, 3, 4]), transform: (n) => Promise.resolve(n > 2 ? n : null) });
            expect(await collect(transformer)).to.deep.equal([3, 4]);
        });

        it('combines mapping and filtering in one pass', async function () {
            const transformer = new AsyncIteratorTransformer({ source: fromArray([1, 2, 3, 4, 5, 6]), transform: (n) => (n % 2 === 0 ? n * 10 : null) });
            expect(await collect(transformer)).to.deep.equal([20, 40, 60]);
        });

        it('produces nothing from an empty source', async function () {
            const transformer = new AsyncIteratorTransformer<number, number>({ source: fromArray<number>([]), transform: (n) => n });
            expect(await collect(transformer)).to.deep.equal([]);
        });
    });

    describe('identity pass-through (no transform)', function () {
        it('forwards every item unchanged when transform is omitted', async function () {
            const transformer = new AsyncIteratorTransformer<number, number>({ source: fromArray([1, 2, 3]) });
            expect(await collect(transformer)).to.deep.equal([1, 2, 3]);
        });

        it('defaults SRC to Uint8Array for a byte pass-through', async function () {
            const a = new Uint8Array([1, 2]);
            const b = new Uint8Array([3, 4]);
            const transformer = new AsyncIteratorTransformer({ source: fromArray([a, b]) });
            expect(await collect(transformer)).to.deep.equal([a, b]);
        });

        it('wraps a source with abort support even without a transform', async function () {
            const controller = new AbortController();
            const reason = new Error('cancelled');
            async function* slow(): AsyncGenerator<number> {
                yield 1;
                await new Promise<void>(() => undefined); // never settles
                yield 2;
            }
            const transformer = new AsyncIteratorTransformer<number, number>({ source: slow(), signal: controller.signal });
            const iterator = transformer[Symbol.asyncIterator]();

            expect(await iterator.next()).to.deep.equal({ value: 1, done: false });
            const pending = settle(iterator.next());
            await macrotask();
            controller.abort(reason);

            const result = await pending;
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
        });
    });

    describe('error propagation', function () {
        it('propagates an error thrown by the source', async function () {
            const reason = new Error('source boom');
            async function* boom(): AsyncGenerator<number> {
                yield 1;
                await Promise.resolve();
                throw reason;
            }
            const transformer = new AsyncIteratorTransformer({ source: boom(), transform: (n) => n });

            const result = await settle(collect(transformer));
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
        });

        it('propagates an error thrown by the transform', async function () {
            const reason = new Error('transform boom');
            const transformer = new AsyncIteratorTransformer({
                source: fromArray([1, 2, 3]),
                transform: (n) => {
                    if (n === 2) {
                        throw reason;
                    }
                    return n;
                },
            });

            const result = await settle(collect(transformer));
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
        });
    });

    describe('early termination', function () {
        it('forwards a consumer break to the source iterator', async function () {
            let cleanedUp = false;
            async function* source(): AsyncGenerator<number> {
                await Promise.resolve();
                try {
                    yield 1;
                    yield 2;
                    yield 3;
                } finally {
                    cleanedUp = true;
                }
            }

            const transformer = new AsyncIteratorTransformer({ source: source(), transform: (n) => n });
            for await (const value of transformer) {
                expect(value).to.equal(1);
                break;
            }

            expect(cleanedUp).to.equal(true);
        });
    });

    describe('abort signal', function () {
        it('passes the provided signal to the transform function', async function () {
            const controller = new AbortController();
            let received: AbortSignal | undefined;
            const transformer = new AsyncIteratorTransformer({
                source: fromArray([1]),
                transform: (n, signal) => {
                    received = signal;
                    return n;
                },
                signal: controller.signal,
            });

            await collect(transformer);
            expect(received).to.equal(controller.signal);
        });

        it('passes undefined to the transform when no signal is given', async function () {
            let received: AbortSignal | undefined | 'unset' = 'unset';
            const transformer = new AsyncIteratorTransformer({
                source: fromArray([1]),
                transform: (n, signal) => {
                    received = signal;
                    return n;
                },
            });

            await collect(transformer);
            expect(received).to.equal(undefined);
        });

        it('rejects immediately and never calls transform when already aborted', async function () {
            const controller = new AbortController();
            controller.abort(new Error('pre-aborted'));
            let called = false;
            const transformer = new AsyncIteratorTransformer({
                source: fromArray([1, 2]),
                transform: (n) => {
                    called = true;
                    return n;
                },
                signal: controller.signal,
            });

            const result = await settle(collect(transformer));
            expect(result.status).to.equal('rejected');
            expect(called).to.equal(false);
        });

        it('interrupts a pull that is in flight', async function () {
            const controller = new AbortController();
            const reason = new Error('cancelled');
            async function* slow(): AsyncGenerator<number> {
                yield 1;
                await new Promise<void>(() => undefined); // never settles
                yield 2;
            }
            const transformer = new AsyncIteratorTransformer({ source: slow(), transform: (n) => n, signal: controller.signal });
            const iterator = transformer[Symbol.asyncIterator]();

            expect(await iterator.next()).to.deep.equal({ value: 1, done: false });
            const pending = settle(iterator.next());
            await macrotask();
            controller.abort(reason);

            const result = await pending;
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
        });

        it('interrupts a transform that is in flight', async function () {
            const controller = new AbortController();
            const reason = new Error('cancelled');
            const transformer = new AsyncIteratorTransformer<number, number>({
                source: fromArray([1]),
                transform: () => new Promise<number>(() => undefined), // never resolves
                signal: controller.signal,
            });
            const iterator = transformer[Symbol.asyncIterator]();

            const pending = settle(iterator.next());
            await macrotask();
            controller.abort(reason);

            const result = await pending;
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
        });
    });

    describe('integration', function () {
        it('transforms an AsyncIteratorQueue source end to end', async function () {
            const queue = new AsyncIteratorQueue<number>();
            const transformer = new AsyncIteratorTransformer({ source: queue, transform: (n) => (n === 0 ? null : n * n) });

            const producer = (async function () {
                await queue.write(0);
                await queue.write(2);
                await queue.write(3);
                queue.end();
            })();

            const items = await collect(transformer);
            await producer;

            expect(items).to.deep.equal([4, 9]);
        });
    });
});
