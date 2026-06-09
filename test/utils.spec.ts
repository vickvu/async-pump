import { expect } from 'chai';
import { noop, nextMacroTask, getAbortPromise } from '#SRC/utils.ts';
import { settle, macrotask } from './helpers.ts';

describe('utils', function () {
    describe('noop', function () {
        it('is callable and does nothing', function () {
            expect(noop).to.not.throw();
        });
    });

    describe('nextMacroTask', function () {
        it('resolves on a later macrotask, after pending microtasks', async function () {
            const order: string[] = [];
            const macro = nextMacroTask().then(function () {
                order.push('macro');
            });
            await Promise.resolve().then(function () {
                order.push('micro');
            });
            await macro;
            expect(order).to.deep.equal(['micro', 'macro']);
        });

        it('falls back to setTimeout where setImmediate is unavailable (browser path)', async function () {
            const globals = globalThis as { setImmediate?: typeof setImmediate };
            const original = globals.setImmediate;
            delete globals.setImmediate;
            try {
                expect(globals.setImmediate).to.equal(undefined);
                await nextMacroTask();
            } finally {
                globals.setImmediate = original;
            }
        });
    });

    describe('getAbortPromise', function () {
        it('rejects with the signal reason when the signal aborts', async function () {
            const controller = new AbortController();
            const reason = new Error('stop');
            const { promise, cleanup } = getAbortPromise(controller.signal);

            controller.abort(reason);
            const result = await settle(promise);
            cleanup();

            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
        });

        it('stays pending forever once cleanup has detached the listener', async function () {
            const controller = new AbortController();
            const { promise, cleanup } = getAbortPromise(controller.signal);

            cleanup();
            controller.abort(new Error('ignored'));

            const outcome = await Promise.race([
                promise.then(
                    () => 'settled',
                    () => 'settled',
                ),
                macrotask().then(() => 'pending'),
            ]);
            expect(outcome).to.equal('pending');
        });
    });
});
