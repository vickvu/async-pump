import { expect } from 'chai';
import { EventEmitter } from 'node:events';
import { isNodeWritableLike, NodeWritableWrapper, type NodeWritableLike } from '#SRC/node.ts';
import { settle, isPending } from './helpers.ts';

/**
 * A scriptable Node-writable stand-in: control what `write()` returns (to force
 * backpressure) and emit lifecycle events on demand.
 */
class FakeWritable<T> implements NodeWritableLike<T> {
    written: T[] = [];
    ended = false;
    destroyed = false;
    destroyError: unknown;
    #emitter = new EventEmitter();
    #writeReturn: boolean;
    #throwOnWrite: unknown;

    constructor(writeReturn = true) {
        this.#writeReturn = writeReturn;
    }

    setWriteReturn(value: boolean): void {
        this.#writeReturn = value;
    }

    throwOnNextWrite(error: unknown): void {
        this.#throwOnWrite = error;
    }

    write(chunk: T): boolean {
        if (this.#throwOnWrite != null) {
            const error = this.#throwOnWrite;
            this.#throwOnWrite = undefined;
            // Re-throw whatever the test scripted, mimicking a stream that throws on write().
            /* eslint-disable-next-line @typescript-eslint/only-throw-error */
            throw error;
        }
        this.written.push(chunk);
        return this.#writeReturn;
    }

    end(): void {
        this.ended = true;
    }

    destroy(error?: unknown): void {
        this.destroyed = true;
        this.destroyError = error;
    }

    on(event: string, listener: (...args: unknown[]) => void): unknown {
        this.#emitter.on(event, listener);
        return this;
    }

    off(event: string, listener: (...args: unknown[]) => void): unknown {
        this.#emitter.off(event, listener);
        return this;
    }

    emit(event: string, ...args: unknown[]): void {
        this.#emitter.emit(event, ...args);
    }

    listenerCount(event: string): number {
        return this.#emitter.listenerCount(event);
    }
}

describe('node', function () {
    describe('isNodeWritableLike', function () {
        it('accepts an object with write() and end()', function () {
            expect(isNodeWritableLike({ write: () => true, end: () => undefined })).to.equal(true);
        });

        it('rejects null, plain objects, and Web writable streams', function () {
            expect(isNodeWritableLike(null)).to.equal(false);
            expect(isNodeWritableLike({})).to.equal(false);
            expect(isNodeWritableLike({ getWriter: () => undefined })).to.equal(false);
        });
    });

    describe('NodeWritableWrapper', function () {
        it('resolves write() synchronously while the stream has room', async function () {
            const fake = new FakeWritable<string>(true);
            const wrapper = new NodeWritableWrapper<string>(fake, true);

            await wrapper.write('a');

            expect(fake.written).to.deep.equal(['a']);
        });

        it('parks write() until a drain event when the stream is full', async function () {
            const fake = new FakeWritable<string>(false);
            const wrapper = new NodeWritableWrapper<string>(fake, true);

            const pending = wrapper.write('a');
            expect(await isPending(pending)).to.equal(true);

            fake.emit('drain');
            const result = await settle(pending);
            expect(result.status).to.equal('resolved');
            expect(fake.written).to.deep.equal(['a']);
        });

        it('resolves a parked write() on close and on finish too', async function () {
            const onClose = new FakeWritable<string>(false);
            const closeWrapper = new NodeWritableWrapper<string>(onClose, true);
            const closePending = settle(closeWrapper.write('a'));
            onClose.emit('close');
            expect((await closePending).status).to.equal('resolved');

            const onFinish = new FakeWritable<string>(false);
            const finishWrapper = new NodeWritableWrapper<string>(onFinish, true);
            const finishPending = settle(finishWrapper.write('a'));
            onFinish.emit('finish');
            expect((await finishPending).status).to.equal('resolved');
        });

        it('rejects a parked write() when the stream emits error', async function () {
            const fake = new FakeWritable<string>(false);
            const wrapper = new NodeWritableWrapper<string>(fake, true);
            const reason = new Error('stream failed');

            const pending = settle(wrapper.write('a'));
            fake.emit('error', reason);

            const result = await pending;
            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
        });

        it('rejects write() when the underlying write() throws synchronously', async function () {
            const fake = new FakeWritable<string>(true);
            const wrapper = new NodeWritableWrapper<string>(fake, true);
            const reason = new Error('boom');
            fake.throwOnNextWrite(reason);

            const result = await settle(wrapper.write('a'));

            expect(result.status).to.equal('rejected');
            if (result.status === 'rejected') {
                expect(result.reason).to.equal(reason);
            }
        });

        it('ends the stream on end() only when shouldEnd is true', async function () {
            const ending = new FakeWritable<string>(true);
            await new NodeWritableWrapper<string>(ending, true).end();
            expect(ending.ended).to.equal(true);

            const open = new FakeWritable<string>(true);
            await new NodeWritableWrapper<string>(open, false).end();
            expect(open.ended).to.equal(false);
        });

        it('detaches every listener on cleanup', async function () {
            const fake = new FakeWritable<string>(true);
            const wrapper = new NodeWritableWrapper<string>(fake, true);
            expect(fake.listenerCount('drain')).to.equal(1);

            await wrapper.cleanup(false);

            expect(fake.listenerCount('drain')).to.equal(0);
            expect(fake.listenerCount('close')).to.equal(0);
            expect(fake.listenerCount('finish')).to.equal(0);
            expect(fake.listenerCount('error')).to.equal(0);
            expect(fake.destroyed).to.equal(false);
        });

        it('destroys the stream on an error cleanup', async function () {
            const fake = new FakeWritable<string>(true);
            const wrapper = new NodeWritableWrapper<string>(fake, true);

            await wrapper.cleanup(true);

            expect(fake.destroyed).to.equal(true);
        });
    });
});
