import { expect } from 'chai';
import { isWebWritableLike, WebWritableWrapper } from '#SRC/web-api.ts';
import { settle } from './helpers.ts';

interface Sink<T> {
    stream: WritableStream<T>;
    chunks: T[];
    closed: () => boolean;
    aborted: () => boolean;
    abortReason: () => unknown;
}

function makeSink<T>(): Sink<T> {
    const chunks: T[] = [];
    let closed = false;
    let aborted = false;
    let abortReason: unknown;
    const stream = new WritableStream<T>({
        write(chunk) {
            chunks.push(chunk);
        },
        close() {
            closed = true;
        },
        abort(reason) {
            aborted = true;
            abortReason = reason;
        },
    });
    return { stream, chunks, closed: () => closed, aborted: () => aborted, abortReason: () => abortReason };
}

describe('web-api', function () {
    describe('isWebWritableLike', function () {
        it('accepts an object exposing getWriter()', function () {
            expect(isWebWritableLike({ getWriter: () => undefined })).to.equal(true);
        });

        it('rejects null, plain objects, and Node-style writables', function () {
            expect(isWebWritableLike(null)).to.equal(false);
            expect(isWebWritableLike({})).to.equal(false);
            expect(isWebWritableLike({ write: () => true, end: () => undefined })).to.equal(false);
        });
    });

    describe('WebWritableWrapper', function () {
        it('writes chunks through to the stream in order', async function () {
            const sink = makeSink<string>();
            const wrapper = new WebWritableWrapper<string>(sink.stream, true);

            await wrapper.write('a');
            await wrapper.write('b');

            expect(sink.chunks).to.deep.equal(['a', 'b']);
        });

        it('closes the stream on end() when shouldEnd is true', async function () {
            const sink = makeSink<string>();
            const wrapper = new WebWritableWrapper<string>(sink.stream, true);

            await wrapper.end();

            expect(sink.closed()).to.equal(true);
        });

        it('leaves the stream open on end() when shouldEnd is false', async function () {
            const sink = makeSink<string>();
            const wrapper = new WebWritableWrapper<string>(sink.stream, false);

            await wrapper.end();

            expect(sink.closed()).to.equal(false);
        });

        it('releases the writer lock on a clean cleanup', async function () {
            const sink = makeSink<string>();
            const wrapper = new WebWritableWrapper<string>(sink.stream, true);

            await wrapper.cleanup(false);

            // A released lock means the stream can be re-acquired without throwing.
            expect(() => sink.stream.getWriter()).to.not.throw();
            expect(sink.aborted()).to.equal(false);
        });

        it('aborts the stream before releasing the lock on an error cleanup', async function () {
            const sink = makeSink<string>();
            const wrapper = new WebWritableWrapper<string>(sink.stream, true);

            await wrapper.cleanup(true);

            expect(sink.aborted()).to.equal(true);
            // Lock was still released, so re-acquiring does not throw.
            expect(() => sink.stream.getWriter()).to.not.throw();
        });

        it('rejects write() once the underlying stream has errored', async function () {
            const sink = makeSink<string>();
            const wrapper = new WebWritableWrapper<string>(sink.stream, true);
            await wrapper.cleanup(true); // aborts the stream

            const result = await settle(wrapper.write('a'));
            expect(result.status).to.equal('rejected');
        });
    });
});
