import { expect } from 'chai';
import { Writable } from 'node:stream';
import { AsyncIteratorQueue } from '#SRC/async-iterator-queue.ts';
import { AsyncIteratorTransformer } from '#SRC/async-iterator-transform.ts';
import { AsyncIteratorWriter } from '#SRC/async-iterator-writer.ts';
import { settle, macrotask } from './helpers.ts';

interface LogRecord {
    level: string;
    msg: string;
}

describe('integration: queue -> transformer -> writer -> stream', function () {
    it('pumps produced records through a transform into a Node writable', async function () {
        const chunks: Buffer[] = [];
        const destination = new Writable({
            write(chunk: Buffer, _enc, cb) {
                chunks.push(Buffer.from(chunk));
                cb();
            },
        });

        const records = new AsyncIteratorQueue<LogRecord>();
        const encoder = new TextEncoder();
        const lines = new AsyncIteratorTransformer<LogRecord, Uint8Array>({
            source: records,
            transform: (record) => (record.level === 'debug' ? null : encoder.encode(`${record.level}: ${record.msg}\n`)),
        });
        const writer = new AsyncIteratorWriter({ source: lines, destination });

        const pump = writer.write();
        await records.write({ level: 'info', msg: 'started' });
        await records.write({ level: 'debug', msg: 'noisy' }); // dropped by the transform
        await records.write({ level: 'error', msg: 'boom' });
        records.end();
        await pump;

        expect(Buffer.concat(chunks).toString()).to.equal('info: started\nerror: boom\n');
        expect(destination.writableEnded).to.equal(true);
    });

    it('pumps produced items through a transform into a Web WritableStream', async function () {
        const written: string[] = [];
        let closed = false;
        const destination = new WritableStream<string>({
            write(chunk) {
                written.push(chunk);
            },
            close() {
                closed = true;
            },
        });

        const numbers = new AsyncIteratorQueue<number>();
        const evens = new AsyncIteratorTransformer<number, string>({
            source: numbers,
            transform: (n) => (n % 2 === 0 ? `even:${String(n)}` : null),
        });
        const writer = new AsyncIteratorWriter<string>({ source: evens, destination });

        const pump = writer.write();
        for (const n of [1, 2, 3, 4]) {
            await numbers.write(n);
        }
        numbers.end();
        await pump;

        expect(written).to.deep.equal(['even:2', 'even:4']);
        expect(closed).to.equal(true);
    });

    it('cancels the whole pipeline through a shared AbortSignal', async function () {
        const controller = new AbortController();
        // objectMode so the writable accepts plain numbers from the transform.
        const destination = new Writable({
            objectMode: true,
            write(_chunk, _enc, cb) {
                cb();
            },
        });

        const records = new AsyncIteratorQueue<number>({ signal: controller.signal });
        const transformed = new AsyncIteratorTransformer<number, number>({
            source: records,
            transform: (n) => n,
            signal: controller.signal,
        });
        const writer = new AsyncIteratorWriter({ source: transformed, destination, signal: controller.signal });

        const pump = settle(writer.write());
        await records.write(1); // flows through and is written
        await macrotask();
        const reason = new Error('cancel pipeline');
        controller.abort(reason);

        const result = await pump;
        expect(result.status).to.equal('rejected');
        if (result.status === 'rejected') {
            expect(result.reason).to.equal(reason);
        }
        expect(destination.destroyed).to.equal(true);
    });
});
