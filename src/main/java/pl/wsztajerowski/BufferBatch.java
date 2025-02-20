package pl.wsztajerowski;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public class BufferBatch {
    static int BATCH_SIZE = 1024; // Define batch size
    private final ArrayBlockingQueue<ByteBuffer> byteBuffers;
    private final CountDownLatch doneSignal;
    private volatile boolean batchFinalized;

    BufferBatch() {
        this.doneSignal = new CountDownLatch(1);
        batchFinalized = false;
        byteBuffers = new ArrayBlockingQueue<>(BATCH_SIZE);
    }

    public void finalizeBatch() {
        batchFinalized = true;
    }

    public void sendDoneSignal() {
        doneSignal.countDown();
    }

    public ByteBuffer[] getBatchContent() {
        return byteBuffers.toArray(new ByteBuffer[0]);
    }

    public boolean isBatchFinalized() {
        return batchFinalized;
    }

    public CountDownLatch getDoneSignal() {
        return doneSignal;
    }

    public CountDownLatch offer(ByteBuffer content) {
        if (!batchFinalized && byteBuffers.offer(content)) {
            return doneSignal;
        }
        batchFinalized = true;
        return null;
    }

    public boolean isBatchEmpty() {
        return byteBuffers.isEmpty();
    }
}
