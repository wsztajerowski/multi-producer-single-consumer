package pl.wsztajerowski;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

public class LockFreeMPSCWithBatch {

    private final AtomicReference<BufferBatch> currentBatchReference = new AtomicReference<>(new BufferBatch());
    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Consumer<ByteBuffer[]> processor;
    private final AtomicBoolean consumerAlive = new AtomicBoolean(true);

    public LockFreeMPSCWithBatch(Consumer<ByteBuffer[]> processor) {
        this.processor = processor;
    }

    public void stopConsumer() throws InterruptedException {
        consumerAlive.set(false);
        consumerExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void startConsumer() {


        consumerExecutor.submit(() -> {
            while (consumerAlive.get()) {

                var bufferBatch = currentBatchReference.get();

                var batchEmpty = bufferBatch.isBatchEmpty();

                if (!batchEmpty) {
                    lock.writeLock().lock();
                    try {
                        bufferBatch.finalizeBatch();
                    } finally {
                        lock.writeLock().unlock();
                    }
                    currentBatchReference.set(new BufferBatch());
                    processBatch(bufferBatch);
                }
            }
            System.out.println("Consumer stopped");
        });
    }

    private void processBatch(BufferBatch polledBatch) {
        ByteBuffer[] batchContent = polledBatch.getBatchContent();
        processor.accept(batchContent);
        polledBatch.sendDoneSignal();
    }

    public void produce(ByteBuffer content) {

        while (true) { //lock-free loop
            var bufferBatch = currentBatchReference.get();
            var batchFinalized = bufferBatch.isBatchFinalized();


            if (!batchFinalized) {
                lock.readLock().lock();
                // <-- tu OS scheduler może odjebać
                try {
                    batchFinalized = bufferBatch.isBatchFinalized();
                    if (!batchFinalized) {
                        if (bufferBatch.byteBuffers.offer(content)) {
                            try {
                                bufferBatch.doneSignal.await();
                                return;
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    }
                } finally {
                    lock.readLock().unlock();
                }
            }
        }

    }

}
