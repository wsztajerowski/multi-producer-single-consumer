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
    private final Queue<BufferBatch> queue = new ConcurrentLinkedQueue<>();
    private final AtomicReference<BufferBatch> currentBatchReference = new AtomicReference<>();
    private final ExecutorService consumerExecutor = Executors.newSingleThreadExecutor();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Consumer<ByteBuffer[]> processor;
    private final AtomicBoolean consumerAlive = new AtomicBoolean(true);

    public LockFreeMPSCWithBatch(Consumer<ByteBuffer[]> processor) {
        this.processor = processor;
        BufferBatch newValue = new BufferBatch();
        currentBatchReference.set(newValue);
        queue.add(newValue);
    }

    public void stopConsumer() throws InterruptedException {
        consumerAlive.set(false);
        consumerExecutor.awaitTermination(10, TimeUnit.SECONDS);
    }

    public void startConsumer() {
        consumerExecutor.submit(() -> {
            while (consumerAlive.get()) {
                BufferBatch peek = queue.peek();
                if (peek == null || peek.isBatchEmpty()) {
                    // smart sleep? Wait for semaphore / signal?
                    System.out.println("Batch is empty");
                    Thread.onSpinWait();
                    continue;
                }
                BufferBatch polledBatch = queue.poll();
                lock.writeLock().lock();
                try {
                    polledBatch.finalizeBatch();
                } finally {
                    lock.writeLock().unlock();
                }
                processBatch(polledBatch);
            }
            System.out.println("Consumer stopped");
        });
    }

    private void processBatch(BufferBatch polledBatch) {
        ByteBuffer[] batchContent = polledBatch.getBatchContent();
        processor.accept(batchContent);
        polledBatch.sendDoneSignal();
    }

    public CountDownLatch forceProduce(ByteBuffer content){
        while (true){
            CountDownLatch countDownLatch = produce(content);
            if (countDownLatch != null) {
                return countDownLatch;
            }
        }
    }

    public Queue<BufferBatch> getQueue() {
        return queue;
    }

    public CountDownLatch produce(ByteBuffer content) {
        BufferBatch currentBatch;
        CountDownLatch doneSignal;
        lock.readLock().lock();
        try {
            currentBatch = currentBatchReference.get();
            doneSignal = currentBatch.offer(content);
        } finally {
            lock.readLock().unlock();
        }
        if (doneSignal == null) {
            System.out.println("Batch is closed");
            BufferBatch newBatch = new BufferBatch();
            BufferBatch bufferBatch = currentBatchReference.compareAndExchange(currentBatch, newBatch);

            if (currentBatch.equals(bufferBatch)) {
                queue.add(newBatch);
            }
        }
        return doneSignal;
    }

}


