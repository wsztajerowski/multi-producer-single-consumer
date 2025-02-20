package pl.wsztajerowski;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class App {
    public static void main(String... args) throws Exception {
        new App().runProducers();
    }

    void runProducers() throws Exception {
        LockFreeMPSCWithBatch mpsc = new LockFreeMPSCWithBatch(buffers -> {
            System.out.println("Processing batch:");
            for (ByteBuffer msg : buffers) {
                System.out.println("Consumed: " + msg.getInt(0));
            }
            try {
                Thread.sleep(200); // Simulate batch processing delay
            } catch (InterruptedException e) {
                System.out.println("Consumer Interrupted");
                Thread.currentThread().interrupt();
            }
        });
        mpsc.startConsumer();

        try (ExecutorService producerPool = Executors.newFixedThreadPool(3)) {
            for (int i = 0; i < 9; i++) {
                int msgId = i;
                producerPool.submit(() -> {
                    ByteBuffer content = ByteBuffer.allocateDirect(10);
                    content.putInt(0, msgId);
                    CountDownLatch latch = mpsc.forceProduce(content);
                    try {
                        latch.await(); // Wait for batch to be processed
                        System.out.println("Producer confirmed: Message " + msgId);
                    } catch (InterruptedException e) {
                        System.out.println("Producer Interrupted");
                        Thread.currentThread().interrupt();
                    }
                });
            }
            producerPool.awaitTermination(10, TimeUnit.SECONDS);
            mpsc.stopConsumer();
        }
    }
}
