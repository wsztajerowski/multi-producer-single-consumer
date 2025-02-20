package pl.wsztajerowski;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.II_Result;
import org.openjdk.jcstress.infra.results.JJJJ_Result;
import org.openjdk.jcstress.infra.results.JJ_Result;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import static org.openjdk.jcstress.annotations.Expect.*;

public class StressTest {


//    @Outcome(id = "1, 1", expect = FORBIDDEN, desc = "Both actors came up with the same value: atomicity failure.")
    @Outcome(id = "1, 1, 1, 1", expect = ACCEPTABLE, desc = "actor1 incremented, then actor2.")
    @JCStressTest
    @State
    public static class TestWithForbiddenResults {
        private LockFreeMPSCWithBatch batch;
        private ByteBuffer byteBuffer;

        public TestWithForbiddenResults(){
            BufferBatch.BATCH_SIZE = 2;
            batch = new LockFreeMPSCWithBatch(buffers -> {});
            byteBuffer = ByteBuffer.allocate(4);
        }

        @Actor
        public void actor1(JJJJ_Result r) {
            CountDownLatch countDownLatch = batch.produce(byteBuffer);
            if (countDownLatch != null){
                r.r1 = countDownLatch.getCount();
            } else {
                r.r1 = -1;
            }
            r.r2 = batch.getQueue().size();
        }

        @Actor
        public void actor2(JJJJ_Result r) {
            CountDownLatch countDownLatch = batch.produce(byteBuffer);
            if (countDownLatch != null){
                r.r3 = countDownLatch.getCount();
            } else {
                r.r3 = -1;
            }
            r.r4 = batch.getQueue().size();
        }
    }
}
