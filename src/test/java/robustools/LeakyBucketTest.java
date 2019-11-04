package robustools;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class LeakyBucketTest
{
    @Test
    public void testBurstByFlowIn()
    {
        LeakyBucket bucket = LeakyBucket.newBuilder()
            .capacity(3.0)
            .leakRate(0.0)
            .build();
        assertThat(bucket.getAvailableCapacity(), closeTo(3.0, 0.001));
        bucket.flowIn(1.0);
        assertThat(bucket.getAvailableCapacity(), closeTo(2.0, 0.001));
        bucket.flowIn(1.0);
        assertThat(bucket.getAvailableCapacity(), closeTo(1.0, 0.001));
        bucket.flowIn(1.0);
        assertThat(bucket.getAvailableCapacity(), closeTo(0.0, 0.001));
        bucket.flowIn(1.0);
        assertThat(bucket.getAvailableCapacity(), closeTo(0.0, 0.001));
    }

    @Test
    public void testBurstByTryFlowIn()
    {
        LeakyBucket bucket = LeakyBucket.newBuilder()
            .capacity(3.00001)
            .leakRate(0.0)
            .build();
        assertThat(bucket.tryFlowIn(1.0), is(true));
        assertThat(bucket.tryFlowIn(1.0), is(true));
        assertThat(bucket.tryFlowIn(1.0), is(true));
        assertThat(bucket.tryFlowIn(1.0), is(false));
        assertThat(bucket.tryFlowIn(1.0), is(false));
    }

    @Test
    public void testLeak() throws Exception
    {
        LeakyBucket bucket = LeakyBucket.newBuilder()
            .capacity(3.0)
            .leakRate(0.5)
            .build();
        Thread.sleep(1000);
        assertThat(bucket.getAvailableCapacity(), closeTo(3.0, 0.1));
        assertThat(bucket.tryFlowIn(1.0), is(true));
        assertThat(bucket.getAvailableCapacity(), closeTo(2.0, 0.1));
        Thread.sleep(1000);
        assertThat(bucket.tryFlowIn(1.0), is(true));
        assertThat(bucket.getAvailableCapacity(), closeTo(1.5, 0.1));
    }

    @Test
    public void testAllowNegativeCapacity()
    {
        LeakyBucket bucket = LeakyBucket.newBuilder()
            .capacity(1.001)
            .leakRate(0.0)
            .allowNegativeCapacity(-1.0)
            .build();
        assertThat(bucket.tryFlowIn(1.0), is(true));
        assertThat(bucket.getAvailableCapacity(), closeTo(0.0, 0.01));
        assertThat(bucket.tryFlowIn(1.0), is(false));
        bucket.flowIn(0.8);
        assertThat(bucket.getAvailableCapacity(), closeTo(-0.8, 0.01));
        bucket.flowIn(1.0);
        assertThat(bucket.getAvailableCapacity(), closeTo(-1.0, 0.01));
    }

    @Test
    public void testClear()
    {
        LeakyBucket bucket = LeakyBucket.newBuilder()
            .capacity(2.001)
            .leakRate(0.0)
            .allowNegativeCapacity(-1.0)
            .build();
        assertThat(bucket.tryFlowIn(1.0), is(true));
        assertThat(bucket.getAvailableCapacity(), closeTo(1.0, 0.01));
        bucket.clear();
        assertThat(bucket.getAvailableCapacity(), closeTo(2.0, 0.01));
    }
}
