package robustools;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class FaultTolerantCacheAsyncTest
{
    private FaultTolerantCache<String, String> cache;

    private AtomicInteger loadCount;

    @Before
    public void setup()
    {
        this.loadCount = new AtomicInteger(0);

        this.cache = new FaultTolerantCache.Builder<String, String>()
            .maximumSize(5)
            .concurrencyLevel(1)
            .asynchronousRefreshAfterWrite(Duration.ofSeconds(1))
            .refreshAfterWrite(Duration.ofSeconds(2))
            .expireAfterWrite(Duration.ofSeconds(3))
            .build((key) -> {
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException ex) {
                }
                loadCount.incrementAndGet();
                return key + "v";
            });
    }

    @Test
    public void testSyncLoadWhenNotCached()
    {
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(1));
    }

    @Test
    public void testSyncLoadWhenExpired()
            throws Exception
    {
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(1));

        Thread.sleep(2200);

        // Hard-expired. Synchronous load runs
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(2));
    }

    @Test
    public void testAsyncLoad()
            throws Exception
    {
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(1));

        Thread.sleep(1200);

        // Soft-expired. Asynchronous load starts
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(1));

        // Asynchronous load completes
        Thread.sleep(1000);
        assertThat(loadCount.get(), is(2));
    }
}
