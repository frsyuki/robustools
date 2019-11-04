package robustools;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class FaultTolerantCacheConcurrentTest
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
            .refreshAfterWrite(Duration.ofSeconds(1))
            .expireAfterWrite(Duration.ofSeconds(2))
            .build((key) -> {
                loadCount.incrementAndGet();
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException ex) {
                }
                return key + "v";
            });
    }

    @Test
    public void testLockedLoad()
            throws Exception
    {
        new Thread(() -> {
            cache.get("a");
        }).start();
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(1));  // just once

        // soft-expired, concurrent synchronous refresh
        Thread.sleep(1200);
        new Thread(() -> {
            cache.get("a");
        }).start();
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(2));

        // hard-expired, concurrent synchronous refresh
        Thread.sleep(2200);
        new Thread(() -> {
            cache.get("a");
        }).start();
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(3));
    }

    @Test
    public void testLockedLoadWithRefresh()
            throws Exception
    {
        cache.get("a");

        // soft-expired, concurrent synchronous refresh
        new Thread(() -> {
            cache.get("a");
        }).start();
        cache.refreshNow();
        assertThat(loadCount.get(), is(2));

        // hard-expired, synchronous refresh does nothing
        Thread.sleep(2200);
        cache.refreshNow();
        assertThat(loadCount.get(), is(2));  // no refresh
    }
}
