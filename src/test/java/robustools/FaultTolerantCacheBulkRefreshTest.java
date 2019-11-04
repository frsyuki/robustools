package robustools;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class FaultTolerantCacheBulkRefreshTest
{
    private FaultTolerantCache<String, String> cache;

    private AtomicInteger loadCount;
    private AtomicInteger reloadCount;

    @Before
    public void setup()
    {
        this.loadCount = new AtomicInteger(0);
        this.reloadCount = new AtomicInteger(0);

        this.cache = new FaultTolerantCache.Builder<String, String>()
            .maximumSize(5)
            .concurrencyLevel(1)
            .refreshAfterWrite(Duration.ofSeconds(1))
            .expireAfterWrite(Duration.ofSeconds(2))
            .reloader((keys, result) -> {
                reloadCount.incrementAndGet();
                for (String key : keys) {
                    result.accept(key, key + "b");
                }
            })
            .build((key) -> {
                loadCount.incrementAndGet();
                return key + "v";
            });
    }

    @Test
    public void testRefreshNow()
            throws Exception
    {
        for (int i = 0; i < 5; i++) {
            cache.get("a" + i);
        }

        cache.refreshNow();

        assertThat(loadCount.get(), is(5));
        assertThat(reloadCount.get(), is(1));

        assertThat(cache.getIfPresent("a0"), is("a0b"));
        assertThat(cache.getIfPresent("a1"), is("a1b"));
        assertThat(cache.getIfPresent("a2"), is("a2b"));
        assertThat(cache.getIfPresent("a3"), is("a3b"));
        assertThat(cache.getIfPresent("a4"), is("a4b"));
    }

    @Test
    public void testRefresh()
            throws Exception
    {
        for (int i = 0; i < 5; i++) {
            cache.get("a" + i);
        }

        cache.refresh();
        Thread.sleep(500);

        // no reload happens
        assertThat(loadCount.get(), is(5));
        assertThat(reloadCount.get(), is(0));

        // soft-expired. should be refreshed
        Thread.sleep(1000);

        cache.refresh();
        Thread.sleep(500);

        assertThat(loadCount.get(), is(5));
        assertThat(reloadCount.get(), is(1));

        assertThat(cache.getIfPresent("a0"), is("a0b"));
        assertThat(cache.getIfPresent("a1"), is("a1b"));
        assertThat(cache.getIfPresent("a2"), is("a2b"));
        assertThat(cache.getIfPresent("a3"), is("a3b"));
        assertThat(cache.getIfPresent("a4"), is("a4b"));
    }
}
