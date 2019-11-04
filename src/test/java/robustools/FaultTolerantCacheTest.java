package robustools;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

public class FaultTolerantCacheTest
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
                return key + "v";
            });
    }

    @Test
    public void testLoad()
    {
        assertThat(cache.get("a"), is("av"));
        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(1));
    }

    @Test
    public void testInvalidateAndGetIfPresent()
    {
        assertThat(cache.getIfPresent("a"), is(nullValue()));
        assertThat(loadCount.get(), is(0));

        assertThat(cache.get("a"), is("av"));
        assertThat(loadCount.get(), is(1));

        assertThat(cache.invalidate("a"), is(true));
        assertThat(cache.getIfPresent("a"), is(nullValue()));
        assertThat(loadCount.get(), is(1));
    }

    @Test
    public void testInvalidateAll()
    {
        assertThat(cache.get("a"), is("av"));
        assertThat(cache.get("b"), is("bv"));
        assertThat(loadCount.get(), is(2));

        cache.invalidateAll();

        assertThat(cache.getIfPresent("a"), is(nullValue()));
        assertThat(cache.getIfPresent("b"), is(nullValue()));
        assertThat(loadCount.get(), is(2));
        assertThat(cache.get("a"), is("av"));
        assertThat(cache.get("b"), is("bv"));
        assertThat(loadCount.get(), is(4));
    }

    @Test
    public void testInvalidateWithKeys()
    {
        assertThat(cache.get("a"), is("av"));
        assertThat(cache.get("b"), is("bv"));
        assertThat(cache.get("c"), is("cv"));
        assertThat(loadCount.get(), is(3));

        cache.invalidateAll(Stream.of("a", "b").collect(toList()));

        assertThat(cache.getIfPresent("a"), is(nullValue()));
        assertThat(cache.getIfPresent("b"), is(nullValue()));
        assertThat(cache.getIfPresent("c"), is("cv"));
        assertThat(loadCount.get(), is(3));
        assertThat(cache.get("a"), is("av"));
        assertThat(cache.get("b"), is("bv"));
        assertThat(cache.get("c"), is("cv"));
        assertThat(loadCount.get(), is(5));
    }

    @Test
    public void testEvictionOrderByMaximumSize()
    {
        for (int i = 0; i < 7; i++) {
            cache.get("a" + i);
        }
        assertThat(cache.getIfPresent("a0"), is(nullValue()));
        assertThat(cache.getIfPresent("a1"), is(nullValue()));
        assertThat(cache.getIfPresent("a2"), is("a2v"));
        assertThat(cache.getIfPresent("a3"), is("a3v"));
        assertThat(cache.getIfPresent("a4"), is("a4v"));
        assertThat(cache.getIfPresent("a5"), is("a5v"));
        assertThat(cache.getIfPresent("a6"), is("a6v"));

        cache.get("a2");  // touch a2
        cache.getIfPresent("a3");  // touch a3
        cache.get("a7");
        cache.get("a8");

        assertThat(cache.getIfPresent("a0"), is(nullValue()));
        assertThat(cache.getIfPresent("a1"), is(nullValue()));
        assertThat(cache.getIfPresent("a2"), is("a2v"));
        assertThat(cache.getIfPresent("a3"), is("a3v"));
        assertThat(cache.getIfPresent("a4"), is(nullValue()));
        assertThat(cache.getIfPresent("a5"), is(nullValue()));
        assertThat(cache.getIfPresent("a6"), is("a6v"));
        assertThat(cache.getIfPresent("a7"), is("a7v"));
        assertThat(cache.getIfPresent("a8"), is("a8v"));
    }

    @Test
    public void testExpire()
            throws Exception
    {
        cache.get("a0");
        assertThat(loadCount.get(), is(1));

        Thread.sleep(1200);
        // soft-expired, still cached.
        assertThat(cache.getIfPresent("a0"), is("a0v"));
        assertThat(loadCount.get(), is(1));

        Thread.sleep(1000);
        // hard-expired, not cached.
        assertThat(cache.getIfPresent("a0"), is(nullValue()));
        assertThat(loadCount.get(), is(1));
    }

    @Test
    public void testSynchronousRefreshOnHardExpire()
            throws Exception
    {
        cache.get("a0");
        assertThat(loadCount.get(), is(1));

        Thread.sleep(2200);
        // hard-expired. Refresh runs.
        assertThat(cache.get("a0"), is("a0v"));
        assertThat(loadCount.get(), is(2));
    }

    @Test
    public void testSynchronousRefreshOnSoftExpire()
            throws Exception
    {
        cache.get("a0");
        assertThat(loadCount.get(), is(1));

        Thread.sleep(1200);
        // soft-expired. Refresh runs.
        assertThat(cache.get("a0"), is("a0v"));
        assertThat(loadCount.get(), is(2));
    }
}
