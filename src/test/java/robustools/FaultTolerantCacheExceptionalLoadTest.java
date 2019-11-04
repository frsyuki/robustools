package robustools;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Test;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class FaultTolerantCacheExceptionalLoadTest
{
    private FaultTolerantCache<String, String> cache;

    private volatile int allowBulk;
    private volatile boolean throwNext;
    private volatile boolean sleepNext;

    private AtomicInteger loadCount;
    private AtomicInteger reloadCount;

    @Before
    public void setup()
    {
        this.loadCount = new AtomicInteger(0);
        this.reloadCount = new AtomicInteger(0);

        this.allowBulk = 0;
        this.throwNext = false;
        this.sleepNext = false;

        this.cache = new FaultTolerantCache.Builder<String, String>()
            .maximumSize(5)
            .concurrencyLevel(1)
            .refreshAfterWrite(Duration.ofSeconds(1))
            .expireAfterWrite(Duration.ofSeconds(2))
            .reloader((keys, result) -> {
                for (String key : keys) {
                    int n = reloadCount.getAndIncrement();
                    if (allowBulk > 0) {
                        loadCount.getAndIncrement();
                        result.accept(key, key + "b");
                        allowBulk--;
                    }
                    else if (throwNext) {
                        throw new RuntimeException("Bulk fail at " + n);
                    }
                    else {
                        loadCount.getAndIncrement();
                        result.accept(key, key + "b");
                    }
                }
            })
            .build((key) -> {
                int n = loadCount.getAndIncrement();
                boolean throwThis = throwNext;
                if (sleepNext) {
                    try {
                        Thread.sleep(500);
                    }
                    catch (InterruptedException ex) {
                    }
                }
                if (throwThis) {
                    throw new RuntimeException("Fail at " + n);
                }
                else {
                    return key + "v";
                }
            });
    }

    @Test
    public void testExceptionalSynchronousLoad()
    {
        throwNext = true;
        try {
            cache.get("a");
            fail();
        }
        catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("Fail at 0"));
        }
        assertThat(cache.getIfPresent("a"), is(nullValue()));
    }

    @Test
    public void testExceptionalRefresh()
            throws Exception
    {
        assertThat(cache.get("a0"), is("a0v"));
        assertThat(cache.get("a1"), is("a1v"));
        assertThat(cache.get("a2"), is("a2v"));
        assertThat(cache.get("a3"), is("a3v"));
        assertThat(loadCount.get(), is(4));

        allowBulk = 2;
        throwNext = true;
        cache.refreshNow();  // no throw
        assertThat(loadCount.get(), is(6));

        // refreshed first 2 in access order
        assertThat(cache.get("a3"), is("a3b"));
        assertThat(cache.get("a2"), is("a2b"));
        assertThat(cache.get("a1"), is("a1v"));
        assertThat(cache.get("a0"), is("a0v"));

        throwNext = false;
        cache.refreshNow();
        assertThat(cache.get("a0"), is("a0b"));
        assertThat(cache.get("a1"), is("a1b"));
        assertThat(cache.get("a2"), is("a2b"));
        assertThat(cache.get("a3"), is("a3b"));
    }

    @Test
    public void testExceptionalBlockedRefresh()
            throws Exception
    {
        throwNext = true;
        sleepNext = true;
        new Thread(() -> {
            try {
                cache.get("a");
            }
            catch (RuntimeException ex) {
                // expected
            }
        }).start();
        Thread.sleep(500);

        throwNext = false;
        sleepNext = false;
        try {
            cache.get("a");
            fail();
        }
        catch (RuntimeException ex) {
            assertThat(ex.getMessage(), is("Fail at 0"));
        }
    }
}
