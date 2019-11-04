package robustools;

import robustools.LockedRefresher.RefreshableValue;
import robustools.LockedRefresher.ValueVersion;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import static java.util.Objects.requireNonNull;

/**
 * A cache that returns cached entry as much as possible when reloading fails.
 * <br>
 * FaultTolerantCache is useful when your system load data from a remote server
 * and your system should be alive when the remote server is down. When the
 * remote server system is down, you see cached entries for longer time than
 * regular expiration time.
 * <br>
 * The worst case is that the remote server is not completely down but extremely
 * slow. To isolate the impact, FaultTolerantCache refreshes cached entry using
 * background threads. FaultTolerantCache uses foreground thread only when
 * cached data is older than configured hard limit time or data is not even
 * cached so that you receive the exception passed through from the cache loader
 * function.
 *
 */
public class FaultTolerantCache<K, V>
{
    public static class Builder<K, V>
    {
        int maximumSize = 0;

        int concurrencyLevel = 4;

        Duration asynchronousRefreshAfterWrite = null;

        Duration refreshAfterWrite = null;

        Duration expireAfterWrite = null;

        Executor executor = null;

        Function<K, V> loader = null;

        BiConsumer<Collection<K>, BiConsumer<K, V>> reloader = null;

        int bulkReloadSizeLimit = 100;

        boolean useFailureRateLimit = false;

        double failureCapacity;

        double failureRateLimit;

        Consumer<Throwable> exceptionListener = null;

        /**
         * Maximum number of entries in cache.
         *
         * When more than this number of entries are loaded in the cache,
         * the least accessed entry is removed from the cache.
         */
        public Builder<K, V> maximumSize(int maximumSize)
        {
            this.maximumSize = maximumSize;
            return this;
        }

        /**
         * Number of lock segments.
         *
         * This option is less common to configure. Default is 0.
         */
        public Builder<K, V> concurrencyLevel(int concurrencyLevel)
        {
            this.concurrencyLevel = concurrencyLevel;
            return this;
        }

        /**
         * Duration to trigger background maintenance refresh of a cached entry on a use of the entry.
         *
         * Failure of a maintenance refresh doesn't throw exceptions.
         * By default, background maintenance refresh is disabled.
         * Usually, this duration should be shorter than refreshAfterWrite.
         */
        public Builder<K, V> asynchronousRefreshAfterWrite(Duration asynchronousRefreshAfterWrite)
        {
            this.asynchronousRefreshAfterWrite = asynchronousRefreshAfterWrite;
            return this;
        }

        /**
         * Duration to trigger foreground maintenance refresh of a cached entry on a use of the entry.
         *
         * Failure of a maintenance refresh doesn't throw exceptions.
         * By default, foreground maintenance refresh is disabled.
         * Usually, this duration should be shorter than expireAfterWrite.
         */
        public Builder<K, V> refreshAfterWrite(Duration refreshAfterWrite)
        {
            this.refreshAfterWrite = refreshAfterWrite;
            return this;
        }

        /**
         * Duration to trigger foreground refresh of a cached entry on a use of the entry.
         *
         * Failure of a this refresh throws exceptions. This duration should be long enough.
         */
        public Builder<K, V> expireAfterWrite(Duration expireAfterWrite)
        {
            this.expireAfterWrite = expireAfterWrite;
            return this;
        }

        /**
         * Override executor to run reloader.
         *
         * Default is ForkJoinPool.commonPool().
         */
        public Builder<K, V> executor(Executor executor)
        {
            this.executor = executor;
            return this;
        }

        /**
         * Set a bulk cache reloader.
         *
         * Bulk cache reloader is called instead of loader when multiple keys are
         * queued for maintenance refresh. Enqueuing keys happen when refresh or
         * refreshNow method is called, or when get or getIfPresent method
         * is called with asynchronousRefreshAfterWrite enabled.
         */
        public <NewK extends K, NewV extends V> Builder<NewK, NewV> reloader(BiConsumer<Collection<NewK>, BiConsumer<NewK, NewV>> reloader)
        {
            @SuppressWarnings("unchecked")
            Builder<NewK, NewV> self = (Builder<NewK, NewV>) this;
            self.reloader = reloader;
            return self;
        }

        /**
         * Maximum number of keys given to the reloader at once.
         *
         * If smaller number is set here, reloader is called for more times.
         */
        public Builder<K, V> bulkReloadSizeLimit(int bulkReloadSizeLimit)
        {
            this.bulkReloadSizeLimit = bulkReloadSizeLimit;
            return this;
        }

        /**
         * Enable failure rate limiting using LeakyBucket.
         *
         * When failure rate limit is enabled, attempt to call bulk reloader is skipped when
         * reloader fails too frequently. Such skipped attempt is assumed as a repeating of
         * the same exception without actually calling the reloader.
         */
        public Builder<K, V> failureRateLimit(double burstLimit, double allowedFailuresPerSecond)
        {
            this.useFailureRateLimit = true;
            this.failureCapacity = burstLimit;
            this.failureRateLimit = allowedFailuresPerSecond;
            return this;
        }

        public Builder<K, V> exceptionListener(Consumer<Throwable> exceptionListener)
        {
            this.exceptionListener = exceptionListener;
            return this;
        }

        /**
         * Creates a new FaultTolerantCache with the given loader.
         */
        public <NewK extends K, NewV extends V> FaultTolerantCache<NewK, NewV> build(Function<NewK, NewV> loader)
        {
            @SuppressWarnings("unchecked")
            Builder<NewK, NewV> self = (Builder<NewK, NewV>) this;
            self.loader = loader;
            return new FaultTolerantCache<NewK, NewV>(self);
        }
    }

    public static class FailureRateLimitException
            extends RuntimeException
    {
        public FailureRateLimitException()
        {
            super("Failure rate limit reached");
        }
    }

    /**
     * Creates a new builder to build FaultTolerantCache.
     */
    public static Builder<Object, Object> newBuilder()
    {
        return new Builder<Object, Object>();
    }

    private static long EXPIRE_REFRESH_DISABLED = -1L;

    private final Segment[] segments;
    private final int maximumSizePerSegment;  // 0 to disable
    private final long expireAfterWriteMillis;
    private final long asynchronousRefreshAfterWriteMillis;
    private final long refreshAfterWriteMillis;
    private final Executor executor;
    private final LockedRefresher<K, V> lockedRefresher;
    private final RefreshQueue<K, V> refreshQueue;

    @SuppressWarnings("unchecked")
    FaultTolerantCache(Builder<K, V> builder)
    {
        int numSegments = Math.max(builder.concurrencyLevel, 1);
        if (builder.maximumSize == 0) {
            this.maximumSizePerSegment = 0;
        }
        else {
            this.maximumSizePerSegment = Math.max((builder.maximumSize + numSegments - 1) / numSegments, 1);
        }
        this.expireAfterWriteMillis = (builder.expireAfterWrite == null ? EXPIRE_REFRESH_DISABLED
                : builder.expireAfterWrite.toMillis());
        this.asynchronousRefreshAfterWriteMillis = (builder.asynchronousRefreshAfterWrite == null ? EXPIRE_REFRESH_DISABLED
                : builder.asynchronousRefreshAfterWrite.toMillis());
        this.refreshAfterWriteMillis = (builder.refreshAfterWrite == null ? EXPIRE_REFRESH_DISABLED
                : builder.refreshAfterWrite.toMillis());
        this.executor = (builder.executor == null ? ForkJoinPool.commonPool() :
                builder.executor);
        LeakyBucket failureRateLimit;
        if (builder.useFailureRateLimit) {
            failureRateLimit = LeakyBucket.newBuilder()
                .capacity(builder.failureCapacity)
                .leakRate(builder.failureRateLimit)
                .build();
        }
        else {
            failureRateLimit = null;
        }
        this.lockedRefresher = new LockedRefresher<>(builder.loader, builder.reloader, failureRateLimit, builder.exceptionListener);
        this.refreshQueue = new RefreshQueue<>(executor, lockedRefresher, builder.bulkReloadSizeLimit);
        this.segments = new FaultTolerantCache.Segment[numSegments];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment();
        }
    }

    /**
     * Removes given key from the cache.
     *
     * @return true if given key was cached.
     */
    public boolean invalidate(K key)
    {
        requireNonNull(key);
        return segmentOf(key).invalidate(key);
    }

    /**
     * Removes given keys from the cache.
     *
     * @return true if at least one of given keys was cached.
     */
    public boolean invalidateAll(Iterable<? extends K> keys)
    {
        boolean changed = false;
        for (K key : keys) {
            requireNonNull(key);
            if (segmentOf(key).invalidate(key)) {
                changed = true;
            }
        }
        return changed;
    }

    /**
     * Removes all keys from the cache.
     */
    public void invalidateAll()
    {
        for (Segment segment : segments) {
            segment.invalidateAll();
        }
    }

    /**
     * Get a key if given it is cached.
     *
     * If the key is not cached, this method returns null. This method never triggers cache loading.
     */
    public V getIfPresent(K key)
    {
        requireNonNull(key);
        return segmentOf(key).getIfPresent(key);
    }

    /**
     * Get a cached key, or load the key to cache and return it.
     *
     * Depending on the time when the cache was loaded, this method behaves differently:
     * <ul>
     * <li> If expireAfterWrite has passed, or the cache is not even loaded yet:
     *   Cache loader is called in current thread. If loader throws an exception, this method
     *   throws the exception.
     * <li> If refreshAfterWrite has passed:
     *   Cache loader is called in current thread. If loader throws an exception, this method
     *   returns the cached value without throwing the exception.
     * <li> If asynchronousRefreshAfterWrite has passed:
     *   This method returns the cached value. The key is enqueued for background executor to refresh.
     * <li> If asynchronousRefreshAfterWrite, refreshAfterWrite, and expireAfterWrite have not passed:
     *   This method simply returns the cached value.
     * </ul>
     */
    public V get(K key)
    {
        requireNonNull(key);
        return segmentOf(key).get(key);
    }

    /**
     * Refreshes cached values which are subject to refresh.
     *
     * This method scans all cached values. If it finds keys which are subject to refresh
     * but not expired yet, then it enqueues them to queue for refresh. Finally, this method
     * calls cache loader in current thread.
     * <br>
     * Even if cache loader throw exception, this method doesn't throw exception.
     */
    public void refresh()
    {
        refreshImpl(false);
    }

    /**
     * Refreshes all cached values.
     *
     * This method is sable with refresh excepting that this method enqueues keys even if
     * the key is not subject to refresh.
     */
    public void refreshNow()
    {
        refreshImpl(true);
    }

    private void refreshImpl(boolean all)
    {
        List<RefreshableValue<K, V>> toBeRefreshed = new ArrayList<>();
        for (Segment segment : segments) {
            segment.collectEntriesToRefresh(toBeRefreshed, all);
        }
        if (!toBeRefreshed.isEmpty()) {
            refreshQueue.addAllNoRun(toBeRefreshed);
            refreshQueue.run();
        }
    }

    private Segment segmentOf(K key)
    {
        return segments[Math.abs(key.hashCode() % segments.length)];
    }

    class Segment
    {
        private final HashMap<K, RefreshableValue<K, V>> map = new HashMap<>();
        private final AccessOrderLinkedList<RefreshableValue<K, V>> accessOrder = new AccessOrderLinkedList<>();

        public boolean invalidate(K key)
        {
            synchronized (map) {
                RefreshableValue<K, V> entry = map.remove(key);
                if (entry != null) {
                    accessOrder.remove(entry);
                    entry.setEvicted();
                    return true;
                }
                return false;
            }
        }

        public void invalidateAll()
        {
            synchronized (map) {
                for (RefreshableValue<K, V> entry : map.values()) {
                    entry.setEvicted();
                }
                map.clear();
                accessOrder.clear();
            }
        }

        public V getIfPresent(K key)
        {
            synchronized (map) {
                RefreshableValue<K, V> entry = map.get(key);
                if (entry == null) {
                    return null;
                }
                if (isHardExpired(entry, lockedRefresher.currentMilliTime())) {
                    return null;
                }
                accessOrder.moveToHead(entry);
                return entry.getCurrentVersion().getValue();
            }
        }

        public V get(K key)
        {
            RefreshableValue<K, V> toBeRefreshed;
            boolean allowCurrentVersion;
            boolean allowAsyncRefresh;
            long now = lockedRefresher.currentMilliTime();

            synchronized (map) {
                RefreshableValue<K, V> entry = map.get(key);
                if (entry == null) {
                    // No cache entry yet. Create a new RefreshableValue so that
                    // following get(key) can get the RefreshableValue. Refresh
                    // the value later with allowCurrentVersion=false.
                    ensureMaximumSizeBeforeAdd(1);
                    entry = accessOrder.addToHead(new RefreshableValue<>(key));
                    map.put(key, entry);
                    toBeRefreshed = entry;
                    allowCurrentVersion = false;
                    allowAsyncRefresh = false;
                }
                else if (isHardExpired(entry, now)) {
                    // Hard-expired. Refresh or throw.
                    accessOrder.moveToHead(entry);
                    toBeRefreshed = entry;
                    allowCurrentVersion = false;
                    allowAsyncRefresh = false;
                }
                else if (isSyncOrAsyncRefreshWanted(entry, now)) {
                    // Refresh wanted but not hard-expired. Try to refresh, but return the
                    // current value if asynchronous refresh is allowed or refresh fails.
                    accessOrder.moveToHead(entry);
                    toBeRefreshed = entry;
                    allowCurrentVersion = true;
                    allowAsyncRefresh = !isSyncRefreshWanted(entry, now);
                }
                else {
                    // No need to refresh at all.
                    accessOrder.moveToHead(entry);
                    return entry.getCurrentVersion().getValue();
                }
            }

            ValueVersion<V> version;
            if (!allowCurrentVersion) {
                // No value to return. Refresh or throw.
                version = lockedRefresher.refreshOrJoin(toBeRefreshed);
            }
            else if (allowAsyncRefresh) {
                // Has valid current value, and asynchronous refresh is allowed.
                // Enqueue refresh, and return the current version,
                version = toBeRefreshed.getCurrentVersion();
                refreshQueue.add(toBeRefreshed);
            }
            else {
                // Has valid current value, asynchronous refresh is not allowed.
                // Try synchronous refresh. Return the current value if exception thrown.
                try {
                    version = lockedRefresher.refreshOrJoin(toBeRefreshed);
                }
                catch (Exception ex) {
                    version = toBeRefreshed.getCurrentVersion();
                }
            }
            return version.getValue();
        }

        private void ensureMaximumSizeBeforeAdd(int numToAdd)
        {
            if (maximumSizePerSegment <= 0) {
                return;
            }
            while (map.size() > maximumSizePerSegment - numToAdd) {
                RefreshableValue<K, V> entry = accessOrder.removeTail();
                map.remove(entry.getKey());
                entry.setEvicted();
            }
        }

        private boolean isSyncOrAsyncRefreshWanted(RefreshableValue<K, V> entry, long milliTime)
        {
            ValueVersion<V> currentVersion = entry.getCurrentVersion();
            if (currentVersion == null) {
                return true;
            }
            return (refreshAfterWriteMillis != EXPIRE_REFRESH_DISABLED &&
                currentVersion.getWrittenAt() + refreshAfterWriteMillis < milliTime)
                || (asynchronousRefreshAfterWriteMillis != EXPIRE_REFRESH_DISABLED &&
                        currentVersion.getWrittenAt() + asynchronousRefreshAfterWriteMillis < milliTime);
        }

        private boolean isSyncRefreshWanted(RefreshableValue<K, V> entry, long milliTime)
        {
            ValueVersion<V> currentVersion = entry.getCurrentVersion();
            if (currentVersion == null) {
                return true;
            }
            return refreshAfterWriteMillis != EXPIRE_REFRESH_DISABLED &&
                currentVersion.getWrittenAt() + refreshAfterWriteMillis < milliTime;
        }

        private boolean isHardExpired(RefreshableValue<K, V> entry, long milliTime)
        {
            ValueVersion<V> currentVersion = entry.getCurrentVersion();
            if (currentVersion == null) {
                return true;
            }
            return expireAfterWriteMillis != EXPIRE_REFRESH_DISABLED &&
                currentVersion.getWrittenAt() + expireAfterWriteMillis < milliTime;
        }

        void collectEntriesToRefresh(List<RefreshableValue<K, V>> results, boolean all)
        {
            synchronized (map) {
                long now = lockedRefresher.currentMilliTime();
                accessOrder.forEach((entry) -> {
                    if (isHardExpired(entry, now)) {
                        accessOrder.remove(entry);
                        entry.setEvicted();
                    }
                    else if (all || isSyncOrAsyncRefreshWanted(entry, now)) {
                        results.add(entry);
                    }
                });
            }
        }
    }
}
