package robustools;

import robustools.FaultTolerantCache.FailureRateLimitException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

// This class manages concurrent refresh of a key by multiple threads.
// When one thread refreshes a key, it sets a future to `refreshLock` atomically.
// When other threads try to refresh the same key, it gets the future first. If a
// future is set, wait for completion of the future instead of refreshing again.
//
// The same semantics is applied both to single reloading and bulk reloading.
//
// `evicted` flag is just a hint to suppress unnecessary refresh as much as possible.
class LockedRefresher<K, V>
{
    public static class ValueVersion<V>
    {
        final V value;

        final long writtenAt;

        public ValueVersion(V value, long writtenAt)
        {
            this.value = value;
            this.writtenAt = writtenAt;
        }

        public V getValue()
        {
            return value;
        }

        public long getWrittenAt()
        {
            return writtenAt;
        }
    }

    public static class RefreshableValue<K, V>
            extends AccessOrderLinkedList.Node<RefreshableValue<K, V>>
    {
        private final K key;

        private volatile ValueVersion<V> currentVersion;

        final AtomicReference<CompletableFuture<ValueVersion<V>>> refreshLock = new AtomicReference<>();

        volatile boolean evicted = false;

        public RefreshableValue(K key)
        {
            this.key = key;
            this.currentVersion = null;
        }

        public K getKey()
        {
            return key;
        }

        public ValueVersion<V> getCurrentVersion()
        {
            return currentVersion;
        }

        void setCurrentVersion(ValueVersion<V> value)
        {
            this.currentVersion = value;
        }

        public void setEvicted()
        {
            evicted = true;
        }
    }

    private final Function<K, V> loader;
    private final BiConsumer<Collection<K>, BiConsumer<K, V>> reloader;  // null if not available

    private final LeakyBucket failureRateLimit;
    private final Consumer<Throwable> exceptionListener;

    public LockedRefresher(
            Function<K, V> loader,
            BiConsumer<Collection<K>, BiConsumer<K, V>> reloader,
            LeakyBucket failureRateLimit,
            Consumer<Throwable> exceptionListener)
    {
        this.loader = loader;
        this.reloader = reloader;
        this.failureRateLimit = failureRateLimit;
        this.exceptionListener = exceptionListener;
    }

    public boolean isBulkReloaderAvaialble()
    {
        return reloader != null;
    }

    public void refreshOrLeave(RefreshableValue<K, V> element)
    {
        if (element.evicted) {
            return;
        }
        refreshImpl(element, false);
    }

    public ValueVersion<V> refreshOrJoin(RefreshableValue<K, V> element)
    {
        return refreshImpl(element, true);
    }

    private ValueVersion<V> refreshImpl(RefreshableValue<K, V> element, boolean join)
    {
        // Check refreshLock.
        CompletableFuture<ValueVersion<V>> before = element.refreshLock.get();
        if (before == null) {
            CompletableFuture<ValueVersion<V>> future = newFuture(element);
            before = element.refreshLock.getAndUpdate((current) -> (current == null ? future : current));
            if (before == null) {
                // Succeeded to set refreshLock atomically. Run loader.
                if (failureRateLimit != null && failureRateLimit.getAvailableCapacity() < 1) {
                    FailureRateLimitException ex = new FailureRateLimitException();
                    if (exceptionListener != null) {
                        exceptionListener.accept(ex);
                    }
                    throw ex;
                }
                V value;
                try {
                    value = loader.apply(element.getKey());
                }
                catch (Throwable ex) {
                    future.completeExceptionally(ex);
                    try {
                        if (failureRateLimit != null) {
                            failureRateLimit.flowIn(1);
                        }
                        if (exceptionListener != null) {
                            exceptionListener.accept(ex);
                        }
                    }
                    finally {
                        throw ex;
                    }
                }
                ValueVersion<V> v = new ValueVersion<>(value, currentMilliTime());
                future.complete(v);
                return v;
            }
        }
        // refreshLock is already set. Another thread is refreshing the value.
        if (join) {
            // Wait for completion
            try {
                return before.get();
            }
            catch (ExecutionException ex) {
                if (ex.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) ex.getCause();
                }
                else if (ex.getCause() instanceof Error) {
                    throw (Error) ex.getCause();
                }
                throw new RuntimeException(ex);
            }
            catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
        else {
            // Leave it running
            return null;
        }
    }

    public void refreshOrLeaveBulk(Collection<? extends RefreshableValue<K, V>> elements)
    {
        if (failureRateLimit != null && failureRateLimit.getAvailableCapacity() < 1) {
            if (exceptionListener != null) {
                exceptionListener.accept(new FailureRateLimitException());
            }
            return;
        }

        Map<K, RefreshableValue<K, V>> locked = new LinkedHashMap<>();
        Throwable exception = null;
        try {
            for (RefreshableValue<K, V> element : elements) {
                if (locked.containsKey(element.getKey())) {
                    continue;
                }
                // Check refreshLock.
                CompletableFuture<ValueVersion<V>> before = element.refreshLock.get();
                if (before == null) {
                    CompletableFuture<ValueVersion<V>> future = newFuture(element);
                    if (element.refreshLock.compareAndSet(null, future)) {
                        // Succeeded to set refreshLock atomically. Add it to locked.
                        locked.put(element.getKey(), element);
                    }
                }
            }

            // Run reloader for the all elements in locked.
            List<K> keys = new ArrayList<>(locked.keySet());
            reloader.accept(keys, (key, value) -> {
                RefreshableValue<K, V> element = locked.get(key);
                if (element != null) {
                    element.refreshLock.get().complete(new ValueVersion<>(value, currentMilliTime()));
                    element.refreshLock.set(null);
                    locked.remove(key);
                }
            });
        }
        catch (Throwable ex) {
            exception = ex;
            if (failureRateLimit != null) {
                failureRateLimit.flowIn(1);
            }
            if (exceptionListener != null) {
                exceptionListener.accept(ex);
            }
            // do not throw
        }
        finally {
            // Some locked elements are not processed (an exception occurred).
            // Release the lock and complete the future with exception.
            for (RefreshableValue<K, V> unprocessed : locked.values()) {
                unprocessed.refreshLock.get().completeExceptionally(exception);
                unprocessed.refreshLock.set(null);
            }
        }
    }

    static long currentMilliTime()
    {
        return System.nanoTime() / 1000000;
    }

    private static <K, V> CompletableFuture<ValueVersion<V>> newFuture(RefreshableValue<K, V> element)
    {
        CompletableFuture<ValueVersion<V>> future = new CompletableFuture<ValueVersion<V>>();
        future.whenComplete((v, ex) -> {
            if (ex == null) {
                element.setCurrentVersion(v);
            }
            element.refreshLock.set(null);
        });
        return future;
    }
}
