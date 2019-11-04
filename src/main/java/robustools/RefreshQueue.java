package robustools;

import robustools.LockedRefresher.RefreshableValue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import static java.util.stream.Collectors.toList;

class RefreshQueue<K, V>
{
    private final ConcurrentHashMap<RefreshableValue<K, V>, Boolean> deduper = new ConcurrentHashMap<>();
    private final Queue<RefreshableValue<K, V>> queue = new ArrayDeque<>();

    private final Executor executor;
    private final LockedRefresher<K, V> lockedRefresher;
    private final int bulkReloadSizeLimit;

    public RefreshQueue(Executor executor, LockedRefresher<K, V> lockedRefresher,
            int bulkReloadSizeLimit)
    {
        this.executor = executor;
        this.lockedRefresher = lockedRefresher;
        this.bulkReloadSizeLimit = bulkReloadSizeLimit;
    }

    public void add(RefreshableValue<K, V> toBeRefreshed)
    {
        if (deduper.putIfAbsent(toBeRefreshed, true) == null) {
            synchronized (queue) {
                queue.add(toBeRefreshed);
            }
            // run a thread
            executor.execute(this::run);
        }
    }

    public void addAllNoRun(List<RefreshableValue<K, V>> toBeRefreshed)
    {
        List<RefreshableValue<K, V>> list = toBeRefreshed.stream()
            .filter(e -> deduper.putIfAbsent(e, true) == null)
            .collect(toList());
        if (!list.isEmpty()) {
            synchronized (queue) {
                queue.addAll(list);
            }
            // no run thread
        }
    }

    public void run()
    {
        if (lockedRefresher.isBulkReloaderAvaialble()) {
            runBulk();
        }
        else {
            runSingle();
        }
    }

    private void runSingle()
    {
        while (true) {
            RefreshableValue<K, V> element = removeFirst();
            if (element == null) {
                return;
            }
            try {
                lockedRefresher.refreshOrLeave(element);
            }
            finally {
                deduper.remove(element);
            }
        }
    }

    private void runBulk()
    {
        while (true) {
            List<RefreshableValue<K, V>> list = removeFirstBulk();
            if (list.isEmpty()) {
                return;
            }
            try {
                lockedRefresher.refreshOrLeaveBulk(list);
            }
            finally {
                for (RefreshableValue<K, V> e : list) {
                    deduper.remove(e);
                }
            }
        }
    }

    private RefreshableValue<K, V> removeFirst()
    {
        synchronized (queue) {
            return queue.poll();
        }
    }

    private List<RefreshableValue<K, V>> removeFirstBulk()
    {
        synchronized (queue) {
            List<RefreshableValue<K, V>> list = new ArrayList<>();
            for (int i = 0; i < bulkReloadSizeLimit; i++) {
                RefreshableValue<K, V> e = queue.poll();
                if (e == null) {
                    break;
                }
                list.add(e);
            }
            return list;
        }
    }
}
