package robustools;

/**
 * An implementation of the leaky bucket algorithm.
 *
 * This is useful to implement rate limiting. Build a new LeakyBucket with desired
 * capacity (maximum allowed number of requests upon burst) and leakRate (allowed
 * regular rate to call the requests). When a request comes, call tryFlowIn(1) and
 * process the request if it returns true. If tryFlowIn returned false, reject the
 * request. Available capacity recovers up to the initial capacity at the rate of
 * leakRate every second.
 * <br>
 * LeakyBucket is thread-safe.
 */
public class LeakyBucket
{
    public static class Builder
    {
        double capacity = 0.0;
        double leakRate = 0.0;
        double allowedNegativeCapacity = 0.0;
        double initialVolume = 0.0;

        /**
         * Size of the bucket.
         *
         * Flowing in can burst at most this amount.
         */
        public Builder capacity(double capacity)
        {
            this.capacity = capacity;
            return this;
        }

        /**
         * Amount of volume to be removed from the bucket every second.
         */
        public Builder leakRate(double leakRate)
        {
            this.leakRate = leakRate;
            return this;
        }

        /**
         * Allow capacity to be negative.
         *
         * By default, when amount flowed in is more than the configured capacity,
         * available capacity becomes 0. If negative capacity is allowed, available
         * capacity becomes less than 0. This is useful when rate limiting wants to
         * give "penalty" upon too much use. Too much use is possible by concurrency
         * when multiple threads are flowing in to the same LeakyBucket, or when you
         * skip or loosen checking of getAvailableCapacity().
         *
         * This is identical to allowNegativeCapacity(NEGATIVE_INFINITY).
         */
        public Builder allowNegativeCapacity()
        {
            return allowNegativeCapacity(Double.NEGATIVE_INFINITY);
        }

        /**
         * Allow capacity to be negative.
         *
         * allowedNegativeCapacity must be less than 0 to be effective.
         */
        public Builder allowNegativeCapacity(double allowedNegativeCapacity)
        {
            this.allowedNegativeCapacity = allowedNegativeCapacity;
            return this;
        }

        /**
         * Volume initially flowed in the bucket.
         * This option is less common. Default is 0.
         */
        public Builder initialVolume(double initialVolume)
        {
            this.initialVolume = initialVolume;
            return this;
        }

        /**
         * Creates a new LeakyBucket.
         */
        public LeakyBucket build()
        {
            return new LeakyBucket(this);
        }
    }

    /**
     * Creates a new builder to build LeakyBucket.
     */
    public static Builder newBuilder()
    {
        return new Builder();
    }

    // availableCapacity is the actual available capacity
    private volatile double availableCapacity;
    private volatile long lastFlowInNanoTime;

    // Configuration
    private volatile double capacity;
    private volatile double leakRate;
    private volatile double allowedNegativeCapacity;

    LeakyBucket(Builder builder)
    {
        this.availableCapacity = builder.capacity - builder.initialVolume;
        this.lastFlowInNanoTime = System.nanoTime();

        this.capacity = builder.capacity;
        this.leakRate = builder.leakRate;
        this.allowedNegativeCapacity = builder.allowedNegativeCapacity;
    }

    /**
     * Return current amount of remaining capacity of the bucket.
     *
     * You can fill this much amount. The value returned by this method increases
     * over time at the rate of leakRate every second up to configured capacity.
     */
    public synchronized double getAvailableCapacity()
    {
        adjustWithDelta(0.0);
        return availableCapacity;
    }

    /**
     * Fill the bucket by the given amount if available capacity is more than the amount.
     *
     * This is identical to checking getAvailableCapacity is same or more than amount and
     * calling flowIn(amount) atomically.
     *
     * Returns true if flowed in.
     */
    public synchronized boolean tryFlowIn(double amount)
    {
        if (getAvailableCapacity() >= amount) {
            this.availableCapacity =
                Math.max(allowedNegativeCapacity, Math.min(capacity, availableCapacity - amount));
            return true;
        }
        return false;
    }

    /**
     * Fill the bucket by the given amount.
     *
     * This method decreases available capacity at least allowedNegativeCapacity.
     * allowedNegativeCapacity is 0 unless you call Builder.allowNegativeCapacity().
     */
    public synchronized void flowIn(double amount)
    {
        adjustWithDelta(-amount);
    }

    private void adjustWithDelta(double delta)
    {
        long now = System.nanoTime();
        double leakedSinceLastFlowIn = (now - lastFlowInNanoTime) * leakRate / 1_000_000_000.0;
        double newAvailableCapacity = availableCapacity + delta + leakedSinceLastFlowIn;
        this.availableCapacity =
            Math.max(allowedNegativeCapacity, Math.min(capacity, newAvailableCapacity));
        this.lastFlowInNanoTime = now;
    }

    /**
     * Change leaking rate.
     */
    public synchronized void setLeakRate(double leakRate)
    {
        this.leakRate = leakRate;
    }

    /**
     * Reset availableCapacity to the initial capacity.
     */
    public synchronized void clear()
    {
        this.availableCapacity = capacity;
        this.lastFlowInNanoTime = System.nanoTime();
    }
}
