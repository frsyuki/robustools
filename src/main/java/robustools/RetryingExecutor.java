package robustools;

import java.util.function.Predicate;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * RetryingExecutor executes a Callable or Runnable with retrying.
 *
 * Retrying interval is exponential back-off.
 */
public class RetryingExecutor
{
    public static class RetryGiveupException
            extends ExecutionException
    {
        public RetryGiveupException(String message, Exception cause)
        {
            super(cause);
        }

        public RetryGiveupException(Exception cause)
        {
            super(cause);
        }

        public Exception getCause()
        {
            return (Exception) super.getCause();
        }
    }

    public interface RetryAction
    {
        void onRetry(Exception exception, int retryCount, int retryLimit, int retryWait)
            throws RetryGiveupException;
    }

    public interface GiveupAction
    {
        void onGiveup(Exception firstException, Exception lastException)
            throws RetryGiveupException;
    }

    public static class Builder
    {
        int retryLimit = 5;
        int initialRetryWait = 500;
        int maxRetryWait = 5 * 60 * 1000;
        double waitGrowRate = 2.0;
        long giveupTimeout = 0;
        Predicate<Exception> retryPredicate = null;
        RetryAction retryAction = null;
        GiveupAction giveupAction = null;

        /**
         * Maximum number of retrying.
         */
        public Builder retryLimit(int count)
        {
            this.retryLimit = count;
            return this;
        }

        /**
         * The wait time in millisecond for the first retrying.
         */
        public Builder initialRetryWait(int msec)
        {
            this.initialRetryWait = msec;
            return this;
        }

        /**
         * Maximum wait time in millisecond for retrying.
         */
        public Builder maxRetryWait(int msec)
        {
            this.maxRetryWait = msec;
            return this;
        }

        /**
         * Rate to increase wait interval for each retrying.
         *
         * A retrying waits for previous waiting time multiplied by this rate,
         * with the cap of maxRetryWait.
         * Default is 2.0.
         */
        public Builder waitGrowRate(double rate)
        {
            this.waitGrowRate = rate;
            return this;
        }

        /**
         * The duration in millisecond to giveup retrying.
         *
         * Retrying stops when number of retrying exceeds retryLimit, or total
         * duration from the starting of initial run exceeds giveupTimeout.
         * <br>
         * Default is 0, which disables giveup by timeout.
         */
        public Builder giveupTimeout(long msec)
        {
            this.giveupTimeout = msec;
            return this;
        }

        /**
         * Function to decide whether a thrown exception is retryable not.
         *
         * RetryingExecutor retries only when this function returns true.
         * Default function always returns true.
         */
        public Builder retryIf(Predicate<Exception> predicate)
        {
            this.retryPredicate = predicate;
            return this;
        }

        /**
         * A hook function that is called when retrying happens.
         *
         * This hook is useful to show a log message. Example:
         * <code>
         * String.format("Retrying an execution (%d/%d): %s", retryCount, retryLimit, exception)
         * </code>
         */
        public Builder onRetry(RetryAction hook)
        {
            this.retryAction = hook;
            return this;
        }

        /**
         * A function that is called when retrying is given up.
         *
         * This function is useful to throw a custom exception.
         * If this function is not set, RetryGiveupException is thrown with the exception
         * thrown when the first execution fails.
         */
        public Builder onGiveup(GiveupAction override)
        {
            this.giveupAction = override;
            return this;
        }

        /**
         * Creates a new RetryingExecutor.
         */
        public RetryingExecutor build()
        {
            return new RetryingExecutor(this);
        }
    }

    /**
     * Creates a new builder to build RetryingExecutor.
     */
    public Builder newBuilder()
    {
        return new Builder();
    }

    private final int retryLimit;
    private final int initialRetryWait;
    private final int maxRetryWait;
    private final long giveupTimeout;
    private final double waitGrowRate;
    private final Predicate<Exception> retryPredicate;
    private final RetryAction retryAction;
    private final GiveupAction giveupAction;

    RetryingExecutor(Builder builder)
    {
        this.retryLimit = builder.retryLimit;
        this.initialRetryWait = builder.initialRetryWait;
        this.maxRetryWait = builder.maxRetryWait;
        this.giveupTimeout = builder.giveupTimeout;
        this.waitGrowRate = builder.waitGrowRate;
        this.retryPredicate = builder.retryPredicate;
        this.retryAction = builder.retryAction;
        this.giveupAction = builder.giveupAction;
    }

    /**
     * Run the given Callable and returns returned value of it.
     */
    public <T> T runInterruptible(Callable<T> op)
            throws InterruptedException, RetryGiveupException
    {
        return run(op, true);
    }

    /**
     * Run the given Runnable.
     */
    public void runInterruptible(Runnable op)
            throws InterruptedException, RetryGiveupException
    {
        runInterruptible(() -> {
            op.run();
            return (Void) null;
        });
    }

    /**
     * Run the given Callable and returns returned value of it.
     */
    public <T> T run(Callable<T> op)
            throws RetryGiveupException
    {
        try {
            return run(op, false);
        } catch (InterruptedException ex) {
            throw new RetryGiveupException("Unexpected interruption", ex);
        }
    }

    /**
     * Run the given Runnable.
     */
    public void run(Runnable op)
            throws RetryGiveupException
    {
        run(() -> {
            op.run();
            return (Void) null;
        });
    }

    private <T> T run(Callable<T> op, boolean interruptible)
            throws InterruptedException, RetryGiveupException
    {
        int retryCount = 0;
        long giveupAtNanoTime = (giveupTimeout > 0 ? System.nanoTime() + giveupTimeout * 1000000L : 0);

        Exception firstException = null;

        while(true) {
            try {
                return op.call();
            } catch (Exception exception) {
                if (firstException == null) {
                    firstException = exception;
                }
                if (retryCount >= retryLimit || (retryPredicate != null && !retryPredicate.test(exception))) {
                    if (giveupAction != null) {
                        giveupAction.onGiveup(firstException, exception);
                    }
                    throw new RetryGiveupException(firstException);
                }

                // exponential back-off with hard limit
                int retryWait = (int) Math.min((double) maxRetryWait, initialRetryWait * Math.pow(waitGrowRate, retryCount));

                if (giveupTimeout > 0) {
                    int canWait = (int) ((giveupAtNanoTime - System.nanoTime()) / 1000000L);
                    if (canWait <= retryWait) {
                        throw new RetryGiveupException(firstException);
                    }
                }

                retryCount++;
                if (retryAction != null) {
                    retryAction.onRetry(exception, retryCount, retryLimit, retryWait);
                }

                try {
                    Thread.sleep(retryWait);
                } catch (InterruptedException ex) {
                    if (interruptible) {
                        throw ex;
                    }
                }
            }
        }
    }
}
