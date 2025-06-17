package io.kestra.plugin.datagen.internal;

import java.time.Duration;

/**
 * A utility class to throttle throughput (e.g., bytes/sec or messages/sec) by inserting delays
 * between operations to ensure that the rate of output does not exceed a specified limit.
 *
 * <p>Supports both data and message rate limiting. If the throughput is set to 0, it blocks
 * indefinitely until {@link #wakeup()} is called. If the throughput is negative, throttling is disabled.
 */
public final class ThroughputThrottler {

    private static final long MIN_SLEEP_NS = Duration.ofMillis(2).toNanos();
    private static final Duration MAX_DURATION = Duration.ofMillis(Long.MAX_VALUE);

    private final long startTimeMs;
    private final Duration sleepInterval;
    private final long maxThroughput;

    private long sleepDeficitNs = 0;
    private boolean shouldWakeup = false;

    /**
     * Constructs a {@code ThroughputThrottler} with a specified throughput limit and start time.
     *
     * @param maxThroughput The desired maximum throughput, in units per second (e.g., bytes/sec or messages/sec).
     *                      A value of 0 causes the {@link #throttle()} method to block indefinitely until {@link #wakeup()} is called.
     *                      A negative value disables throttling altogether.
     * @param startTimeMs   The start timestamp (in milliseconds) for when throttling calculations begin.
     */
    public ThroughputThrottler(long maxThroughput, long startTimeMs) {
        this.startTimeMs = startTimeMs;
        this.maxThroughput = maxThroughput;
        this.sleepInterval = maxThroughput > 0
            ? Duration.ofNanos(Duration.ofSeconds(1).toNanos() / maxThroughput)
            : MAX_DURATION;
    }

    /**
     * Determines whether throttling should be applied based on the amount of data or messages
     * processed so far and the elapsed time.
     *
     * @param totalSent     The total number of units (e.g., bytes or messages) sent so far.
     * @param currentTimeMs The current time in milliseconds.
     * @return {@code true} if the current throughput exceeds the configured maximum; {@code false} otherwise.
     */
    public boolean shouldThrottle(long totalSent, long currentTimeMs) {
        if (maxThroughput < 0) return false;

        float elapsedSec = (currentTimeMs - startTimeMs) / 1000f;
        return elapsedSec > 0 && (totalSent / elapsedSec) > maxThroughput;
    }

    /**
     * Applies throttling by introducing a delay, if needed, to match the configured throughput rate.
     *
     * <p>If enough time has not passed since the last operation, the method sleeps to reduce the rate of execution.
     * If {@code maxThroughput} is set to 0, this method blocks indefinitely until {@link #wakeup()} is called.
     */
    public void throttle() {
        if (maxThroughput == 0) {
            try {
                synchronized (this) {
                    while (!shouldWakeup) wait();
                }
            } catch (InterruptedException ignored) {
            }
            return;
        }

        // throttle throughput by sleeping, on average,
        // (1 / this.maxThroughput) seconds between "things sent"
        sleepDeficitNs += sleepInterval.toNanos();

        // If enough sleep deficit has accumulated, sleep a little
        if (sleepDeficitNs >= MIN_SLEEP_NS) {
            long sleepStartNs = System.nanoTime();
            try {
                synchronized (this) {
                    long remaining = sleepDeficitNs;
                    while (!shouldWakeup && remaining > 0) {
                        Duration sleepDuration = Duration.ofNanos(remaining);
                        wait(sleepDuration.toMillis(), sleepDuration.getNano() % 1_000_000);
                        long elapsed = System.nanoTime() - sleepStartNs;
                        remaining = sleepDeficitNs - elapsed;
                    }
                    shouldWakeup = false;
                }
                sleepDeficitNs = 0;
            } catch (InterruptedException e) {
                // If sleep is interrupted, reduce deficit by the amount of time actually spent sleeping
                long sleepElapsedNs = System.nanoTime() - sleepStartNs;
                sleepDeficitNs = Math.max(0, sleepDeficitNs - sleepElapsedNs);
            }
        }
    }

    /**
     * Wakes up the throttler if it is currently sleeping.
     * This is primarily used when {@code maxThroughput == 0} to unblock the thread.
     */
    public void wakeup() {
        synchronized (this) {
            shouldWakeup = true;
            notifyAll();
        }
    }
}
