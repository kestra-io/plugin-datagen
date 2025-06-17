package io.kestra.plugin.datagen.internal;

import io.kestra.plugin.datagen.model.Callback;
import org.slf4j.Logger;

import java.util.Arrays;

public class Stats {

    private final long start;
    private long windowStart;
    private final int[] latencies;
    private final long sampling;
    private final Logger logger;
    private final long reportingInterval;

    private long iteration;
    private int index;

    // Global Stats
    private long count;
    private long bytes;
    private int maxLatency;
    private long totalLatency;
    // Windows Stats
    private long windowCount;
    private int windowMaxLatency;
    private long windowTotalLatency;
    private long windowBytes;



    public Stats(Logger logger, long numRecords, long reportingInterval) {
        this.logger = logger;
        this.start = System.currentTimeMillis();
        this.windowStart = System.currentTimeMillis();
        this.iteration = 0;
        this.sampling = numRecords / Math.min(numRecords, 500000);
        this.latencies = new int[(int) (numRecords / this.sampling) + 1];
        this.index = 0;
        this.maxLatency = 0;
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
        this.totalLatency = 0;
        this.reportingInterval = reportingInterval;
    }

    public void record(Long iter, int latency, long bytes, long time) {
        this.count++;
        this.bytes += bytes;
        this.totalLatency += latency;
        this.maxLatency = Math.max(this.maxLatency, latency);
        this.windowCount++;
        this.windowBytes += bytes;
        this.windowTotalLatency += latency;
        this.windowMaxLatency = Math.max(windowMaxLatency, latency);

        if (iter % this.sampling == 0) {
            this.latencies[index] = latency;
            this.index++;
        }
        if (time - windowStart >= reportingInterval) {
            printWindow();
            newWindow();
        }
    }

    public Callback nextCompletion(long start, long bytes, Stats stats) {
        Callback cb = new StatsCallback(start, this.iteration, bytes, stats);
        this.iteration++;
        return cb;
    }

    public void printWindow() {
        long elapsedMs = System.currentTimeMillis() - windowStart;
        if (elapsedMs <= 0 || windowCount == 0) {
            logger.info("[{}] No data to report yet.", Thread.currentThread().getName());
            return;
        }

        double elapsedSec = elapsedMs / 1000.0;
        double recsPerSec = windowCount / elapsedSec;
        double mbPerSec = (windowBytes / (1024.0 * 1024.0)) / elapsedSec;
        double avgLatency = windowTotalLatency / (double) windowCount;

        logger.info(
            "[{}] {} records generated, {} records/sec ({} MB/sec), {} ms avg latency, {} ms max latency.",
            Thread.currentThread().getName(),
            windowCount,
            String.format("%.1f", recsPerSec),
            String.format("%.2f", mbPerSec),
            String.format("%.1f", avgLatency),
            String.format("%.1f", (double) windowMaxLatency)
        );
    }

    public void newWindow() {
        this.windowStart = System.currentTimeMillis();
        this.windowCount = 0;
        this.windowMaxLatency = 0;
        this.windowTotalLatency = 0;
        this.windowBytes = 0;
    }

    public void printTotal() {
        long elapsed = System.currentTimeMillis() - start;
        double recsPerSec = 1000.0 * count / (double) elapsed;
        double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0 * 1024.0);
        int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
        logger.info("{} records generated, {} records/sec ({} MB/sec), {} ms avg latency, {}"
                + " ms max latency, {} ms 50th, {} ms 95th, {} ms 99th, {} ms"
                + " 99.9th.",
                count,
                recsPerSec,
                String.format("%.2f",mbPerSec),
                String.format("%.2f", totalLatency / (double) count),
                String.format("%.2f", (double) maxLatency),
                percs[0],
                percs[1],
                percs[2],
                percs[3]
        );
    }

    private static int[] percentiles(int[] latencies, int count, double... percentiles) {
        int size = Math.min(count, latencies.length);
        Arrays.sort(latencies, 0, size);
        int[] values = new int[percentiles.length];
        for (int i = 0; i < percentiles.length; i++) {
            int index = (int) (percentiles[i] * size);
            values[i] = latencies[index];
        }
        return values;
    }

    public record StatsCallback(
        long start,
        long iteration,
        long bytes,
        Stats stats
    ) implements Callback {
        @Override
        public void run() {
            long now = System.currentTimeMillis();
            int latency = (int)(now - start);
            this.stats.record(iteration, latency, bytes, now);
        }
    }
}
