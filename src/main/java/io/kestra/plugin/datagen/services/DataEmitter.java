package io.kestra.plugin.datagen.services;

import io.kestra.plugin.datagen.Data;
import io.kestra.plugin.datagen.internal.Stats;
import io.kestra.plugin.datagen.internal.ThroughputThrottler;
import io.kestra.plugin.datagen.model.Callback;
import io.kestra.plugin.datagen.model.Producer;
import org.slf4j.Logger;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Serv
 */
public class DataEmitter implements Runnable {

    private final Consumer<Data> consumer;
    private final Producer<Data> producer;

    private final DataEmitterOptions options;

    private final CountDownLatch isShutdownLatch;

    private final AtomicBoolean shutdown;

    private final Logger logger;

    private ThroughputThrottler throttler;

    /**
     * Creates a new {@link DataEmitter} instance.
     */
    public DataEmitter(
        final Producer<Data> producer,
        final Consumer<Data> consumer,
        final DataEmitterOptions options,
        final Logger logger) {
        this.logger = logger;
        this.consumer = consumer;
        this.producer = producer;
        this.options = options;
        this.isShutdownLatch = new CountDownLatch(1);
        this.shutdown = new AtomicBoolean(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        logger.info("Starting {}", this.getClass().getSimpleName());
        try {
            long startMs = System.currentTimeMillis();
            throttler = new ThroughputThrottler(options.throughput(), startMs);

            long i = 0;
            Stats stats = new Stats(logger, options.numExecutions(), options.reportingInterval().toMillis());
            while (!shutdown.get()) {
                long sendStartMs = System.currentTimeMillis();

                Data data = producer.produce();
                Callback cb = stats.nextCompletion(sendStartMs, data.getSize(), stats);
                doSendData(data, cb);
                if (throttler.shouldThrottle(options.numExecutions(), sendStartMs)) {
                    throttler.throttle();
                }

                i++;

                if (i >= options.numExecutions()) {
                    logger.info("Reach maximum number of data generation {}", options.numExecutions());
                    shutdown.set(true);
                }
            }
            stats.printTotal();
        } catch (Exception e) {
            logger.error("Error while emitting generated data. Stopping.", e);
            shutdown.set(true);
            throw e; // re-throw exception so that task can be restarted
        } finally {
            isShutdownLatch.countDown();
            logger.info("{} has been stopped", this.getClass().getSimpleName());
        }
    }

    public void doSendData(Data data, Callback callback) {
        try {
            consumer.accept(data);
            callback.run();
        } catch (Exception e) {
            String error =
                e.getCause() != null ? e.getCause().getLocalizedMessage() : e.getMessage();
            logger.warn("Unexpected error while emitting generated data. Error: {}", error);
        }
    }

    public void stop() {
        if (shutdown.compareAndSet(false, true)) {
            try {
                if (throttler != null) {
                    throttler.wakeup();
                }
                waitForTermination();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void waitForTermination() throws InterruptedException {
        boolean await = isShutdownLatch.await(5000, TimeUnit.MILLISECONDS);
        if (!await) {
            logger.warn("Timeout reached before data-emitter task being stopped");
        }
    }
}
