package io.kestra.plugin.datagen.services;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.datagen.Data;
import io.kestra.plugin.datagen.generators.StringValueGenerator;
import io.kestra.plugin.datagen.model.Producer;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@KestraTest
class DataEmitterTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldGenerateData() throws IllegalVariableEvaluationException {
        // Given
        List<Data> generated = new ArrayList<>(10);
        RunContext runContext = runContextFactory.of();
        StringValueGenerator generator = StringValueGenerator
            .builder()
            .value("value")
            .build();
        generator.init(runContext);
        Logger logger = runContext.logger();

        Consumer<Data> consumer = generated::add;
        Producer<Data> producer = () -> {
            String val = generator.produce();
            return Data
                .builder()
                .value(val)
                .size((long)val.length())
                .build();
        };

        DataEmitterOptions options = new DataEmitterOptions(10L, DataEmitterOptions.NO_THROUGHPUT, Duration.ZERO);
        DataEmitter task = new DataEmitter(producer, consumer, options, logger);

        // When
        task.run();

        // Then
        assertThat(generated.size()).isEqualTo(10);
    }

    @Test
    void shouldEnforceMaxThroughput() throws IllegalVariableEvaluationException {
        // Given a finite run with a bounded throughput.
        // The throttler is fed the running sent count, so it actually limits the rate:
        // emitting N records at T/s must take at least ~ (N - 1) / T seconds.
        final long throughput = 20L;
        final long numExecutions = 20L;

        List<Data> generated = new ArrayList<>((int) numExecutions);
        RunContext runContext = runContextFactory.of();
        StringValueGenerator generator = StringValueGenerator
            .builder()
            .value("value")
            .build();
        generator.init(runContext);
        Logger logger = runContext.logger();

        Consumer<Data> consumer = generated::add;
        Producer<Data> producer = () -> {
            String val = generator.produce();
            return Data
                .builder()
                .value(val)
                .size((long) val.length())
                .build();
        };

        DataEmitterOptions options = new DataEmitterOptions(numExecutions, throughput, Duration.ZERO);
        DataEmitter task = new DataEmitter(producer, consumer, options, logger);

        // When
        long startMs = System.currentTimeMillis();
        task.run();
        long elapsedMs = System.currentTimeMillis() - startMs;

        // Then
        // All records are emitted...
        assertThat(generated.size()).isEqualTo((int) numExecutions);
        // ...and throttling kept the effective rate at or below the configured throughput.
        // Without enforcement the run would complete almost instantly; here it must be throttled.
        long minExpectedMs = (numExecutions - 1) * 1000L / throughput;
        assertThat(elapsedMs).isGreaterThanOrEqualTo((long) (minExpectedMs * 0.7));
    }
}