package io.kestra.plugin.datagen.core;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.TestsUtils;
import io.kestra.plugin.datagen.generators.StringValueGenerator;
import io.kestra.plugin.datagen.model.DataGenerator;
import jakarta.inject.Inject;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class TriggerTest {

    @Inject
    RunContextFactory runContextFactory;

    @Test
    void shouldGenerateExecutionWithInlineValue() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(UUID.randomUUID().toString())
            .type(Trigger.class.getName())
            .store(Property.ofValue(false))
            .batchSize(Property.ofValue(1))
            .generator(StringValueGenerator.builder().value("hello").build())
            .build();

        Optional<Execution> evaluate = evaluate(trigger);

        assertThat(evaluate).isPresent();
        Map<String, Object> variables = evaluate.get().getTrigger().getVariables();
        assertThat(variables.get("value")).isEqualTo("hello");
        assertThat(variables.get("count")).isEqualTo(1);
        assertThat(variables.get("uri")).isNull();
    }

    @Test
    void shouldGenerateExecutionWithStoredFile() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(UUID.randomUUID().toString())
            .type(Trigger.class.getName())
            .store(Property.ofValue(true))
            .batchSize(Property.ofValue(3))
            .generator(StringValueGenerator.builder().value("hello").build())
            .build();

        Optional<Execution> evaluate = evaluate(trigger);

        assertThat(evaluate).isPresent();
        Map<String, Object> variables = evaluate.get().getTrigger().getVariables();
        assertThat(variables.get("value")).isNull();
        assertThat(variables.get("count")).isEqualTo(3);
        assertThat(variables.get("uri")).isNotNull();
    }

    @Test
    void shouldUnblockEvaluateWhenKilled() throws Exception {
        Trigger trigger = Trigger.builder()
            .id(UUID.randomUUID().toString())
            .type(Trigger.class.getName())
            .store(Property.ofValue(false))
            .batchSize(Property.ofValue(1))
            .generator(new SleepingGenerator(Duration.ofSeconds(30)))
            .build();

        CompletableFuture<Optional<Execution>> future = CompletableFuture.supplyAsync(() -> {
            try {
                return evaluate(trigger);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // Wait until the trigger has actually submitted the (currently sleeping) generation.
        Field generationField = Trigger.class.getDeclaredField("generation");
        generationField.setAccessible(true);
        Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> generationField.get(trigger) != null);

        trigger.kill();

        Awaitility.await().atMost(Duration.ofSeconds(1)).until(future::isDone);
        assertThat(future.get()).isEmpty();
    }

    @Test
    void shouldReturnEmptyWhenKilledBeforeEvaluate() throws Exception {
        AtomicInteger produceCount = new AtomicInteger(0);
        Trigger trigger = Trigger.builder()
            .id(UUID.randomUUID().toString())
            .type(Trigger.class.getName())
            .store(Property.ofValue(false))
            .batchSize(Property.ofValue(1))
            .generator(new CountingGenerator(produceCount))
            .build();

        trigger.kill();
        Optional<Execution> evaluate = evaluate(trigger);

        assertThat(evaluate).isEmpty();
        assertThat(produceCount.get()).isZero();
    }

    private Optional<Execution> evaluate(Trigger trigger) throws Exception {
        var entry = TestsUtils.mockTrigger(runContextFactory, trigger);
        return trigger.evaluate(entry.getKey(), entry.getValue());
    }

    // Must be public with a public no-arg constructor: DataGenerator is a @Plugin type, so Kestra's
    // annotation processor registers it as a ServiceLoader provider; an inaccessible provider throws
    // ServiceConfigurationError, which aborts the whole classpath plugin scan (including StorageInterface).
    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public static class SleepingGenerator extends DataGenerator<String> {
        private Duration sleep;

        SleepingGenerator(Duration sleep) {
            this.sleep = sleep;
        }

        @Override
        public String produce() {
            try {
                Thread.sleep(sleep.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return "value";
        }
    }

    @SuperBuilder
    @NoArgsConstructor
    @Getter
    public static class CountingGenerator extends DataGenerator<String> {
        private AtomicInteger counter;

        CountingGenerator(AtomicInteger counter) {
            this.counter = counter;
        }

        @Override
        public String produce() {
            counter.incrementAndGet();
            return "value";
        }
    }
}
