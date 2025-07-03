package io.kestra.plugin.datagen.services;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.plugin.datagen.Data;
import io.kestra.plugin.datagen.generators.StringValueGenerator;
import io.kestra.plugin.datagen.model.Producer;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Slf4j
@KestraTest
class DataEmitterTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldGenerateData() throws IllegalVariableEvaluationException {
        // Given
        List<Data> generated = new ArrayList<>(10);
        StringValueGenerator generator = StringValueGenerator
            .builder()
            .value("value")
            .build();
        generator.init(runContextFactory.of());

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
        DataEmitter task = new DataEmitter(producer, consumer, options, log);

        // When
        task.run();

        // Then
        assertThat(generated.size()).isEqualTo(10);
    }
}