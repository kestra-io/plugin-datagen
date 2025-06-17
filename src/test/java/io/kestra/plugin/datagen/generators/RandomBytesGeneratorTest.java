package io.kestra.plugin.datagen.generators;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@KestraTest
class RandomBytesGeneratorTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldGenerateData() throws IllegalVariableEvaluationException {
        // Given
        RandomBytesGenerator generator = RandomBytesGenerator
            .builder()
            .size(1024)
            .build();
        generator.init(runContextFactory.of());

        // When
        byte[] next1 = generator.produce();
        byte[] next2 = generator.produce();

        assertThat(next1).isNotNull();
        assertThat(next2).isNotNull();
        assertThat(next1.length).isEqualTo(1024);
        assertThat(next2.length).isEqualTo(1024);

        assertThat(next1).isNotEqualTo(next2);
    }
}