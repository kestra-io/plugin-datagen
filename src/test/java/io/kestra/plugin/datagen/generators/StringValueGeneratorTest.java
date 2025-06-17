package io.kestra.plugin.datagen.generators;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@KestraTest
class StringValueGeneratorTest {
    @Inject
    private RunContextFactory runContextFactory;


    @Test
    void shouldGenerateDataGivenFakerExpression() throws IllegalVariableEvaluationException {
        // Given
        StringValueGenerator generator = StringValueGenerator
            .builder()
            .value("#{name.fullName}")
            .build();
        generator.init(runContextFactory.of());

        // When
        String next1 = generator.produce();
        String next2 = generator.produce();

        assertThat(next1).isNotNull();
        assertThat(next2).isNotNull();

        assertThat(next1).isNotEqualTo(next2);
        assertThat(next2).isNotEqualTo("#{name.fullName}");
    }
}