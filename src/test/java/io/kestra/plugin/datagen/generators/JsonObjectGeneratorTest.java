package io.kestra.plugin.datagen.generators;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@KestraTest
class JsonObjectGeneratorTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldGenerateData() throws IllegalVariableEvaluationException {
        // Given
        JsonObjectGenerator generator = JsonObjectGenerator
            .builder()
            .value(Map.of(
                "name", "#{name.fullName}",
                "email", "#{internet.emailAddress}",
                "age", 30,
                "address", Map.of(
                    "city", "#{address.city}",
                    "zip", "#{address.zipCode}"
                ),
                "skills", List.of("#{job.keySkills}", "#{job.position}", "hardcoded"),
                "ts", "{{ now() }}",
                "tag", "hardcoded"
            ))
            .build();
        generator.init(runContextFactory.of());

        // When
        Map<String, Object> next1 = generator.produce();
        Map<String, Object> next2 = generator.produce();

        assertThat(next1).isNotNull();
        assertThat(next2).isNotNull();

        assertThat(next1).isNotEqualTo(next2);
        assertThat(next1.get("tag")).isEqualTo("hardcoded");
        assertThat(next1.get("name")).isNotEqualTo("#{name.fullName}");
        assertThat(next1.get("email")).isNotEqualTo("#{internet.emailAddress}");
        assertThat(next1.get("age")).isEqualTo(30);
    }
}