package io.kestra.plugin.datagen.generators;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.exceptions.KestraRuntimeException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.datagen.internal.Fakers;
import io.kestra.plugin.datagen.model.DataGenerator;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import net.datafaker.Faker;

import java.util.List;

@Schema(
    title = "String Value Generator",
    description = "Generates textual data based on fixed values, pebble expressions, or [Datafaker expressions](https://www.datafaker.net/documentation/expressions/)."
)
@Plugin
@NoArgsConstructor
@SuperBuilder
@JsonDeserialize
@Getter
public final class StringValueGenerator extends DataGenerator<String> {

    @Schema(
        title = "The string value",
        description = "The string value to generate for each output value"
    )
    @NotNull
    @PluginProperty
    private String value;

    @Schema(
        title = "Locales",
        description = "List of locale values in the format [language, country, variant] (e.g., [\"en\", \"US\"], [\"fr\", \"FR\"]). Controls the language and region of the generated data."
    )
    private Property<List<String>> locale;

    @Getter(AccessLevel.NONE)
    private Faker faker;

    /** {@inheritDoc} **/
    @Override
    public void init(RunContext runContext) throws IllegalVariableEvaluationException {
        super.init(runContext);
        this.faker = Fakers.create(runContext.render(this.locale).asList(String.class));
    }

    /** {@inheritDoc} **/
    @Override
    public String produce() {
        try {
            // because Property rendering is cached we can't use it directly
            return Fakers.evaluate(faker, runContext.render(value));
        } catch (IllegalVariableEvaluationException e) {
            throw new KestraRuntimeException("Failed to generate data", e);
        }
    }
}
