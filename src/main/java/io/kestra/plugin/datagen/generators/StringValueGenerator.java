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
    title = "Generate strings from templates",
    description = "Renders the `value` string (Pebble) and then evaluates [Datafaker expressions](https://www.datafaker.net/documentation/expressions/) like `#{name.firstName}` for each record. Locale list overrides Faker locale; empty list uses the library default."
)
@Plugin
@NoArgsConstructor
@SuperBuilder
@JsonDeserialize
@Getter
public final class StringValueGenerator extends DataGenerator<String> {

    @Schema(
        title = "String template",
        description = "String rendered per record; supports Pebble variables and Datafaker expressions starting with `#{`"
    )
    @NotNull
    @PluginProperty
    private String value;

    @Schema(
        title = "Locales",
        description = "Optional locale list in the format [language, country, variant]; empty list uses Faker's default locale."
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
