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
import java.util.Map;

@Schema(
    title = "JSON Object Generator",
    description = "Generates structured objects"
)
@Plugin
@NoArgsConstructor
@SuperBuilder
@JsonDeserialize
@Getter
public final class JsonObjectGenerator extends DataGenerator<Map<String, Object>> {

    @Schema(
        title = "Object to generate",
        description = "A map of key-value pairs where values can contain [Datafaker expressions](https://www.datafaker.net/documentation/expressions/) (e.g., #{name.firstName}) to be evaluated for each output record."
    )
    @NotNull
    @PluginProperty
    private Map<String, Object> value;

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
    public Map<String, Object> produce() {
        try {
            Map<String, Object> objectMap = runContext.render(this.value);
            return Fakers.evaluate(faker, objectMap);
        } catch (IllegalVariableEvaluationException e) {
            throw new KestraRuntimeException("Failed to generate data", e);
        }
    }
}
