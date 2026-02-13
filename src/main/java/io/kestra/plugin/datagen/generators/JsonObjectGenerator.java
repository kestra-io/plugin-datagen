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
    title = "Generate JSON objects from templates",
    description = "Renders the `value` map, then evaluates Datafaker expressions (`#{...}`) on every record. Supports nested maps/lists; locale list overrides Faker locale, otherwise the library default is used."
)
@Plugin
@NoArgsConstructor
@SuperBuilder
@JsonDeserialize
@Getter
public final class JsonObjectGenerator extends DataGenerator<Map<String, Object>> {

    @Schema(
        title = "Object template",
        description = "Map of key-value pairs rendered per record; strings starting with `#{` are evaluated by [Datafaker](https://www.datafaker.net/documentation/expressions/), including nested maps and lists."
    )
    @NotNull
    @PluginProperty
    private Map<String, Object> value;

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
    public Map<String, Object> produce() {
        try {
            Map<String, Object> objectMap = runContext.render(this.value);
            return Fakers.evaluate(faker, objectMap);
        } catch (IllegalVariableEvaluationException e) {
            throw new KestraRuntimeException("Failed to generate data", e);
        }
    }
}
