package io.kestra.plugin.datagen.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.plugins.AdditionalPlugin;
import io.kestra.core.plugins.serdes.PluginDeserializer;
import io.kestra.core.runners.RunContext;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.Objects;

@Plugin
@NoArgsConstructor
@SuperBuilder
@JsonDeserialize(using = PluginDeserializer.class)
public abstract class DataGenerator<T> extends AdditionalPlugin implements Producer<T> {

    protected RunContext runContext;

    /**
     * Initializes this {@link DataGenerator}.
     *
     * @param runContext the {@link RunContext}.
     */
    public void init(final RunContext runContext) throws IllegalVariableEvaluationException {
        this.runContext = Objects.requireNonNull(runContext, "runContext must not be null");
    }
}
