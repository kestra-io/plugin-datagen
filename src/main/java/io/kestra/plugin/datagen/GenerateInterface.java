package io.kestra.plugin.datagen;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.datagen.model.DataGenerator;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface GenerateInterface {
    
    @Schema(
        title = "Choose data generator",
        description = "Generator used for each record; required for all generate tasks and triggers."
    )
    @NotNull
    @PluginProperty
    DataGenerator getGenerator();
    
}
