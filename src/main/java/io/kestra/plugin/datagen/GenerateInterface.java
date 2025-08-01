package io.kestra.plugin.datagen;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.plugin.datagen.model.DataGenerator;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;

public interface GenerateInterface {
    
    @Schema(
        title = "The Data Generator",
        description = "The data generator implementation responsible for producing the execution data."
    )
    @NotNull
    @PluginProperty
    DataGenerator getGenerator();
    
}
