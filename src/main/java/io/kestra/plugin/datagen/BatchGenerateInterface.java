package io.kestra.plugin.datagen;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface BatchGenerateInterface extends GenerateInterface{

    boolean DEFAULT_STORE = false;
    int DEFAULT_BATCH_SIZE = 1;
    
    @Schema(
        title = "Store generated data",
        description = "Persist output to Kestra internal storage as Ion lines when true; defaults to false to return the value inline."
    )
    Property<Boolean> getStore();

    @Schema(
        title = "Batch size",
        description = "Number of items to generate when storing data; only used if `store` is true. Defaults to 1."
    )
    Property<Integer> getBatchSize();

}
