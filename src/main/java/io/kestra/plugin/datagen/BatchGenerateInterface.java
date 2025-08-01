package io.kestra.plugin.datagen;

import io.kestra.core.models.property.Property;
import io.swagger.v3.oas.annotations.media.Schema;

public interface BatchGenerateInterface extends GenerateInterface{

    boolean DEFAULT_STORE = false;
    int DEFAULT_BATCH_SIZE = 1;
    
    @Schema(
        title = "Store generated data",
        description = "If true, the generated data will be persisted to Kestra's internal storage. If false, the data is emitted part of the task output."
    )
    Property<Boolean> getStore();

    @Schema(
        title = "Batch size",
        description = "The number of items to generate when storing data. This is only applicable when 'store' is set to true."
    )
    Property<Integer> getBatchSize();

}
