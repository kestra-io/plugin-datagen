package io.kestra.plugin.datagen;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.net.URI;

@AllArgsConstructor
@Getter
@Builder
public class Data implements io.kestra.core.models.tasks.Output {

    @Schema(
        title = "Generated size (bytes)",
        description = "Total size in bytes of the returned value or stored Ion file."
    )
    private Long size;

    @Schema(
        title = "Items generated",
        description = "Number of records produced by the generator."
    )
    private Integer count;

    @Schema(
        title = "Generated value",
        description = "Inline content when `store` is false; null when data is stored. May be string, number, JSON object, or byte array depending on the generator."
    )
    private final Object value;

    @Schema(
        title = "Stored file URI",
        description = "URI in internal storage when `store` is true; null for inline outputs."
    )
    private final URI uri;
}
