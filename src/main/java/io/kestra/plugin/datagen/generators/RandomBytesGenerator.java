package io.kestra.plugin.datagen.generators;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.plugin.datagen.model.DataGenerator;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.security.SecureRandom;

@Schema(
    title = "Random Bytes Generator",
    description = "Generates random bytes"
)
@Plugin
@NoArgsConstructor
@SuperBuilder
@JsonDeserialize
@Getter
public class RandomBytesGenerator extends DataGenerator<byte[]> {

    @Schema(
        title = "Byte array size",
        description = "The number of bytes to generate for each output value"
    )
    @NotNull
    private int size;

    @Getter(AccessLevel.NONE)
    @Builder.Default
    private SecureRandom random = new SecureRandom();

    /**
     * {@inheritDoc }
     **/
    @Override
    public byte[] produce() {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }
}