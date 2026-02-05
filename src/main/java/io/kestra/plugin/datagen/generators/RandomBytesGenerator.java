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
    title = "Generate random byte arrays",
    description = "Allocates a byte array of the configured size and fills it with `SecureRandom` bytes. Size should be positive; randomness relies on the JVM's default seed."
)
@Plugin
@NoArgsConstructor
@SuperBuilder
@JsonDeserialize
@Getter
public class RandomBytesGenerator extends DataGenerator<byte[]> {

    @Schema(
        title = "Byte array size",
        description = "Number of bytes produced per record; should be greater than zero"
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
