package io.kestra.plugin.datagen.core;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@KestraTest
class AnonymizeTest {

    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldAnonymizeCsvFlatColumns() throws Exception {
        var csv = "name,email,age\nJohn Doe,john@example.com,30\nJane Smith,jane@example.com,25\n";
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, csv, ".csv");

        var task = Anonymize.builder()
            .id("anonymize-csv")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .contentType(Property.ofValue(Anonymize.ContentType.CSV))
            .fields(Property.ofValue(Map.of(
                "name", "#{name.fullName}",
                "email", "#{internet.emailAddress}"
            )))
            .build();

        var output = task.run(runContext);

        assertThat(output).isNotNull();
        assertThat(output.getCount()).isEqualTo(2L);
        assertThat(output.getUri()).isNotNull();

        var result = readOutput(runContext, output.getUri());
        var lines = result.strip().split("\n");

        // Header preserved
        assertThat(lines[0]).isEqualTo("name,email,age");

        // Both data rows processed
        assertThat(lines).hasSize(3);

        // name and email replaced; age preserved
        var firstRow = lines[1].split(",");
        assertThat(firstRow[2]).isEqualTo("30");
        // Replaced values differ from the originals
        assertThat(lines[1]).doesNotContain("john@example.com");
        assertThat(lines[2]).doesNotContain("jane@example.com");
    }

    @Test
    void shouldAnonymizeJsonNestedPaths() throws Exception {
        var json = """
            {"user":{"profile":{"email":"secret@example.com","name":"John"},"age":30}}
            {"user":{"profile":{"email":"other@example.com","name":"Jane"},"age":25}}
            """;
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, json, ".json");

        var task = Anonymize.builder()
            .id("anonymize-json")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .contentType(Property.ofValue(Anonymize.ContentType.JSON))
            .fields(Property.ofValue(Map.of(
                "user.profile.email", "#{internet.emailAddress}",
                "user.profile.name", "#{name.fullName}"
            )))
            .build();

        var output = task.run(runContext);

        assertThat(output.getCount()).isEqualTo(2L);

        var result = readOutput(runContext, output.getUri());
        var lines = result.strip().split("\n");
        assertThat(lines).hasSize(2);

        // Replaced values differ from originals
        assertThat(lines[0]).doesNotContain("secret@example.com");
        assertThat(lines[1]).doesNotContain("other@example.com");

        // Untouched field preserved
        assertThat(lines[0]).contains("\"age\":30");
    }

    @Test
    void shouldPreserveFieldsNotListedInFieldsMap() throws Exception {
        var csv = "name,email,age\nAlice,alice@example.com,28\n";
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, csv, ".csv");

        var task = Anonymize.builder()
            .id("anonymize-passthrough")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .contentType(Property.ofValue(Anonymize.ContentType.CSV))
            .fields(Property.ofValue(Map.of("name", "#{name.fullName}")))
            .build();

        var output = task.run(runContext);

        var result = readOutput(runContext, output.getUri());
        var lines = result.strip().split("\n");

        var cols = lines[1].split(",");
        // email (index 1) and age (index 2) preserved
        assertThat(cols[1]).isEqualTo("alice@example.com");
        assertThat(cols[2]).isEqualTo("28");
    }

    @Test
    void shouldSilentlySkipMissingJsonPath() throws Exception {
        var json = "{\"name\":\"Bob\",\"age\":40}\n";
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, json, ".json");

        var task = Anonymize.builder()
            .id("anonymize-skip")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .contentType(Property.ofValue(Anonymize.ContentType.JSON))
            .fields(Property.ofValue(Map.of(
                "user.profile.email", "#{internet.emailAddress}"  // path does not exist
            )))
            .build();

        // Should not throw; record written as-is
        var output = task.run(runContext);

        assertThat(output.getCount()).isEqualTo(1L);
        var result = readOutput(runContext, output.getUri());
        assertThat(result).contains("\"name\":\"Bob\"");
        assertThat(result).contains("\"age\":40");
    }

    @Test
    void shouldReturnZeroCountForEmptyInput() throws Exception {
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, "name,email\n", ".csv");

        var task = Anonymize.builder()
            .id("anonymize-empty")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .contentType(Property.ofValue(Anonymize.ContentType.CSV))
            .fields(Property.ofValue(Map.of("name", "#{name.fullName}")))
            .build();

        var output = task.run(runContext);

        assertThat(output.getCount()).isEqualTo(0L);
    }

    @Test
    void shouldPassThroughWhenFieldsMapIsEmpty() throws Exception {
        var json = "{\"name\":\"Carol\",\"age\":35}\n";
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, json, ".json");

        var task = Anonymize.builder()
            .id("anonymize-no-fields")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .contentType(Property.ofValue(Anonymize.ContentType.JSON))
            .fields(Property.ofValue(Map.of()))
            .build();

        var output = task.run(runContext);

        assertThat(output.getCount()).isEqualTo(1L);
        var result = readOutput(runContext, output.getUri());
        assertThat(result).contains("\"name\":\"Carol\"");
    }

    @Test
    void shouldThrowOnUnrecognisedExtensionWithoutContentType() throws Exception {
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, "data", ".txt");

        var task = Anonymize.builder()
            .id("anonymize-unknown-ext")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .fields(Property.ofValue(Map.of("name", "#{name.fullName}")))
            .build();

        assertThatThrownBy(() -> task.run(runContext))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("content type");
    }

    @Test
    void shouldNotStripIonAnnotationWhenColonBraceInJsonValue() throws Exception {
        // A JSON value containing '::{' must not be falsely treated as an Ion annotation prefix
        var json = "{\"key\":\"foo::{bar}\",\"age\":5}\n";
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, json, ".json");

        var task = Anonymize.builder()
            .id("anonymize-ion-false-positive")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .contentType(Property.ofValue(Anonymize.ContentType.JSON))
            .fields(Property.ofValue(Map.of("age", "#{number.numberBetween '1','99'}")))
            .build();

        var output = task.run(runContext);

        assertThat(output.getCount()).isEqualTo(1L);
        // The record must have been parsed correctly: key field is untouched
        var result = readOutput(runContext, output.getUri());
        assertThat(result).contains("\"key\":\"foo::{bar}\"");
    }

    @Test
    void shouldWriteThroughAndWarnOnUnparsableJsonLine() throws Exception {
        // A malformed JSON line must be written through un-anonymized, and count must still increment
        var json = "not-valid-json\n{\"name\":\"Bob\"}\n";
        var runContext = runContextFactory.of();
        var inputUri = uploadText(runContext, json, ".json");

        var task = Anonymize.builder()
            .id("anonymize-bad-json")
            .type(Anonymize.class.getName())
            .from(Property.ofValue(inputUri.toString()))
            .contentType(Property.ofValue(Anonymize.ContentType.JSON))
            .fields(Property.ofValue(Map.of("name", "#{name.fullName}")))
            .build();

        var output = task.run(runContext);

        // Both lines counted (bad line written through, good line anonymized)
        assertThat(output.getCount()).isEqualTo(2L);
        var result = readOutput(runContext, output.getUri());
        var lines = result.strip().split("\n");
        // First line written through verbatim
        assertThat(lines[0]).isEqualTo("not-valid-json");
        // Second line anonymized (name replaced)
        assertThat(lines[1]).doesNotContain("\"name\":\"Bob\"");
    }

    // --- Helpers ---

    private URI uploadText(io.kestra.core.runners.RunContext runContext, String text, String suffix) throws Exception {
        var tempFile = File.createTempFile("anonymize-test", suffix);
        tempFile.deleteOnExit();
        try (var writer = new BufferedWriter(new FileWriter(tempFile, StandardCharsets.UTF_8))) {
            writer.write(text);
        }
        return runContext.storage().putFile(tempFile);
    }

    private String readOutput(io.kestra.core.runners.RunContext runContext, URI uri) throws Exception {
        try (InputStream is = runContext.storage().getFile(uri)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
