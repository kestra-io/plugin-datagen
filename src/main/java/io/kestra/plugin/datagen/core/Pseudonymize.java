package io.kestra.plugin.datagen.core;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Output;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.datagen.internal.Fakers;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import net.datafaker.Faker;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Plugin(
    examples = {
        @Example(
            title = "Pseudonymize flat columns in a CSV export",
            full = true,
            code = """
            id: pseudonymize_customer_csv
            namespace: company.team

            tasks:
              - id: pseudonymize
                type: io.kestra.plugin.datagen.core.Pseudonymize
                from: "{{ inputs.file }}"
                contentType: CSV
                locale: ["en", "US"]
                fields:
                  first_name: "#{name.first_name}"
                  last_name: "#{name.last_name}"
                  email: "#{internet.emailAddress}"
                  phone: "#{phoneNumber.cellPhone}"
                  address: "#{address.fullAddress}"

              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Pseudonymized {{ outputs.pseudonymize.count }} records: {{ outputs.pseudonymize.uri }}"
            """
        ),
        @Example(
            title = "Pseudonymize nested fields in a JSON export",
            full = true,
            code = """
            id: pseudonymize_customer_json
            namespace: company.team

            tasks:
              - id: pseudonymize
                type: io.kestra.plugin.datagen.core.Pseudonymize
                from: "{{ outputs.previous_task.uri }}"
                contentType: JSON
                locale: ["fr", "FR"]
                fields:
                  "user.profile.fullName": "#{name.fullName}"
                  "user.profile.email": "#{internet.emailAddress}"
                  "user.address.city": "#{address.city}"
                  "user.address.zipCode": "#{address.zipCode}"

              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Pseudonymized {{ outputs.pseudonymize.count }} JSON records — result at {{ outputs.pseudonymize.uri }}"
            """
        ),
        @Example(
            title = "Nightly GDPR-safe data preparation on a schedule",
            full = true,
            code = """
            id: nightly_pseudonymize_export
            namespace: company.team

            triggers:
              - id: nightly
                type: io.kestra.plugin.core.trigger.Schedule
                cron: "0 2 * * *"

            tasks:
              - id: pseudonymize
                type: io.kestra.plugin.datagen.core.Pseudonymize
                from: "{{ vars.daily_export_uri }}"
                contentType: CSV
                locale: ["en", "US"]
                fields:
                  customer_name: "#{name.fullName}"
                  customer_email: "#{internet.emailAddress}"
                  customer_phone: "#{phoneNumber.cellPhone}"
                  national_id: "#{idNumber.ssnValid}"

              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Nightly pseudonymization complete — {{ outputs.pseudonymize.count }} records written to {{ outputs.pseudonymize.uri }}"
            """
        ),
        @Example(
            title = "Generate a CSV of superhero real names and pseudonymize them",
            full = true,
            code = """
            id: pseudonymize_superhero_csv
            namespace: company.team

            tasks:
              - id: working_directory
                type: io.kestra.plugin.core.flow.WorkingDir
                tasks:
                  - id: generate_csv_script
                    type: io.kestra.plugin.scripts.python.Commands
                    taskRunner:
                      type: io.kestra.plugin.core.runner.Process
                    outputFiles:
                      - "heroes.csv"
                    commands:
                      - |
                        cat << 'PYEOF' > generate.py
                        import csv

                        data = [
                            {"id": 1,  "name": "Clark Kent",      "age": 35},
                            {"id": 2,  "name": "Bruce Wayne",      "age": 38},
                            {"id": 3,  "name": "Diana Prince",     "age": 30},
                            {"id": 4,  "name": "Peter Parker",     "age": 22},
                            {"id": 5,  "name": "Tony Stark",       "age": 48},
                            {"id": 6,  "name": "Natasha Romanoff", "age": 35},
                            {"id": 7,  "name": "Steve Rogers",     "age": 32},
                            {"id": 8,  "name": "Bruce Banner",     "age": 40},
                            {"id": 9,  "name": "Barry Allen",      "age": 28},
                            {"id": 10, "name": "Wanda Maximoff",   "age": 26},
                        ]

                        with open("heroes.csv", "w", newline="") as f:
                            writer = csv.DictWriter(f, fieldnames=["id", "name", "age"])
                            writer.writeheader()
                            writer.writerows(data)
                        PYEOF
                        python generate.py

              - id: pseudonymize
                type: io.kestra.plugin.datagen.core.Pseudonymize
                from: "{{ outputs.generate_csv_script.outputFiles['heroes.csv'] }}"
                contentType: CSV
                fields:
                  name: "#{name.fullName}"

              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Pseudonymized {{ outputs.pseudonymize.count }} superhero records — result at {{ outputs.pseudonymize.uri }}"
            """
        ),
    }
)
@Schema(
    title = "Pseudonymize PII fields in a structured file",
    description = """
        Reads a CSV or JSON input file from internal storage, replaces the specified fields with
        realistic fake values generated by [Datafaker](https://www.datafaker.net/documentation/expressions/),
        and writes the pseudonymized result back to internal storage.

        For JSON files, fields are addressed with dot-notation to reach nested structures
        (e.g. `user.profile.email`). For CSV files, fields are matched by column header name.
        Any field not listed in `fields` is passed through unchanged.
        A path that does not exist in a record is silently skipped.
        """
)
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class Pseudonymize extends Task implements RunnableTask<Pseudonymize.PseudonymizeOutput> {

    @Schema(
        title = "Input file URI",
        description = "URI of the file in Kestra internal storage to pseudonymize."
    )
    @NotNull
    @PluginProperty(group = "source")
    private Property<String> from;

    @Schema(
        title = "Fields to pseudonymize",
        description = """
            Map of field paths to Datafaker expressions. For JSON, dot-notation paths (e.g.
            `user.profile.email`) address nested structures. For CSV, flat column names are used.
            Fields absent from this map are preserved verbatim.
            """
    )
    @PluginProperty(group = "main")
    private Property<Map<String, String>> fields;

    @Schema(
        title = "Content type",
        description = "Input (and output) format: `CSV`, `JSON`, or `ION`. When absent, auto-detected from the file extension (`.csv`, `.json`, or `.ion`)."
    )
    @PluginProperty(group = "main")
    private Property<ContentType> contentType;

    @Schema(
        title = "Locales",
        description = "Optional Faker locale list in the format `[language]`, `[language, country]`, or `[language, country, variant]`. Defaults to Faker's built-in locale when omitted."
    )
    @PluginProperty(group = "advanced")
    private Property<List<String>> locale;

    @Override
    public PseudonymizeOutput run(RunContext runContext) throws Exception {
        var rFrom = runContext.render(this.from).as(String.class).orElseThrow();
        var rFields = this.fields != null
            ? runContext.render(this.fields).asMap(String.class, String.class)
            : Map.<String, String>of();
        var rLocale = this.locale != null
            ? runContext.render(this.locale).asList(String.class)
            : List.<String>of();

        var faker = Fakers.create(rLocale);
        var inputUri = URI.create(rFrom);
        var resolvedContentType = resolveContentType(runContext, inputUri);

        var ext = switch (resolvedContentType) {
            case CSV -> ".csv";
            case ION -> ".ion";
            case JSON -> ".json";
        };
        var tempFile = runContext.workingDir().createTempFile(ext).toFile();
        long count;

        try (var inputStream = runContext.storage().getFile(inputUri)) {
            count = switch (resolvedContentType) {
                case CSV -> processCsv(inputStream, tempFile, faker, rFields);
                case JSON -> processJson(runContext, inputStream, tempFile, faker, rFields);
                case ION -> processIon(inputStream, tempFile, faker, rFields);
            };
        }

        var outputUri = runContext.storage().putFile(tempFile);
        runContext.logger().info("Pseudonymized {} record(s) written to {}", count, outputUri);

        return PseudonymizeOutput.builder()
            .uri(outputUri)
            .count(count)
            .build();
    }

    private ContentType resolveContentType(RunContext runContext, URI inputUri) throws Exception {
        if (this.contentType != null) {
            return runContext.render(this.contentType).as(ContentType.class).orElseThrow();
        }
        var path = inputUri.getPath().toLowerCase(Locale.ROOT);
        if (path.endsWith(".csv")) {
            return ContentType.CSV;
        } else if (path.endsWith(".json")) {
            return ContentType.JSON;
        } else if (path.endsWith(".ion")) {
            return ContentType.ION;
        }
        throw new IllegalArgumentException(
            "Cannot auto-detect content type from URI '%s'. Set the 'contentType' property explicitly.".formatted(inputUri)
        );
    }

    private long processCsv(InputStream inputStream, File outputFile, Faker faker, Map<String, String> fields) throws IOException {
        try (
            var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            var writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8))
        ) {
            var headerLine = reader.readLine();
            if (headerLine == null) {
                return 0L;
            }

            var headers = parseCsvLine(headerLine);
            writer.write(headerLine);
            writer.newLine();

            // Build O(1) column index once so wide CSVs don't pay O(columns) per field per row
            Map<String, Integer> columnIndex = new HashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                columnIndex.put(headers.get(i), i);
            }

            long count = 0L;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                var values = parseCsvLine(line);
                var record = new ArrayList<>(values);
                while (record.size() < headers.size()) {
                    record.add("");
                }

                for (var entry : fields.entrySet()) {
                    var idx = columnIndex.getOrDefault(entry.getKey(), -1);
                    if (idx >= 0 && idx < record.size()) {
                        record.set(idx, Fakers.evaluate(faker, entry.getValue()));
                    }
                }

                writer.write(buildCsvLine(record));
                writer.newLine();
                count++;
            }
            return count;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private long processJson(RunContext runContext, InputStream inputStream, File outputFile, Faker faker, Map<String, String> fields) throws IOException {
        var mapper = JacksonMapper.ofJson();

        try (
            var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            var writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8))
        ) {
            long count = 0L;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                // Strip Ion type annotations if present (e.g. produced by Generate with store=true)
                var jsonLine = stripIonAnnotation(line);

                Map<String, Object> record;
                try {
                    record = mapper.readValue(jsonLine, mapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, Object.class));
                } catch (Exception e) {
                    runContext.logger().warn("Record {} could not be parsed as JSON and was written through un-pseudonymized", count);
                    writer.write(line);
                    writer.newLine();
                    count++;
                    continue;
                }

                for (var entry : fields.entrySet()) {
                    applyDotPath(record, entry.getKey(), faker, entry.getValue());
                }

                writer.write(mapper.writeValueAsString(record));
                writer.newLine();
                count++;
            }
            return count;
        }
    }

    @SuppressWarnings("unchecked")
    private long processIon(InputStream inputStream, File outputFile, Faker faker, Map<String, String> fields) throws Exception {
        try (
            var reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            var writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.UTF_8))
        ) {
            var flowable = FileSerde.readAll(reader)
                .map(row -> {
                    if (row instanceof Map<?, ?> map) {
                        var record = (Map<String, Object>) map;
                        for (var entry : fields.entrySet()) {
                            applyDotPath(record, entry.getKey(), faker, entry.getValue());
                        }
                    }
                    return row;
                });
            Long count = FileSerde.writeAll(writer, flowable).block();
            return count != null ? count : 0L;
        }
    }

    /**
     * Strips a leading Ion type annotation (e.g. {@code 'SomeType'::{...}} → {@code {...}})
     * so Ion-serialized lines produced by Generate can be parsed as plain JSON.
     * Search is restricted to the prefix before the first '{' to avoid false-positives
     * when JSON values contain '::{'.
     */
    private static String stripIonAnnotation(String line) {
        var trimmed = line.trim();
        var firstBrace = trimmed.indexOf('{');
        var ionMarker = (firstBrace > 0) ? trimmed.lastIndexOf("::{", firstBrace) : -1;
        if (ionMarker >= 0) {
            return trimmed.substring(ionMarker + 2);
        }
        return trimmed;
    }

    @SuppressWarnings("unchecked")
    private static void applyDotPath(Map<String, Object> record, String dotPath, Faker faker, String expression) {
        var parts = dotPath.split("\\.", -1);
        Map<String, Object> current = record;

        for (int i = 0; i < parts.length - 1; i++) {
            var next = current.get(parts[i]);
            if (!(next instanceof Map)) {
                // Intermediate node missing or not a map: silently skip
                return;
            }
            current = (Map<String, Object>) next;
        }

        var leaf = parts[parts.length - 1];
        if (current.containsKey(leaf)) {
            current.put(leaf, Fakers.evaluate(faker, expression));
        }
    }

    private static List<String> parseCsvLine(String line) {
        var result = new ArrayList<String>();
        var sb = new StringBuilder();
        var inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            var ch = line.charAt(i);
            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                result.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(ch);
            }
        }
        result.add(sb.toString());
        return result;
    }

    private static String buildCsvLine(List<String> values) {
        var sb = new StringBuilder();
        for (int i = 0; i < values.size(); i++) {
            if (i > 0) sb.append(',');
            var val = values.get(i);
            if (val.contains(",") || val.contains("\"") || val.contains("\n")) {
                sb.append('"').append(val.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(val);
            }
        }
        return sb.toString();
    }

    public enum ContentType {
        CSV,
        JSON,
        ION
    }

    @Schema(title = "Pseudonymize task output")
    @Builder
    @Getter
    public static class PseudonymizeOutput implements Output {

        @Schema(
            title = "Output file URI",
            description = "URI of the pseudonymized file in Kestra internal storage."
        )
        private final URI uri;

        @Schema(
            title = "Records processed",
            description = "Number of records read and written (including records where no field was replaced)."
        )
        private final long count;
    }
}
