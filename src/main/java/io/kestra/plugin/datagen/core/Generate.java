package io.kestra.plugin.datagen.core;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.datagen.BatchGenerateInterface;
import io.kestra.plugin.datagen.Data;
import io.kestra.plugin.datagen.model.DataGenerator;
import io.kestra.plugin.datagen.utils.DataUtils;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

@Plugin(
    aliases = {"io.kestra.plugin.datagen.Generate"},
    examples = {
        @Example(
            full = true,
            code = """
            id: datagen_person_json
            namespace: com.example.datagen

            tasks:
              - id: datagen
                type: io.kestra.plugin.datagen.core.Generate
                generator:
                  type: io.kestra.plugin.datagen.generators.JsonObjectGenerator
                  locale: ["fr", "FR"]
                  value:
                    name: "#{name.fullName}"
                    email: "#{internet.emailAddress}"
                    age: 30
                    address:
                      city: "#{address.city}"
                      zip: "#{address.zipCode}"
                    skills: [ "#{job.keySkills}", "#{job.position}", "hardcoded" ]
                    ts: "{{ now() }}"

              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Created: {{ outputs.datagen.value.name }} ({{ outputs.datagen.value.email }})!"
            """
        ),
        @Example(
            full = true,
            code = """
            id: datagen_person_json_batch
            namespace: com.example.datagen

            tasks:
              - id: datagen
                type: io.kestra.plugin.datagen.core.Generate
                batchSize: 100
                store: true
                generator:
                  type: io.kestra.plugin.datagen.generators.JsonObjectGenerator
                  locale: ["fr", "FR"]
                  value:
                    name: "#{name.fullName}"
                    email: "#{internet.emailAddress}"
                    age: 30
                    address:
                      city: "#{address.city}"
                      zip: "#{address.zipCode}"
                    skills: [ "#{job.keySkills}", "#{job.position}", "hardcoded" ]
                    ts: "{{ now() }}"

              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Created: {{ outputs.datagen.value.name }} ({{ outputs.datagen.value.email }})!"
            """
        ),
        @Example(
            full = true,
            code = """
            id: datagen_person_csv
            namespace: com.example.datagen
            inputs:
              - id: rows
                type: INT
                displayName: "Number of Rows"
                description: "Number of data rows to generate (excluding header)."
                defaults: 10

              - id: separator
                type: STRING
                displayName: "Separator"
                description: "Column separator character (e.g., ',', ';')."
                defaults: ","

              - id: header
                type: BOOL
                displayName: "Include header"
                description: "Include header row in the CSV output."
                defaults: true
            tasks:
              - id: datagen
                type: io.kestra.plugin.datagen.core.Generate
                generator:
                  type: io.kestra.plugin.datagen.generators.StringValueGenerator
                  locale: ["en", "US"]
                  value: |
                    #{csv '{{ inputs.separator }}','"','{{inputs.header}}','{{ inputs.rows}}','name_column','#{Name.first_name}','last_name_column','#{Name.last_name}'}

              - id: log
                type: io.kestra.plugin.core.log.Log
                message: |
                    {{ outputs.datagen.value }}
            """
        ),
    }
)
@Schema(
    title = "Generate synthetic data",
    description = "Runs the configured generator (e.g., [Datafaker](https://www.datafaker.net/documentation/expressions/)) once (inline) or for a batch when `store` is true. When stored, results are written as Ion lines to internal storage; defaults are `store=false` and `batchSize=1`."
)
@SuperBuilder
@NoArgsConstructor
@ToString
@EqualsAndHashCode
@Getter
public class Generate extends Task implements RunnableTask<Data>, BatchGenerateInterface {

    private DataGenerator<?> generator;

    @Builder.Default
    private Property<Boolean> store = Property.ofValue(DEFAULT_STORE);

    @Builder.Default
    private Property<Integer> batchSize = Property.ofValue(DEFAULT_BATCH_SIZE);

    @Override
    public Data run(RunContext runContext) throws Exception {

        Boolean store = runContext.render(this.store).as(Boolean.class).orElse(DEFAULT_STORE);
        int batchSize = runContext.render(this.batchSize).as(Integer.class).orElse(DEFAULT_BATCH_SIZE);

        this.generator.init(runContext);

        if (store) {
            File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
            try (
                BufferedOutputStream output = new BufferedOutputStream(new FileOutputStream(tempFile));
            ) {
                long totalSize = 0L;
                for (int i = 1; i <= batchSize; i++) {
                    Object value = generator.produce();
                    if (value != null) {
                        byte[] bytes = JacksonMapper.ofIon().writeValueAsBytes(value);
                        output.write(bytes);
                        output.write("\n".getBytes());
                        totalSize += bytes.length;
                    }
                }
                output.flush();
                URI uri = runContext.storage().putFile(tempFile);
                return Data
                    .builder()
                    .size(totalSize)
                    .count(batchSize)
                    .uri(uri)
                    .build();
            }
        } else {
            Object value = generator.produce();
            return Data
                .builder()
                .size(DataUtils.computeSize(value, runContext.logger()))
                .count(1)
                .value(value)
                .build();
        }
    }
}
