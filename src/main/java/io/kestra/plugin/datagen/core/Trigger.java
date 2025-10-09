package io.kestra.plugin.datagen.core;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.PollingTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.plugin.datagen.BatchGenerateInterface;
import io.kestra.plugin.datagen.Data;
import io.kestra.plugin.datagen.model.DataGenerator;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;

@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
            id: datagen_person_trigger_json
            namespace: com.example.datagen

            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Created: {{ trigger.uri }}"

            triggers:
              - id: datagen
                type: io.kestra.plugin.datagen.core.Trigger
                batchSize: 10
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
            """
        )
    }
)
@Schema(
    title = "Generate data in real time",
    description = "This task continuously emits generated data in real time, using a configured data generator. It can be used to simulate event streams or high-throughput environments."
)
@NoArgsConstructor
@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Data>, BatchGenerateInterface {

    private DataGenerator<?> generator;

    @Builder.Default
    private Property<Boolean> store = Property.ofValue(false);

    @Builder.Default
    private Property<Integer> batchSize = Property.ofValue(1);

    @Builder.Default
    private final Duration interval = Duration.ofSeconds(1);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        Generate task = Generate
            .builder()
            .id(this.id)
            .type(Generate.class.getName())
            .version(version)
            .store(store)
            .batchSize(batchSize)
            .generator(generator)
            .build();

        Data output = task.run(conditionContext.getRunContext());
        return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
    }
}
