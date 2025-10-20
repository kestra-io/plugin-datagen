package io.kestra.plugin.datagen.core;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.datagen.Data;
import io.kestra.plugin.datagen.GenerateInterface;
import io.kestra.plugin.datagen.model.DataGenerator;
import io.kestra.plugin.datagen.model.Producer;
import io.kestra.plugin.datagen.services.DataEmitterOptions;
import io.kestra.plugin.datagen.services.DataEmitter;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Plugin(
    aliases = {"io.kestra.plugin.datagen.RealtimeTrigger"},
    examples = {
        @Example(
            full = true,
            code = """
            id: datagen_person_realtime_trigger_json
            namespace: com.example.datagen

            tasks:
              - id: log
                type: io.kestra.plugin.core.log.Log
                message: "Created: {{ trigger.value.name }} ({{ trigger.value.email }})!"

            triggers:
              - id: datagen
                type: io.kestra.plugin.datagen.core.RealtimeTrigger
                throughput: 10
                reportingInterval: PT5S
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
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Data>, GenerateInterface {

    @Schema(
        title = "Total Number of Records",
        description = "The total number of records to generate.  No further record will be generate once this number is reached."
    )
    @Builder.Default
    private Property<Long> maxRecords = Property.ofValue(Long.MAX_VALUE);

    @Schema(
        title = "Trigger Throughput",
        description = "The approximate number of records per second that will be created by this trigger."
    )
    @Builder.Default
    private Property<Integer> throughput = Property.ofValue(1);

    @Schema(
        title = "Reporting Interval",
        description = "The time interval at which reporting is performed during generation."
    )
    @Builder.Default
    private Property<Duration> reportingInterval = Property.ofValue(Duration.ofSeconds(15));

    @Schema(
        title = "Data Generator",
        description = "The data generator implementation responsible for producing the data."
    )
    @NotNull
    @PluginProperty
    private DataGenerator<?> generator;

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Getter(AccessLevel.NONE)
    private DataEmitter dataEmitter;

    /**
     * {@inheritDoc}
     **/
    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();

        DataEmitterOptions options = new DataEmitterOptions(
            runContext.render(this.maxRecords).as(Long.class).orElseThrow(),
            Math.max(1, runContext.render(this.throughput).as(Integer.class).orElseThrow()),
            runContext.render(this.reportingInterval).as(Duration.class).orElseThrow()
        );

        return Flux.create(emitter -> {

            // handle dispose - invoked after complete/error.
            emitter.onDispose(waitForTermination::countDown);

            // Create Generate Task
            Generate task = Generate
                .builder()
                .id(this.id)
                .type(Generate.class.getName())
                .version(version)
                .store(Property.ofValue(false))
                .batchSize(Property.ofValue(1))
                .generator(generator)
                .build();

            // Create DataEmitter
            Producer<Data> producer = () -> {
                try {
                    return task.run(runContext);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to generate data", e);
                }
            };
            Consumer<Data> consumer = data -> emitter.next(TriggerService.generateRealtimeExecution(this, conditionContext, context, data));
            dataEmitter = new DataEmitter(producer, consumer, options, runContext.logger());
            try {
                // run DataEmitter
                dataEmitter.run();
                emitter.complete();
            } catch (Exception throwable) {
                emitter.error(throwable);
            }
        });
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        if (wait) {
            try {
                if (dataEmitter != null) {
                    dataEmitter.stop();
                }
                waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
