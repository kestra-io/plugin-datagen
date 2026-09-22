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
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.datagen.BatchGenerateInterface;
import io.kestra.plugin.datagen.Data;
import io.kestra.plugin.datagen.model.DataGenerator;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Plugin(
    aliases = {"io.kestra.plugin.datagen.Trigger"},
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
    title = "Poll to generate data batches",
    description = "Periodically invokes the generator and emits an execution. Defaults: `store=false`, `batchSize=1`, `interval=PT1S`. Use `store=true` to persist ION lines to internal storage instead of embedding the value."
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

    @Builder.Default
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    // Safe as a single shared field (not per-invocation state) because Kestra's scheduler
    // guarantees at most one in-flight evaluate() per trigger instance by default: this class
    // doesn't override AbstractTrigger#allowConcurrent (defaults to false), so
    // TriggerScheduler#processWorkerTrigger locks the trigger's TriggerState before dispatch and
    // TriggerStateStore excludes locked triggers from being fetched again, until the worker
    // reports the evaluate() result. Overlapping evaluate() calls on the same instance therefore
    // cannot happen under this trigger's configuration.
    @Getter(AccessLevel.NONE)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private volatile CompletableFuture<Data> generation;

    // Disambiguates the virtual thread name of each evaluation, including orphaned (killed but
    // still-running) generations, so concurrent orphans are distinguishable in a thread dump.
    // Static fields are already ignored by Lombok's generated getter/equals/hashCode/toString.
    private static final AtomicLong INVOCATION_COUNTER = new AtomicLong();

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        if (!isActive.get()) {
            return Optional.empty();
        }

        var task = Generate
            .builder()
            .id(this.id)
            .type(Generate.class.getName())
            .version(version)
            .store(store)
            .batchSize(batchSize)
            .generator(generator)
            .build();

        var runContext = conditionContext.getRunContext();
        var invocationId = INVOCATION_COUNTER.incrementAndGet();

        // Indirection to let the async task below check whether *its own* future was cancelled:
        // `future` can't be referenced inside its own initializer (illegal forward reference), and
        // `this.generation` isn't safe to check instead since it may already point to a later
        // invocation's future by the time an orphaned generation finishes. Set right after `future`
        // is created.
        var futureRef = new AtomicReference<CompletableFuture<Data>>();

        // run off the worker thread so kill() can abandon a blocked/slow generation without
        // waiting for it: produce() ignores interrupts and putFile() can block on IO. There is no
        // cap on the number of orphaned generations that can pile up if kill() is repeatedly
        // called against a genuinely stuck generator; in the normal case each one is bounded by
        // batchSize, so this is an accepted limitation rather than something bounded here.
        var future = CompletableFuture.supplyAsync(
            () -> {
                try {
                    var output = task.run(runContext);
                    var self = futureRef.get();
                    if (self != null && self.isCancelled()) {
                        // evaluate() already returned (via CancellationException below) by the time
                        // this finished: surface the otherwise-silent outcome of the orphaned generation.
                        // Checked against the future itself (not isActive) so a kill() that races the
                        // completion but arrives too late to actually cancel() isn't misreported as orphaned.
                        runContext.logger().debug("Trigger '{}' orphaned generation (invocation {}) completed successfully after being killed.", this.id, invocationId);
                    }
                    return output;
                } catch (Exception e) {
                    var self = futureRef.get();
                    if (self != null && self.isCancelled()) {
                        runContext.logger().warn("Trigger '{}' orphaned generation (invocation {}) failed after being killed.", this.id, invocationId, e);
                    }
                    throw new CompletionException(e);
                }
            },
            runnable -> Thread.ofVirtual().name("datagen-trigger-" + this.id + "-" + invocationId).start(runnable)
        );
        futureRef.set(future);
        this.generation = future;

        // kill() may have flipped isActive between the check above and this assignment; re-check
        // and self-cancel here so a kill() landing in that window isn't silently lost, which would
        // otherwise leave future.join() below blocking forever with no further kill signal to come.
        if (!isActive.get()) {
            future.cancel(false);
        }

        try {
            var output = future.join();
            return Optional.of(TriggerService.generateExecution(this, conditionContext, context, output));
        } catch (CancellationException e) {
            runContext.logger().debug("Trigger '{}' was killed while generating; the in-flight generation keeps running in the background until it completes.", this.id);
            return Optional.empty();
        } catch (CompletionException e) {
            switch (e.getCause()) {
                case Exception cause -> throw cause;
                case null, default -> throw e;
            }
        } finally {
            this.generation = null;
        }
    }

    @Override
    public void kill() {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        var inFlight = this.generation;
        if (inFlight != null) {
            // cancel(boolean) on a CompletableFuture ignores its argument entirely (per its javadoc)
            // and never interrupts the running task either way; false is used here simply to avoid
            // implying otherwise. The orphaned generation is left running not because it can safely
            // finish, but because there's no way to meaningfully stop it once started. In practice
            // the worker synchronously tears down this trigger's RunContext (including its working
            // directory) right after evaluate() returns, so the orphan's later file/storage writes
            // will almost always fail with an IOException (already logged as the "orphaned
            // generation ... failed" warn above). On the rare timing window where the write
            // completes first, it produces an ION file in internal storage that no Execution ever
            // references - an accepted, unbounded-but-rare leak, the same class of tradeoff as the
            // unbounded-orphans limitation already documented above.
            inFlight.cancel(false);
        }
    }
}
