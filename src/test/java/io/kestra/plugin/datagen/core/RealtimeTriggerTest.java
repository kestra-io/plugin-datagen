package io.kestra.plugin.datagen.core;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.plugin.datagen.Data;
import io.kestra.plugin.datagen.services.DataEmitter;
import io.kestra.plugin.datagen.services.DataEmitterOptions;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

@KestraTest
class RealtimeTriggerTest {

    /**
     * A {@link DataEmitter} test double that records whether {@link #stop()} was invoked,
     * without blocking on termination.
     */
    static class RecordingDataEmitter extends DataEmitter {
        final AtomicBoolean stopped = new AtomicBoolean(false);

        RecordingDataEmitter() {
            super(
                () -> Data.builder().value("v").size(1L).build(),
                data -> {},
                new DataEmitterOptions(Long.MAX_VALUE, DataEmitterOptions.NO_THROUGHPUT, Duration.ofSeconds(15)),
                LoggerFactory.getLogger(RealtimeTriggerTest.class)
            );
        }

        @Override
        public void stop() {
            stopped.set(true);
        }
    }

    @Test
    void shouldStopEmitterOnNonBlockingStop() throws Exception {
        // Given a trigger with a running emitter.
        RealtimeTrigger trigger = RealtimeTrigger.builder()
            .id(UUID.randomUUID().toString())
            .type(RealtimeTrigger.class.getName())
            .build();

        RecordingDataEmitter emitter = new RecordingDataEmitter();
        injectEmitter(trigger, emitter);

        // When the (non-blocking) stop() is called.
        trigger.stop();

        // Then the emitter must be halted, even though stop() does not wait for termination.
        assertThat(emitter.stopped.get()).isTrue();
    }

    private static void injectEmitter(RealtimeTrigger trigger, DataEmitter emitter) throws Exception {
        Field field = RealtimeTrigger.class.getDeclaredField("dataEmitter");
        field.setAccessible(true);
        field.set(trigger, emitter);
    }
}
