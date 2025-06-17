package io.kestra.plugin.datagen.services;

import java.time.Duration;

public record DataEmitterOptions(
    long numExecutions,
    long throughput,
    Duration reportingInterval){

    public static long NO_THROUGHPUT = -1L;
}
