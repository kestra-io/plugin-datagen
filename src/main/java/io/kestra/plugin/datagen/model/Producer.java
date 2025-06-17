package io.kestra.plugin.datagen.model;

/**
 * Service interface for producing item.
 *
 * @param <T> type of the item.
 */
@FunctionalInterface
public interface Producer<T> {
    /**
     * Gets the next produced item.
     *
     * @return the produced item
     */
    T produce();
}
