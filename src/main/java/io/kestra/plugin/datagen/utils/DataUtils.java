package io.kestra.plugin.datagen.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.serializers.JacksonMapper;
import org.slf4j.Logger;

public interface DataUtils {

    static long computeSize(Object o, Logger logger) {
        try {
            return o == null ? 0L : JacksonMapper.ofJson().writeValueAsBytes(o).length;
        } catch (JsonProcessingException e) {
            logger.warn("Failed to serialize data", e);
            return 0L;
        }
    }
}
