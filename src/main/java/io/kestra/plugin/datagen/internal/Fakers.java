package io.kestra.plugin.datagen.internal;

import net.datafaker.Faker;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utilities for {@link Faker} library.
 */
public interface Fakers {

    static Faker create(final List<String> locale) {
        Faker faker;
        if (locale.isEmpty()) {
            faker = new Faker();
        } else {
            switch (locale.size()) {
                case 1 -> faker = new Faker(Locale.of(locale.get(0)));
                case 2 -> faker = new Faker(Locale.of(locale.get(0), locale.get(1)));
                case 3 -> faker = new Faker(Locale.of(locale.get(0), locale.get(1), locale.get(2)));
                default -> throw new IllegalArgumentException("Invalid value for property 'locale'. Expected format: [language, country, variant], but received: %s.".formatted(locale));
            }
        }
        return faker;
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> evaluate(Faker faker, Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            Object value = entry.getValue();

            if (value instanceof String) {
                String str = (String) value;
                if (str.startsWith("#{")) {
                    try {
                        entry.setValue(evaluate(faker, str));
                    } catch (Exception ignored) {
                        /* silently ignored */
                    }
                }
            } else if (value instanceof Map) {
                // Recursive call for nested maps
                evaluate(faker, (Map<String, Object>) value);
            } else if (value instanceof List<?> list) {
                // Evaluate strings in lists
                List<Object> evaluatedList = new ArrayList<>();
                for (Object item : list) {
                    if (item instanceof String && ((String) item).startsWith("#{")) {
                        evaluatedList.add(evaluate(faker, (String)item));
                    } else {
                        evaluatedList.add(item);
                    }
                }
                entry.setValue(evaluatedList);
            }
        }
        return map;
    }

    static String evaluate(Faker faker, String expression) {
        try {
            return faker.expression(expression);
        } catch (Exception e) {
           return expression;
        }
    }
}
