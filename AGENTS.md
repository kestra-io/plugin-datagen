# Kestra Datagen Plugin

## What

- Provides plugin components under `io.kestra.plugin.datagen`.
- Includes classes such as `Data`, `Trigger`, `RealtimeTrigger`, `Generate`.

## Why

- This plugin integrates Kestra with Datagen Core.
- It provides tasks that generate synthetic records on demand or emit them continuously.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `datagen`

Infrastructure dependencies (Docker Compose services):

- `app`

### Key Plugin Classes

- `io.kestra.plugin.datagen.core.Generate`
- `io.kestra.plugin.datagen.core.RealtimeTrigger`
- `io.kestra.plugin.datagen.core.Trigger`

### Project Structure

```
plugin-datagen/
├── src/main/java/io/kestra/plugin/datagen/utils/
├── src/test/java/io/kestra/plugin/datagen/utils/
├── build.gradle
└── README.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
