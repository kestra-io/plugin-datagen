# Kestra Datagen Plugin

## What

description = 'Plugin Data Gen for Kestra Exposes 3 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with Datagen, allowing orchestration of Datagen-based operations as part of data pipelines and automation workflows.

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

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
