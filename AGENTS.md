# Kestra Datagen Plugin

## What

- Provides plugin components under `io.kestra.plugin.datagen`.
- Includes classes such as `Data`, `Trigger`, `RealtimeTrigger`, `Generate`.

## Why

- What user problem does this solve? Teams need to produce synthetic data payloads for triggering and exercising Kestra workflows from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps Datagen steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on Datagen.

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
