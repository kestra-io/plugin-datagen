# How to use the Datagen plugin

Generate synthetic test data — JSON objects, strings, or random bytes — to drive and exercise Kestra flows without external data sources.

## Tasks

The generating tasks (`core.Generate`, `core.Trigger`, `core.RealtimeTrigger`) require a `generator` — choose from `JsonObjectGenerator` (set `value` as a map of field names to Datafaker expressions, e.g. `#{name.firstName}`), `StringValueGenerator` (set `value` as a string with Datafaker expressions), or `RandomBytesGenerator` (set `size` as the byte count). Optionally set `locale` on JSON and string generators to control Faker locale.

`core.Generate` produces data once — set `batchSize` to generate multiple records (default 1). Set `store: true` to write results to internal storage instead of returning them inline.

`core.Trigger` polls on a schedule (default every 1 second) and starts one execution per batch. Set `batchSize` and `store` the same way as `core.Generate`.

`core.RealtimeTrigger` generates records continuously — start one execution per record as it is produced. Control throughput with `throughput` (records per second, default 1) and cap total records with `maxRecords` (default unlimited).

`core.Pseudonymize` does not generate data — instead it replaces PII in an existing file. Set `from` (the URI of a CSV, JSON, or ION file in internal storage) and `fields` (a map of field paths to Datafaker expressions). For JSON and ION, use dot-notation paths to reach nested values (e.g. `user.profile.email`); for CSV, use column header names. Any field not listed is passed through unchanged. Set `contentType` (`CSV`, `JSON`, or `ION`) explicitly, or let it be auto-detected from the file extension.
