# How to use the Datagen plugin

Generate synthetic test data — JSON objects, strings, or random bytes — to drive and exercise Kestra flows without external data sources.

## Tasks

All tasks require a `generator` — choose from `JsonObjectGenerator` (set `value` as a map of field names to Datafaker expressions, e.g. `#{name.firstName}`), `StringValueGenerator` (set `value` as a string with Datafaker expressions), or `RandomBytesGenerator` (set `size` as the byte count). Optionally set `locale` on JSON and string generators to control Faker locale.

`core.Generate` produces data once — set `batchSize` to generate multiple records (default 1). Set `store: true` to write results to internal storage instead of returning them inline.

`core.Trigger` polls on a schedule (default every 1 second) and starts one execution per batch. Set `batchSize` and `store` the same way as `core.Generate`.

`core.RealtimeTrigger` generates records continuously — start one execution per record as it is produced. Control throughput with `throughput` (records per second, default 1) and cap total records with `maxRecords` (default unlimited).
