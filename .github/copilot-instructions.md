# Copilot Instructions

## Build, test, and lint commands

- Restore: `dotnet restore .\Dredis.Extensions.Storage.Postgres.slnx`
- Build: `dotnet build .\Dredis.Extensions.Storage.Postgres.slnx -c Release`
- Test: `dotnet test .\Dredis.Extensions.Storage.Postgres.slnx -c Release`
- Single test: `dotnet test .\Dredis.Extensions.Storage.Postgres.slnx -c Release --filter "FullyQualifiedName~PostgresKeyValueStoreTests"`
- Lint/format: no dedicated lint or formatting command is configured in this repository

## High-level architecture

- The repository contains one solution (`Dredis.Extensions.Storage.Postgres.slnx`) with a library project and an xUnit test project.
- Per the README, this library is intended to provide a PostgreSQL-backed key-value store implementation for the broader Dredis ecosystem.
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.cs` owns the shared PostgreSQL schema, string/bulk operations, TTL handling, integer increment, and parent-key lifecycle.
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.HashListSet.cs` implements the current collection slice: hashes, lists, and sets.
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.SortedSets.cs` implements the sorted-set slice, including rank/range operations, score-based range queries, and score updates.
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.Json.cs` implements the JSON slice, including document reads/writes, type/length projections, path deletes, and array mutations.
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.Streams.cs` implements the stream slice, including entry reads/writes, trimming, metadata queries, and consumer-group operations.
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.Vectors.cs` implements the vector slice, including set/get/delete, dimension queries, similarity metrics, and prefix search.
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.Unsupported.cs` holds the still-unimplemented parts of the upstream `Dredis.Abstractions.Storage.IKeyValueStore` contract as explicit `NotSupportedException` stubs. Move methods out of this file only when there is real PostgreSQL behavior and accompanying tests.
- The store depends directly on `Dredis.Abstractions.Storage` for the upstream contract and `Npgsql` for PostgreSQL access. Storage now uses one parent key table with `kind`, `value`, and `expires_at`, plus child tables for hash fields, list items, set members, sorted-set members, and stream data; JSON and vectors stay in the parent-row `value` payload and streams store their last-generated id there.
- `Dredis.Extensions.Storage.Postgres.Tests\PostgresKeyValueStoreTests.cs` mixes lightweight contract tests with PostgreSQL integration coverage for strings, hashes, lists, sets, sorted sets, JSON, streams, and vectors. The integration path uses the `DREDIS_POSTGRES_TEST_CONNECTION_STRING` environment variable.

## Key conventions

- Target framework is `net10.0`.
- `Nullable` and `ImplicitUsings` are enabled in the project file; keep nullability annotations accurate and avoid adding unnecessary `using` directives.
- Naming currently follows the package path directly: solution name, project name, namespace, and primary type all use `Dredis.Extensions.Storage.Postgres`.
- `PostgresKeyValueStore` accepts either a connection string or an `NpgsqlDataSource`. If you add constructor overloads or lifetime behavior, preserve the distinction between owned and externally supplied data sources.
- Custom table names are allowed, but only as unquoted PostgreSQL identifiers made of letters, digits, and underscores, optionally schema-qualified. That validation is intentional because the table name is injected into DDL/DML as an identifier, not a parameter.
- TTL lives on the parent key row, not the child tables. When adding new data types, preserve that pattern so `DeleteAsync`, `ExpireAsync`, `TtlAsync`, and expired-key cleanup continue to work uniformly across key kinds.
- Hash methods use exceptions for wrong-type access because the upstream hash signatures do not carry a status enum; list and set methods should continue using their result-status types for wrong-type cases.
- Sorted sets are ordered by `score` and then `member`, matching the current Dredis command handler expectations for deterministic rank and range results. Preserve that secondary ordering if you optimize the SQL later.
- JSON mutation is currently implemented in-memory against a parsed object model and then persisted back to the parent row. If you optimize it later with native PostgreSQL JSON features, preserve the current path semantics and result statuses first.
- Streams use `ms-seq` ids, keep last-generated id in the parent row, and hang entries, fields, consumer groups, consumers, and pending records off child tables. Stream add/read/range methods throw on wrong type, while the group/info methods use the upstream status/result objects.
- Vectors are stored as serialized payloads in the parent row and evaluated in-memory for similarity/search. Preserve the current metric semantics (`COSINE`, `DOT`, `L2`) and deterministic tie-breaking by key if you optimize later.
