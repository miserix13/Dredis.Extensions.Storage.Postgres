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
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.cs` contains the PostgreSQL-backed core implementation for the first milestone: string/bulk operations, TTL handling, integer increment, schema initialization, and cleanup of expired rows.
- `Dredis.Extensions.Storage.Postgres\PostgresKeyValueStore.Unsupported.cs` implements the remainder of the upstream `Dredis.Abstractions.Storage.IKeyValueStore` contract as explicit `NotSupportedException` stubs. Extend this file by replacing stubs with real PostgreSQL-backed behavior as new data types are implemented.
- The store depends directly on `Dredis.Abstractions.Storage` for the upstream contract and `Npgsql` for PostgreSQL access. Storage is currently a single table with `key`, `value`, and `expires_at` columns.
- `Dredis.Extensions.Storage.Postgres.Tests\PostgresKeyValueStoreTests.cs` mixes lightweight contract tests with one PostgreSQL integration test. The integration path uses the `DREDIS_POSTGRES_TEST_CONNECTION_STRING` environment variable.

## Key conventions

- Target framework is `net10.0`.
- `Nullable` and `ImplicitUsings` are enabled in the project file; keep nullability annotations accurate and avoid adding unnecessary `using` directives.
- Naming currently follows the package path directly: solution name, project name, namespace, and primary type all use `Dredis.Extensions.Storage.Postgres`.
- `PostgresKeyValueStore` accepts either a connection string or an `NpgsqlDataSource`. If you add constructor overloads or lifetime behavior, preserve the distinction between owned and externally supplied data sources.
- Custom table names are allowed, but only as unquoted PostgreSQL identifiers made of letters, digits, and underscores, optionally schema-qualified. That validation is intentional because the table name is injected into DDL/DML as an identifier, not a parameter.
- New supported `IKeyValueStore` members should move from `PostgresKeyValueStore.Unsupported.cs` into the main implementation file only when they have real PostgreSQL behavior and tests.
