# Dredis.Extensions.Storage.Postgres

## A PostgreSQL key-value store implementation for Dredis (https://github.com/miserix13/Dredis)

## Current implementation scope

The first milestone implements the core `Dredis.Abstractions.Storage.IKeyValueStore` string surface against PostgreSQL:

- `GetAsync`
- `SetAsync`
- `GetManyAsync`
- `SetManyAsync`
- `DeleteAsync`
- `ExistsAsync`
- `IncrByAsync`
- `ExpireAsync`
- `PExpireAsync`
- `TtlAsync`
- `PttlAsync`
- `CleanUpExpiredKeysAsync`

Other `IKeyValueStore` members are present but currently throw `NotSupportedException` so the package can already integrate with Dredis while the PostgreSQL-backed feature set expands in later milestones.

## Build and test

```powershell
dotnet restore .\Dredis.Extensions.Storage.Postgres.slnx
dotnet build .\Dredis.Extensions.Storage.Postgres.slnx -c Release
dotnet test .\Dredis.Extensions.Storage.Postgres.slnx -c Release
```

The test project includes one PostgreSQL integration test for the core string operations. Set `DREDIS_POSTGRES_TEST_CONNECTION_STRING` to run it against a real PostgreSQL instance; otherwise that test returns early and the rest of the suite still runs.
