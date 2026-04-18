# Dredis.Extensions.Storage.Postgres

## A PostgreSQL key-value store implementation for Dredis (https://github.com/miserix13/Dredis)

## Current implementation scope

The current PostgreSQL-backed milestone covers the core `Dredis.Abstractions.Storage.IKeyValueStore` string surface plus hashes, lists, sets, sorted sets, JSON, stream, vector, and probabilistic operations:

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
- `HashSetAsync`
- `HashGetAsync`
- `HashDeleteAsync`
- `HashGetAllAsync`
- `ListPushAsync`
- `ListPopAsync`
- `ListRangeAsync`
- `ListLengthAsync`
- `ListIndexAsync`
- `ListSetAsync`
- `ListTrimAsync`
- `SetAddAsync`
- `SetRemoveAsync`
- `SetMembersAsync`
- `SetCardinalityAsync`
- `SortedSetAddAsync`
- `SortedSetRemoveAsync`
- `SortedSetRangeAsync`
- `SortedSetCardinalityAsync`
- `SortedSetScoreAsync`
- `SortedSetRangeByScoreAsync`
- `SortedSetIncrementAsync`
- `SortedSetCountByScoreAsync`
- `SortedSetRankAsync`
- `SortedSetReverseRankAsync`
- `SortedSetRemoveRangeByScoreAsync`
- `JsonSetAsync`
- `JsonGetAsync`
- `JsonDelAsync`
- `JsonTypeAsync`
- `JsonStrlenAsync`
- `JsonArrlenAsync`
- `JsonArrappendAsync`
- `JsonArrindexAsync`
- `JsonArrinsertAsync`
- `JsonArrremAsync`
- `JsonArrtrimAsync`
- `JsonMgetAsync`
- `StreamAddAsync`
- `StreamDeleteAsync`
- `StreamLengthAsync`
- `StreamLastIdAsync`
- `StreamReadAsync`
- `StreamRangeAsync`
- `StreamRangeReverseAsync`
- `StreamTrimAsync`
- `StreamInfoAsync`
- `StreamSetIdAsync`
- `StreamGroupCreateAsync`
- `StreamGroupDestroyAsync`
- `StreamGroupSetIdAsync`
- `StreamGroupDelConsumerAsync`
- `StreamGroupReadAsync`
- `StreamAckAsync`
- `StreamPendingAsync`
- `StreamClaimAsync`
- `StreamGroupsInfoAsync`
- `StreamConsumersInfoAsync`
- `VectorSetAsync`
- `VectorGetAsync`
- `VectorSizeAsync`
- `VectorSimilarityAsync`
- `VectorDeleteAsync`
- `VectorSearchAsync`
- `BloomReserveAsync`
- `BloomAddAsync`
- `BloomMAddAsync`
- `BloomExistsAsync`
- `BloomMExistsAsync`
- `BloomInfoAsync`
- `CuckooReserveAsync`
- `CuckooAddAsync`
- `CuckooAddNxAsync`
- `CuckooExistsAsync`
- `CuckooDeleteAsync`
- `CuckooCountAsync`
- `CuckooInfoAsync`
- `HyperLogLogAddAsync`
- `HyperLogLogCountAsync`
- `HyperLogLogMergeAsync`
- `TDigestCreateAsync`
- `TDigestResetAsync`
- `TDigestAddAsync`
- `TDigestQuantileAsync`
- `TDigestCdfAsync`
- `TDigestRankAsync`
- `TDigestRevRankAsync`
- `TDigestByRankAsync`
- `TDigestByRevRankAsync`
- `TDigestTrimmedMeanAsync`
- `TDigestMinAsync`
- `TDigestMaxAsync`
- `TDigestInfoAsync`
- `TopKReserveAsync`
- `TopKAddAsync`
- `TopKIncrByAsync`
- `TopKQueryAsync`
- `TopKCountAsync`
- `TopKListAsync`
- `TopKInfoAsync`

Other `IKeyValueStore` members are present but currently throw `NotSupportedException` so the package can already integrate with Dredis while the PostgreSQL-backed feature set expands in later milestones.

Internally, the store now uses a shared key metadata table with `kind`, `value`, and `expires_at` columns, plus per-type child tables for hash fields, list items, set members, sorted-set members, and stream entries/group state. JSON documents, vectors, and probabilistic structures are stored directly in the parent row payload, and streams persist their last-generated id in the parent row while entries, fields, groups, consumers, and pending metadata live in child tables. TTL is tracked on the parent key row and child rows are deleted through foreign-key cascades.

Probabilistic structures currently use exact persisted state to satisfy the upstream API without external PostgreSQL extensions: Bloom filters track exact membership, Cuckoo filters track exact per-item counts, HyperLogLog stores an exact distinct set, TDigest stores exact values, and TopK stores exact counts with deterministic top-k ordering.

## Build and test

```powershell
dotnet restore .\Dredis.Extensions.Storage.Postgres.slnx
dotnet build .\Dredis.Extensions.Storage.Postgres.slnx -c Release
dotnet test .\Dredis.Extensions.Storage.Postgres.slnx -c Release
```

The test project includes PostgreSQL integration coverage for the string, hash, list, set, sorted-set, JSON, stream, vector, and probabilistic slices. Set `DREDIS_POSTGRES_TEST_CONNECTION_STRING` to run those tests against a real PostgreSQL instance; otherwise the integration tests return early and the rest of the suite still runs.
