using System.Text;
using System.Text.Json;
using Dredis.Abstractions.Storage;
using Npgsql;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore
{
    private const double DefaultBloomErrorRate = 0.01d;
    private const long DefaultBloomCapacity = 1000;
    private const long DefaultCuckooCapacity = 1024;

    public async Task<HyperLogLogAddResult> HyperLogLogAddAsync(string key, byte[][] elements, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(elements);
        foreach (var element in elements)
        {
            ArgumentNullException.ThrowIfNull(element);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var loaded = await LoadProbabilisticStateAsync<HyperLogLogState>(connection, key, HyperLogLogKind, transaction, token).ConfigureAwait(false);
        if (loaded.Status == ProbabilisticResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new HyperLogLogAddResult(HyperLogLogResultStatus.WrongType, changed: false);
        }

        var state = loaded.Status == ProbabilisticResultStatus.NotFound ? new HyperLogLogState() : loaded.State!;
        var changed = false;
        foreach (var element in elements)
        {
            changed |= state.Elements.Add(EncodeBinary(element));
        }

        if (changed || loaded.Status == ProbabilisticResultStatus.NotFound)
        {
            await SaveProbabilisticStateAsync(connection, key, HyperLogLogKind, state, loaded.ExpiresAt, transaction, token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new HyperLogLogAddResult(HyperLogLogResultStatus.Ok, changed);
    }

    public async Task<HyperLogLogCountResult> HyperLogLogCountAsync(string[] keys, CancellationToken token = default)
    {
        ValidateKeys(keys);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var elements = new HashSet<string>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            var loaded = await LoadProbabilisticStateAsync<HyperLogLogState>(connection, key, HyperLogLogKind, transaction: null, token).ConfigureAwait(false);
            if (loaded.Status == ProbabilisticResultStatus.WrongType)
            {
                return new HyperLogLogCountResult(HyperLogLogResultStatus.WrongType, 0);
            }

            if (loaded.Status != ProbabilisticResultStatus.Ok)
            {
                continue;
            }

            elements.UnionWith(loaded.State!.Elements);
        }

        return new HyperLogLogCountResult(HyperLogLogResultStatus.Ok, elements.Count);
    }

    public async Task<HyperLogLogMergeResult> HyperLogLogMergeAsync(
        string destinationKey,
        string[] sourceKeys,
        CancellationToken token = default)
    {
        ValidateKey(destinationKey);
        ValidateKeys(sourceKeys);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var destination = await LoadProbabilisticStateAsync<HyperLogLogState>(connection, destinationKey, HyperLogLogKind, transaction, token).ConfigureAwait(false);
        if (destination.Status == ProbabilisticResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new HyperLogLogMergeResult(HyperLogLogResultStatus.WrongType);
        }

        var merged = destination.Status == ProbabilisticResultStatus.Ok ? destination.State! : new HyperLogLogState();
        foreach (var sourceKey in sourceKeys)
        {
            var source = await LoadProbabilisticStateAsync<HyperLogLogState>(connection, sourceKey, HyperLogLogKind, transaction, token).ConfigureAwait(false);
            if (source.Status == ProbabilisticResultStatus.WrongType)
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return new HyperLogLogMergeResult(HyperLogLogResultStatus.WrongType);
            }

            if (source.Status == ProbabilisticResultStatus.Ok)
            {
                merged.Elements.UnionWith(source.State!.Elements);
            }
        }

        await SaveProbabilisticStateAsync(connection, destinationKey, HyperLogLogKind, merged, destination.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new HyperLogLogMergeResult(HyperLogLogResultStatus.Ok);
    }

    public async Task<ProbabilisticResultStatus> BloomReserveAsync(
        string key,
        double errorRate,
        long capacity,
        CancellationToken token = default)
    {
        ValidateKey(key);
        if (errorRate <= 0 || errorRate >= 1 || capacity <= 0)
        {
            return ProbabilisticResultStatus.InvalidArgument;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return existing.Kind == BloomKind ? ProbabilisticResultStatus.Exists : ProbabilisticResultStatus.WrongType;
        }

        await SaveProbabilisticStateAsync(
            connection,
            key,
            BloomKind,
            new BloomState(errorRate, capacity),
            expiresAt: null,
            transaction,
            token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return ProbabilisticResultStatus.Ok;
    }

    public async Task<ProbabilisticBoolResult> BloomAddAsync(string key, byte[] element, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(element);
        return await MutateBloomAsync(
            key,
            token,
            static (state, encoded) => new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, state.Elements.Add(encoded)),
            element).ConfigureAwait(false);
    }

    public async Task<ProbabilisticArrayResult> BloomMAddAsync(string key, byte[][] elements, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(elements);
        foreach (var element in elements)
        {
            ArgumentNullException.ThrowIfNull(element);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<BloomState>(connection, key, BloomKind, transaction, token).ConfigureAwait(false);
        if (loaded.Status == ProbabilisticResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, []);
        }

        var state = loaded.Status == ProbabilisticResultStatus.NotFound ? new BloomState(DefaultBloomErrorRate, DefaultBloomCapacity) : loaded.State!;
        var values = new long[elements.Length];
        for (var i = 0; i < elements.Length; i++)
        {
            values[i] = state.Elements.Add(EncodeBinary(elements[i])) ? 1 : 0;
        }

        await SaveProbabilisticStateAsync(connection, key, BloomKind, state, loaded.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, values);
    }

    public async Task<ProbabilisticBoolResult> BloomExistsAsync(string key, byte[] element, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(element);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<BloomState>(connection, key, BloomKind, transaction: null, token).ConfigureAwait(false);
        return loaded.Status switch
        {
            ProbabilisticResultStatus.Ok => new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, loaded.State!.Elements.Contains(EncodeBinary(element))),
            ProbabilisticResultStatus.WrongType => new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false),
            _ => new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false)
        };
    }

    public async Task<ProbabilisticArrayResult> BloomMExistsAsync(string key, byte[][] elements, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(elements);
        foreach (var element in elements)
        {
            ArgumentNullException.ThrowIfNull(element);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<BloomState>(connection, key, BloomKind, transaction: null, token).ConfigureAwait(false);
        if (loaded.Status == ProbabilisticResultStatus.WrongType)
        {
            return new ProbabilisticArrayResult(ProbabilisticResultStatus.WrongType, []);
        }

        var values = new long[elements.Length];
        if (loaded.Status == ProbabilisticResultStatus.Ok)
        {
            for (var i = 0; i < elements.Length; i++)
            {
                values[i] = loaded.State!.Elements.Contains(EncodeBinary(elements[i])) ? 1 : 0;
            }
        }

        return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, values);
    }

    public async Task<ProbabilisticInfoResult> BloomInfoAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<BloomState>(connection, key, BloomKind, transaction: null, token).ConfigureAwait(false);
        return loaded.Status switch
        {
            ProbabilisticResultStatus.Ok => new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, CreateBloomInfoFields(loaded.State!)),
            ProbabilisticResultStatus.WrongType => new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, []),
            _ => new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, [])
        };
    }

    public async Task<ProbabilisticResultStatus> CuckooReserveAsync(string key, long capacity, CancellationToken token = default)
    {
        ValidateKey(key);
        if (capacity <= 0)
        {
            return ProbabilisticResultStatus.InvalidArgument;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return existing.Kind == CuckooKind ? ProbabilisticResultStatus.Exists : ProbabilisticResultStatus.WrongType;
        }

        await SaveProbabilisticStateAsync(connection, key, CuckooKind, new CuckooState(capacity), expiresAt: null, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return ProbabilisticResultStatus.Ok;
    }

    public async Task<ProbabilisticBoolResult> CuckooAddAsync(string key, byte[] item, bool noCreate, CancellationToken token = default)
    {
        return await AddToCuckooAsync(key, item, noCreate, onlyIfMissing: false, token).ConfigureAwait(false);
    }

    public async Task<ProbabilisticBoolResult> CuckooAddNxAsync(string key, byte[] item, bool noCreate, CancellationToken token = default)
    {
        return await AddToCuckooAsync(key, item, noCreate, onlyIfMissing: true, token).ConfigureAwait(false);
    }

    public async Task<ProbabilisticBoolResult> CuckooExistsAsync(string key, byte[] item, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(item);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<CuckooState>(connection, key, CuckooKind, transaction: null, token).ConfigureAwait(false);
        return loaded.Status switch
        {
            ProbabilisticResultStatus.Ok => new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, loaded.State!.Counts.TryGetValue(EncodeBinary(item), out var count) && count > 0),
            ProbabilisticResultStatus.WrongType => new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false),
            _ => new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false)
        };
    }

    public async Task<ProbabilisticBoolResult> CuckooDeleteAsync(string key, byte[] item, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(item);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<CuckooState>(connection, key, CuckooKind, transaction, token).ConfigureAwait(false);
        if (loaded.Status == ProbabilisticResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false);
        }

        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, false);
        }

        var encoded = EncodeBinary(item);
        var removed = false;
        if (loaded.State!.Counts.TryGetValue(encoded, out var count) && count > 0)
        {
            removed = true;
            if (count == 1)
            {
                loaded.State.Counts.Remove(encoded);
            }
            else
            {
                loaded.State.Counts[encoded] = count - 1;
            }

            await SaveProbabilisticStateAsync(connection, key, CuckooKind, loaded.State, loaded.ExpiresAt, transaction, token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, removed);
    }

    public async Task<ProbabilisticCountResult> CuckooCountAsync(string key, byte[] item, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(item);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<CuckooState>(connection, key, CuckooKind, transaction: null, token).ConfigureAwait(false);
        return loaded.Status switch
        {
            ProbabilisticResultStatus.Ok => new ProbabilisticCountResult(ProbabilisticResultStatus.Ok, loaded.State!.Counts.GetValueOrDefault(EncodeBinary(item), 0)),
            ProbabilisticResultStatus.WrongType => new ProbabilisticCountResult(ProbabilisticResultStatus.WrongType, 0),
            _ => new ProbabilisticCountResult(ProbabilisticResultStatus.Ok, 0)
        };
    }

    public async Task<ProbabilisticInfoResult> CuckooInfoAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<CuckooState>(connection, key, CuckooKind, transaction: null, token).ConfigureAwait(false);
        return loaded.Status switch
        {
            ProbabilisticResultStatus.Ok => new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, CreateCuckooInfoFields(loaded.State!)),
            ProbabilisticResultStatus.WrongType => new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, []),
            _ => new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, [])
        };
    }

    public async Task<ProbabilisticResultStatus> TDigestCreateAsync(string key, int compression, CancellationToken token = default)
    {
        ValidateKey(key);
        if (compression <= 0)
        {
            return ProbabilisticResultStatus.InvalidArgument;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return existing.Kind == TDigestKind ? ProbabilisticResultStatus.Exists : ProbabilisticResultStatus.WrongType;
        }

        await SaveProbabilisticStateAsync(connection, key, TDigestKind, new TDigestState(compression), expiresAt: null, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return ProbabilisticResultStatus.Ok;
    }

    public async Task<ProbabilisticResultStatus> TDigestResetAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<TDigestState>(connection, key, TDigestKind, transaction, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return loaded.Status;
        }

        loaded.State!.Values.Clear();
        await SaveProbabilisticStateAsync(connection, key, TDigestKind, loaded.State, loaded.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return ProbabilisticResultStatus.Ok;
    }

    public async Task<ProbabilisticResultStatus> TDigestAddAsync(string key, double[] values, CancellationToken token = default)
    {
        ValidateKey(key);
        if (!IsValidProbabilisticValues(values))
        {
            return ProbabilisticResultStatus.InvalidArgument;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<TDigestState>(connection, key, TDigestKind, transaction, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return loaded.Status;
        }

        loaded.State!.Values.AddRange(values);
        await SaveProbabilisticStateAsync(connection, key, TDigestKind, loaded.State, loaded.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return ProbabilisticResultStatus.Ok;
    }

    public async Task<ProbabilisticDoubleArrayResult> TDigestQuantileAsync(string key, double[] quantiles, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(quantiles);

        var loaded = await LoadTDigestAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticDoubleArrayResult(loaded.Status, []);
        }

        var sorted = loaded.State!.Values.OrderBy(static value => value).ToArray();
        var values = new double[quantiles.Length];
        for (var i = 0; i < quantiles.Length; i++)
        {
            values[i] = TryComputeQuantile(sorted, quantiles[i], out var quantile) ? quantile : double.NaN;
        }

        return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, values);
    }

    public async Task<ProbabilisticDoubleArrayResult> TDigestCdfAsync(string key, double[] values, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(values);

        var loaded = await LoadTDigestAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticDoubleArrayResult(loaded.Status, []);
        }

        var sorted = loaded.State!.Values.OrderBy(static value => value).ToArray();
        var result = new double[values.Length];
        if (sorted.Length == 0)
        {
            return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, result);
        }

        for (var i = 0; i < values.Length; i++)
        {
            var count = sorted.LongCount(candidate => candidate <= values[i]);
            result[i] = (double)count / sorted.Length;
        }

        return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, result);
    }

    public async Task<ProbabilisticArrayResult> TDigestRankAsync(string key, double[] values, CancellationToken token = default)
    {
        return await GetTDigestRanksAsync(key, values, reverse: false, token).ConfigureAwait(false);
    }

    public async Task<ProbabilisticArrayResult> TDigestRevRankAsync(string key, double[] values, CancellationToken token = default)
    {
        return await GetTDigestRanksAsync(key, values, reverse: true, token).ConfigureAwait(false);
    }

    public async Task<ProbabilisticDoubleArrayResult> TDigestByRankAsync(string key, long[] ranks, CancellationToken token = default)
    {
        return await GetTDigestValuesByRankAsync(key, ranks, reverse: false, token).ConfigureAwait(false);
    }

    public async Task<ProbabilisticDoubleArrayResult> TDigestByRevRankAsync(string key, long[] ranks, CancellationToken token = default)
    {
        return await GetTDigestValuesByRankAsync(key, ranks, reverse: true, token).ConfigureAwait(false);
    }

    public async Task<ProbabilisticDoubleResult> TDigestTrimmedMeanAsync(
        string key,
        double lowerQuantile,
        double upperQuantile,
        CancellationToken token = default)
    {
        ValidateKey(key);
        if (lowerQuantile < 0 || lowerQuantile > 1 || upperQuantile < 0 || upperQuantile > 1 || lowerQuantile > upperQuantile)
        {
            return new ProbabilisticDoubleResult(ProbabilisticResultStatus.InvalidArgument, value: null);
        }

        var loaded = await LoadTDigestAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticDoubleResult(loaded.Status, value: null);
        }

        var sorted = loaded.State!.Values.OrderBy(static value => value).ToArray();
        if (sorted.Length == 0)
        {
            return new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, value: null);
        }

        var startIndex = (int)Math.Floor(lowerQuantile * (sorted.Length - 1));
        var endIndex = (int)Math.Floor(upperQuantile * (sorted.Length - 1));
        var slice = sorted.Skip(startIndex).Take(endIndex - startIndex + 1).ToArray();
        return new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, slice.Average());
    }

    public async Task<ProbabilisticDoubleResult> TDigestMinAsync(string key, CancellationToken token = default)
    {
        return await GetTDigestExtremaAsync(key, minimum: true, token).ConfigureAwait(false);
    }

    public async Task<ProbabilisticDoubleResult> TDigestMaxAsync(string key, CancellationToken token = default)
    {
        return await GetTDigestExtremaAsync(key, minimum: false, token).ConfigureAwait(false);
    }

    public async Task<ProbabilisticInfoResult> TDigestInfoAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);
        var loaded = await LoadTDigestAsync(key, token).ConfigureAwait(false);
        return loaded.Status switch
        {
            ProbabilisticResultStatus.Ok => new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, CreateTDigestInfoFields(loaded.State!)),
            ProbabilisticResultStatus.WrongType => new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, []),
            _ => new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, [])
        };
    }

    public async Task<ProbabilisticResultStatus> TopKReserveAsync(
        string key,
        int k,
        int width,
        int depth,
        double decay,
        CancellationToken token = default)
    {
        ValidateKey(key);
        if (k <= 0 || width <= 0 || depth <= 0 || decay <= 0 || decay >= 1)
        {
            return ProbabilisticResultStatus.InvalidArgument;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return existing.Kind == TopKKind ? ProbabilisticResultStatus.Exists : ProbabilisticResultStatus.WrongType;
        }

        await SaveProbabilisticStateAsync(connection, key, TopKKind, new TopKState(k, width, depth, decay), expiresAt: null, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return ProbabilisticResultStatus.Ok;
    }

    public async Task<ProbabilisticStringArrayResult> TopKAddAsync(string key, byte[][] items, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
        {
            ArgumentNullException.ThrowIfNull(item);
        }

        return await MutateTopKAsync(
            key,
            token,
            (state) =>
            {
                var dropped = new string?[items.Length];
                for (var i = 0; i < items.Length; i++)
                {
                    dropped[i] = ApplyTopKChange(state, EncodeBinary(items[i]), 1);
                }

                return new ProbabilisticStringArrayResult(ProbabilisticResultStatus.Ok, dropped);
            }).ConfigureAwait(false);
    }

    public async Task<ProbabilisticStringArrayResult> TopKIncrByAsync(
        string key,
        KeyValuePair<byte[], long>[] increments,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(increments);
        foreach (var increment in increments)
        {
            ArgumentNullException.ThrowIfNull(increment.Key);
        }

        return await MutateTopKAsync(
            key,
            token,
            (state) =>
            {
                var dropped = new string?[increments.Length];
                for (var i = 0; i < increments.Length; i++)
                {
                    dropped[i] = ApplyTopKChange(state, EncodeBinary(increments[i].Key), increments[i].Value);
                }

                return new ProbabilisticStringArrayResult(ProbabilisticResultStatus.Ok, dropped);
            }).ConfigureAwait(false);
    }

    public async Task<ProbabilisticArrayResult> TopKQueryAsync(string key, byte[][] items, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
        {
            ArgumentNullException.ThrowIfNull(item);
        }

        var loaded = await LoadTopKAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticArrayResult(loaded.Status, []);
        }

        var top = ComputeTopKItems(loaded.State!);
        var values = items.Select(item => top.Contains(EncodeBinary(item), StringComparer.Ordinal) ? 1L : 0L).ToArray();
        return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, values);
    }

    public async Task<ProbabilisticArrayResult> TopKCountAsync(string key, byte[][] items, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(items);
        foreach (var item in items)
        {
            ArgumentNullException.ThrowIfNull(item);
        }

        var loaded = await LoadTopKAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticArrayResult(loaded.Status, []);
        }

        var values = items.Select(item => loaded.State!.Counts.GetValueOrDefault(EncodeBinary(item), 0)).ToArray();
        return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, values);
    }

    public async Task<ProbabilisticStringArrayResult> TopKListAsync(string key, bool withCount, CancellationToken token = default)
    {
        ValidateKey(key);
        var loaded = await LoadTopKAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticStringArrayResult(loaded.Status, []);
        }

        var ordered = GetOrderedTopKEntries(loaded.State!);
        if (!withCount)
        {
            return new ProbabilisticStringArrayResult(ProbabilisticResultStatus.Ok, ordered.Select(static entry => DecodeBinaryToString(entry.Key)).ToArray());
        }

        var values = new List<string?>(ordered.Count * 2);
        foreach (var entry in ordered)
        {
            values.Add(DecodeBinaryToString(entry.Key));
            values.Add(entry.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        return new ProbabilisticStringArrayResult(ProbabilisticResultStatus.Ok, [.. values]);
    }

    public async Task<ProbabilisticInfoResult> TopKInfoAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);
        var loaded = await LoadTopKAsync(key, token).ConfigureAwait(false);
        return loaded.Status switch
        {
            ProbabilisticResultStatus.Ok => new ProbabilisticInfoResult(ProbabilisticResultStatus.Ok, CreateTopKInfoFields(loaded.State!)),
            ProbabilisticResultStatus.WrongType => new ProbabilisticInfoResult(ProbabilisticResultStatus.WrongType, []),
            _ => new ProbabilisticInfoResult(ProbabilisticResultStatus.NotFound, [])
        };
    }

    private async Task<ProbabilisticBoolResult> MutateBloomAsync(
        string key,
        CancellationToken token,
        Func<BloomState, string, ProbabilisticBoolResult> mutate,
        byte[] element)
    {
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<BloomState>(connection, key, BloomKind, transaction, token).ConfigureAwait(false);
        if (loaded.Status == ProbabilisticResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false);
        }

        var state = loaded.Status == ProbabilisticResultStatus.NotFound ? new BloomState(DefaultBloomErrorRate, DefaultBloomCapacity) : loaded.State!;
        var result = mutate(state, EncodeBinary(element));
        await SaveProbabilisticStateAsync(connection, key, BloomKind, state, loaded.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return result;
    }

    private async Task<ProbabilisticBoolResult> AddToCuckooAsync(
        string key,
        byte[] item,
        bool noCreate,
        bool onlyIfMissing,
        CancellationToken token)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(item);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<CuckooState>(connection, key, CuckooKind, transaction, token).ConfigureAwait(false);
        if (loaded.Status == ProbabilisticResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ProbabilisticBoolResult(ProbabilisticResultStatus.WrongType, false);
        }

        if (loaded.Status == ProbabilisticResultStatus.NotFound && noCreate)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ProbabilisticBoolResult(ProbabilisticResultStatus.NotFound, false);
        }

        var state = loaded.Status == ProbabilisticResultStatus.NotFound ? new CuckooState(DefaultCuckooCapacity) : loaded.State!;
        var encoded = EncodeBinary(item);
        var changed = false;
        if (!state.Counts.TryGetValue(encoded, out var currentCount))
        {
            state.Counts[encoded] = 1;
            changed = true;
        }
        else if (!onlyIfMissing)
        {
            state.Counts[encoded] = currentCount + 1;
            changed = true;
        }

        await SaveProbabilisticStateAsync(connection, key, CuckooKind, state, loaded.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new ProbabilisticBoolResult(ProbabilisticResultStatus.Ok, changed);
    }

    private async Task<(ProbabilisticResultStatus Status, TDigestState? State, DateTimeOffset? ExpiresAt)> LoadTDigestAsync(string key, CancellationToken token)
    {
        ValidateKey(key);
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        return await LoadProbabilisticStateAsync<TDigestState>(connection, key, TDigestKind, transaction: null, token).ConfigureAwait(false);
    }

    private async Task<ProbabilisticArrayResult> GetTDigestRanksAsync(string key, double[] values, bool reverse, CancellationToken token)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(values);

        var loaded = await LoadTDigestAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticArrayResult(loaded.Status, []);
        }

        var sorted = loaded.State!.Values.OrderBy(static value => value).ToArray();
        var result = new long[values.Length];
        for (var i = 0; i < values.Length; i++)
        {
            result[i] = reverse
                ? sorted.LongCount(candidate => candidate > values[i])
                : sorted.LongCount(candidate => candidate < values[i]);
        }

        return new ProbabilisticArrayResult(ProbabilisticResultStatus.Ok, result);
    }

    private async Task<ProbabilisticDoubleArrayResult> GetTDigestValuesByRankAsync(
        string key,
        long[] ranks,
        bool reverse,
        CancellationToken token)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(ranks);

        var loaded = await LoadTDigestAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticDoubleArrayResult(loaded.Status, []);
        }

        var sorted = loaded.State!.Values.OrderBy(static value => value).ToArray();
        var result = new double[ranks.Length];
        for (var i = 0; i < ranks.Length; i++)
        {
            var index = reverse ? sorted.Length - 1 - ranks[i] : ranks[i];
            result[i] = index < 0 || index >= sorted.Length ? double.NaN : sorted[index];
        }

        return new ProbabilisticDoubleArrayResult(ProbabilisticResultStatus.Ok, result);
    }

    private async Task<ProbabilisticDoubleResult> GetTDigestExtremaAsync(string key, bool minimum, CancellationToken token)
    {
        ValidateKey(key);
        var loaded = await LoadTDigestAsync(key, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            return new ProbabilisticDoubleResult(loaded.Status, value: null);
        }

        if (loaded.State!.Values.Count == 0)
        {
            return new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, value: null);
        }

        return new ProbabilisticDoubleResult(ProbabilisticResultStatus.Ok, minimum ? loaded.State.Values.Min() : loaded.State.Values.Max());
    }

    private async Task<(ProbabilisticResultStatus Status, TopKState? State, DateTimeOffset? ExpiresAt)> LoadTopKAsync(string key, CancellationToken token)
    {
        ValidateKey(key);
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        return await LoadProbabilisticStateAsync<TopKState>(connection, key, TopKKind, transaction: null, token).ConfigureAwait(false);
    }

    private async Task<ProbabilisticStringArrayResult> MutateTopKAsync(
        string key,
        CancellationToken token,
        Func<TopKState, ProbabilisticStringArrayResult> mutate)
    {
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadProbabilisticStateAsync<TopKState>(connection, key, TopKKind, transaction, token).ConfigureAwait(false);
        if (loaded.Status != ProbabilisticResultStatus.Ok)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ProbabilisticStringArrayResult(loaded.Status, []);
        }

        var result = mutate(loaded.State!);
        await SaveProbabilisticStateAsync(connection, key, TopKKind, loaded.State!, loaded.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return result;
    }

    private string? ApplyTopKChange(TopKState state, string encodedItem, long delta)
    {
        var before = GetOrderedTopKEntries(state);

        var newValue = state.Counts.GetValueOrDefault(encodedItem, 0) + delta;
        if (newValue <= 0)
        {
            state.Counts.Remove(encodedItem);
        }
        else
        {
            state.Counts[encodedItem] = newValue;
        }

        var after = GetOrderedTopKEntries(state);
        var afterKeys = after.Select(static entry => entry.Key).ToHashSet(StringComparer.Ordinal);
        if (!afterKeys.Contains(encodedItem))
        {
            return null;
        }

        var dropped = before
            .Select(static entry => entry.Key)
            .FirstOrDefault(previousKey => !afterKeys.Contains(previousKey) && !string.Equals(previousKey, encodedItem, StringComparison.Ordinal));
        return dropped is null ? null : DecodeBinaryToString(dropped);
    }

    private List<KeyValuePair<string, long>> GetOrderedTopKEntries(TopKState state) =>
        state.Counts
            .Where(static entry => entry.Value > 0)
            .OrderByDescending(static entry => entry.Value)
            .ThenBy(static entry => entry.Key, StringComparer.Ordinal)
            .Take(state.K)
            .ToList();

    private HashSet<string> ComputeTopKItems(TopKState state) =>
        GetOrderedTopKEntries(state).Select(static entry => entry.Key).ToHashSet(StringComparer.Ordinal);

    private async Task<(ProbabilisticResultStatus Status, TState? State, DateTimeOffset? ExpiresAt)> LoadProbabilisticStateAsync<TState>(
        NpgsqlConnection connection,
        string key,
        string kind,
        NpgsqlTransaction? transaction,
        CancellationToken token)
        where TState : class
    {
        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return (ProbabilisticResultStatus.NotFound, null, null);
        }

        if (existing.Kind != kind)
        {
            return (ProbabilisticResultStatus.WrongType, null, existing.ExpiresAt);
        }

        if (!TryDeserializeProbabilisticState(existing.Value, out TState? state))
        {
            return (ProbabilisticResultStatus.InvalidArgument, null, existing.ExpiresAt);
        }

        return (ProbabilisticResultStatus.Ok, state, existing.ExpiresAt);
    }

    private async Task SaveProbabilisticStateAsync<TState>(
        NpgsqlConnection connection,
        string key,
        string kind,
        TState state,
        DateTimeOffset? expiresAt,
        NpgsqlTransaction transaction,
        CancellationToken token)
        where TState : class
    {
        await UpsertKeyMetadataAsync(
            connection,
            key,
            kind,
            JsonSerializer.SerializeToUtf8Bytes(state),
            expiresAt,
            transaction,
            token).ConfigureAwait(false);
    }

    private static bool TryDeserializeProbabilisticState<TState>(byte[]? bytes, out TState? state)
        where TState : class
    {
        state = null;
        if (bytes is null)
        {
            return false;
        }

        try
        {
            state = JsonSerializer.Deserialize<TState>(bytes);
            return state is not null;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool IsValidProbabilisticValues(double[]? values)
    {
        if (values is null || values.Length == 0)
        {
            return false;
        }

        foreach (var value in values)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryComputeQuantile(double[] sorted, double quantile, out double value)
    {
        value = double.NaN;
        if (quantile < 0 || quantile > 1)
        {
            return false;
        }

        if (sorted.Length == 0)
        {
            value = double.NaN;
            return true;
        }

        var index = quantile * (sorted.Length - 1);
        var lower = (int)Math.Floor(index);
        var upper = (int)Math.Ceiling(index);
        if (lower == upper)
        {
            value = sorted[lower];
            return true;
        }

        var fraction = index - lower;
        value = sorted[lower] + (sorted[upper] - sorted[lower]) * fraction;
        return true;
    }

    private static KeyValuePair<string, string>[] CreateBloomInfoFields(BloomState state) =>
    [
        new("Capacity", state.Capacity.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new("Size", state.Elements.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new("Number of filters", "1"),
        new("Number of items inserted", state.Elements.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new("Expansion rate", "1")
    ];

    private static KeyValuePair<string, string>[] CreateCuckooInfoFields(CuckooState state)
    {
        var total = state.Counts.Values.Sum();
        return
        [
            new("Capacity", state.Capacity.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            new("Size", total.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            new("Number of buckets", state.Capacity.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            new("Number of items inserted", total.ToString(System.Globalization.CultureInfo.InvariantCulture))
        ];
    }

    private static KeyValuePair<string, string>[] CreateTDigestInfoFields(TDigestState state)
    {
        var min = state.Values.Count == 0 ? 0d : state.Values.Min();
        var max = state.Values.Count == 0 ? 0d : state.Values.Max();
        return
        [
            new("Compression", state.Compression.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            new("Observations", state.Values.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            new("Merged nodes", state.Values.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            new("Unmerged nodes", "0"),
            new("Min", min.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)),
            new("Max", max.ToString("G17", System.Globalization.CultureInfo.InvariantCulture))
        ];
    }

    private static KeyValuePair<string, string>[] CreateTopKInfoFields(TopKState state) =>
    [
        new("k", state.K.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new("width", state.Width.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new("depth", state.Depth.ToString(System.Globalization.CultureInfo.InvariantCulture)),
        new("decay", state.Decay.ToString("G17", System.Globalization.CultureInfo.InvariantCulture)),
        new("count", state.Counts.Values.Sum().ToString(System.Globalization.CultureInfo.InvariantCulture))
    ];

    private static string EncodeBinary(byte[] value) => Convert.ToBase64String(value);

    private static string DecodeBinaryToString(string value) => Encoding.UTF8.GetString(Convert.FromBase64String(value));

    private sealed class BloomState
    {
        public BloomState()
        {
        }

        public BloomState(double errorRate, long capacity)
        {
            ErrorRate = errorRate;
            Capacity = capacity;
        }

        public double ErrorRate { get; set; }

        public long Capacity { get; set; }

        public HashSet<string> Elements { get; set; } = new(StringComparer.Ordinal);
    }

    private sealed class CuckooState
    {
        public CuckooState()
        {
        }

        public CuckooState(long capacity)
        {
            Capacity = capacity;
        }

        public long Capacity { get; set; }

        public Dictionary<string, long> Counts { get; set; } = new(StringComparer.Ordinal);
    }

    private sealed class HyperLogLogState
    {
        public HashSet<string> Elements { get; set; } = new(StringComparer.Ordinal);
    }

    private sealed class TDigestState
    {
        public TDigestState()
        {
        }

        public TDigestState(int compression)
        {
            Compression = compression;
        }

        public int Compression { get; set; }

        public List<double> Values { get; set; } = [];
    }

    private sealed class TopKState
    {
        public TopKState()
        {
        }

        public TopKState(int k, int width, int depth, double decay)
        {
            K = k;
            Width = width;
            Depth = depth;
            Decay = decay;
        }

        public int K { get; set; }

        public int Width { get; set; }

        public int Depth { get; set; }

        public double Decay { get; set; }

        public Dictionary<string, long> Counts { get; set; } = new(StringComparer.Ordinal);
    }
}
