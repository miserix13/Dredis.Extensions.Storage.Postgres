using System.Text.Json;
using Dredis.Abstractions.Storage;
using Npgsql;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore
{
    public async Task<TimeSeriesAddResult> TimeSeriesAddAsync(
        string key,
        long timestamp,
        double value,
        TimeSeriesDuplicatePolicy? onDuplicate,
        bool createIfMissing,
        CancellationToken token = default)
    {
        ValidateKey(key);
        if (!IsValidTimeSeriesValue(value))
        {
            return new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, timestamp: null);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadTimeSeriesStateAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (loaded.Status == TimeSeriesResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesAddResult(TimeSeriesResultStatus.WrongType, timestamp: null);
        }

        if (loaded.Status == TimeSeriesResultStatus.InvalidArgument)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, timestamp: null);
        }

        var state = loaded.Status == TimeSeriesResultStatus.NotFound
            ? CreateDefaultTimeSeriesState()
            : loaded.State!;
        if (loaded.Status == TimeSeriesResultStatus.NotFound)
        {
            if (!createIfMissing)
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return new TimeSeriesAddResult(TimeSeriesResultStatus.NotFound, timestamp: null);
            }

            await SaveTimeSeriesStateAsync(connection, key, state, expiresAt: null, transaction, token).ConfigureAwait(false);
        }

        var latest = await GetLatestTimeSeriesSampleAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (latest is not null && IsOutsideTimeSeriesRetention(timestamp, latest.Timestamp, state.RetentionTimeMs))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, timestamp: null);
        }

        var existingSample = await GetTimeSeriesSampleAsync(connection, key, timestamp, transaction, token).ConfigureAwait(false);
        if (existingSample is not null)
        {
            var policy = onDuplicate ?? state.DuplicatePolicy;
            switch (policy)
            {
                case TimeSeriesDuplicatePolicy.Block:
                    await transaction.RollbackAsync(token).ConfigureAwait(false);
                    return new TimeSeriesAddResult(TimeSeriesResultStatus.Exists, timestamp: null);
                case TimeSeriesDuplicatePolicy.First:
                    break;
                case TimeSeriesDuplicatePolicy.Last:
                    await UpsertTimeSeriesSampleAsync(connection, key, timestamp, value, transaction, token).ConfigureAwait(false);
                    break;
                case TimeSeriesDuplicatePolicy.Min:
                    await UpsertTimeSeriesSampleAsync(connection, key, timestamp, Math.Min(existingSample.Value, value), transaction, token).ConfigureAwait(false);
                    break;
                case TimeSeriesDuplicatePolicy.Max:
                    await UpsertTimeSeriesSampleAsync(connection, key, timestamp, Math.Max(existingSample.Value, value), transaction, token).ConfigureAwait(false);
                    break;
                case TimeSeriesDuplicatePolicy.Sum:
                    await UpsertTimeSeriesSampleAsync(connection, key, timestamp, existingSample.Value + value, transaction, token).ConfigureAwait(false);
                    break;
                default:
                    await transaction.RollbackAsync(token).ConfigureAwait(false);
                    return new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, timestamp: null);
            }
        }
        else
        {
            await UpsertTimeSeriesSampleAsync(connection, key, timestamp, value, transaction, token).ConfigureAwait(false);
        }

        var maxTimestamp = latest is null ? timestamp : Math.Max(latest.Timestamp, timestamp);
        await TrimRetainedTimeSeriesSamplesAsync(connection, key, maxTimestamp, state.RetentionTimeMs, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new TimeSeriesAddResult(TimeSeriesResultStatus.Ok, timestamp);
    }

    public async Task<TimeSeriesResultStatus> TimeSeriesCreateAsync(
        string key,
        long? retentionTimeMs,
        TimeSeriesDuplicatePolicy? duplicatePolicy,
        KeyValuePair<string, string>[]? labels,
        CancellationToken token = default)
    {
        ValidateKey(key);
        if (retentionTimeMs is < 0)
        {
            return TimeSeriesResultStatus.InvalidArgument;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return existing.Kind == TimeSeriesKind ? TimeSeriesResultStatus.Exists : TimeSeriesResultStatus.WrongType;
        }

        var state = new TimeSeriesState
        {
            RetentionTimeMs = retentionTimeMs ?? 0,
            DuplicatePolicy = duplicatePolicy ?? TimeSeriesDuplicatePolicy.Block,
            Labels = labels ?? []
        };

        await SaveTimeSeriesStateAsync(connection, key, state, expiresAt: null, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return TimeSeriesResultStatus.Ok;
    }

    public async Task<TimeSeriesDeleteResult> TimeSeriesDeleteAsync(
        string key,
        long fromTimestamp,
        long toTimestamp,
        CancellationToken token = default)
    {
        ValidateKey(key);
        if (fromTimestamp > toTimestamp)
        {
            return new TimeSeriesDeleteResult(TimeSeriesResultStatus.InvalidArgument, deleted: 0);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadTimeSeriesStateAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (loaded.Status == TimeSeriesResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesDeleteResult(TimeSeriesResultStatus.WrongType, deleted: 0);
        }

        if (loaded.Status == TimeSeriesResultStatus.InvalidArgument)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesDeleteResult(TimeSeriesResultStatus.InvalidArgument, deleted: 0);
        }

        if (loaded.Status == TimeSeriesResultStatus.NotFound)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesDeleteResult(TimeSeriesResultStatus.NotFound, deleted: 0);
        }

        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedTimeSeriesSampleTableName}
            WHERE key = @key
              AND timestamp_ms >= @fromTimestamp
              AND timestamp_ms <= @toTimestamp;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("fromTimestamp", fromTimestamp);
        command.Parameters.AddWithValue("toTimestamp", toTimestamp);
        var deleted = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new TimeSeriesDeleteResult(TimeSeriesResultStatus.Ok, deleted);
    }

    public async Task<TimeSeriesGetResult> TimeSeriesGetAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadTimeSeriesStateAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (loaded.Status != TimeSeriesResultStatus.Ok)
        {
            return new TimeSeriesGetResult(loaded.Status, sample: null);
        }

        var latest = await GetLatestTimeSeriesSampleAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        return latest is null
            ? new TimeSeriesGetResult(TimeSeriesResultStatus.Ok, sample: null)
            : new TimeSeriesGetResult(TimeSeriesResultStatus.Ok, latest);
    }

    public async Task<TimeSeriesAddResult> TimeSeriesIncrementByAsync(
        string key,
        double increment,
        long? timestamp,
        bool createIfMissing,
        CancellationToken token = default)
    {
        ValidateKey(key);
        if (!IsValidTimeSeriesValue(increment))
        {
            return new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, timestamp: null);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
        var loaded = await LoadTimeSeriesStateAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (loaded.Status == TimeSeriesResultStatus.WrongType)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesAddResult(TimeSeriesResultStatus.WrongType, timestamp: null);
        }

        if (loaded.Status == TimeSeriesResultStatus.InvalidArgument)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, timestamp: null);
        }

        var state = loaded.Status == TimeSeriesResultStatus.NotFound
            ? CreateDefaultTimeSeriesState()
            : loaded.State!;
        if (loaded.Status == TimeSeriesResultStatus.NotFound)
        {
            if (!createIfMissing)
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return new TimeSeriesAddResult(TimeSeriesResultStatus.NotFound, timestamp: null);
            }

            await SaveTimeSeriesStateAsync(connection, key, state, expiresAt: null, transaction, token).ConfigureAwait(false);
        }

        var latest = await GetLatestTimeSeriesSampleAsync(connection, key, transaction, token).ConfigureAwait(false);
        var effectiveTimestamp = timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (latest is not null && effectiveTimestamp < latest.Timestamp)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new TimeSeriesAddResult(TimeSeriesResultStatus.InvalidArgument, timestamp: null);
        }

        var resultingValue = latest is null ? increment : latest.Value + increment;
        await UpsertTimeSeriesSampleAsync(connection, key, effectiveTimestamp, resultingValue, transaction, token).ConfigureAwait(false);

        var maxTimestamp = latest is null ? effectiveTimestamp : Math.Max(latest.Timestamp, effectiveTimestamp);
        await TrimRetainedTimeSeriesSamplesAsync(connection, key, maxTimestamp, state.RetentionTimeMs, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new TimeSeriesAddResult(TimeSeriesResultStatus.Ok, effectiveTimestamp);
    }

    public async Task<TimeSeriesInfoResult> TimeSeriesInfoAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadTimeSeriesStateAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (loaded.Status != TimeSeriesResultStatus.Ok)
        {
            return new TimeSeriesInfoResult(
                loaded.Status,
                totalSamples: 0,
                firstTimestamp: null,
                lastTimestamp: null,
                retentionTimeMs: 0,
                duplicatePolicy: TimeSeriesDuplicatePolicy.Block,
                labels: []);
        }

        await using var command = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*), MIN(timestamp_ms), MAX(timestamp_ms)
            FROM {_qualifiedTimeSeriesSampleTableName}
            WHERE key = @key;
            """);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        await reader.ReadAsync(token).ConfigureAwait(false);

        var totalSamples = reader.GetInt64(0);
        var firstTimestamp = reader.IsDBNull(1) ? (long?)null : reader.GetInt64(1);
        var lastTimestamp = reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2);

        return new TimeSeriesInfoResult(
            TimeSeriesResultStatus.Ok,
            totalSamples,
            firstTimestamp,
            lastTimestamp,
            loaded.State!.RetentionTimeMs,
            loaded.State.DuplicatePolicy,
            loaded.State.Labels);
    }

    public async Task<TimeSeriesMRangeResult> TimeSeriesMultiRangeAsync(
        long fromTimestamp,
        long toTimestamp,
        bool reverse,
        int? count,
        string? aggregationType,
        long? bucketDurationMs,
        KeyValuePair<string, string>[] filters,
        CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(filters);
        if (!TryNormalizeTimeSeriesRangeArguments(fromTimestamp, toTimestamp, count, aggregationType, bucketDurationMs, out var normalizedAggregation) ||
            filters.Length == 0)
        {
            return new TimeSeriesMRangeResult(TimeSeriesResultStatus.InvalidArgument, []);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT key, value
            FROM {_qualifiedTableName}
            WHERE kind = @kind
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY key;
            """);

        command.Parameters.AddWithValue("kind", TimeSeriesKind);
        var series = new List<(string Key, TimeSeriesState State)>();
        await using (var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(token).ConfigureAwait(false))
            {
                var key = reader.GetString(0);
                var bytes = reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1);
                if (!TryDeserializeTimeSeriesState(bytes, out var state))
                {
                    return new TimeSeriesMRangeResult(TimeSeriesResultStatus.InvalidArgument, []);
                }

                if (MatchesTimeSeriesFilters(state!, filters))
                {
                    series.Add((key, state!));
                }
            }
        }

        var entries = new List<TimeSeriesMRangeEntry>();
        foreach (var seriesEntry in series)
        {
            var samples = await GetTimeSeriesSamplesInRangeAsync(connection, seriesEntry.Key, fromTimestamp, toTimestamp, token).ConfigureAwait(false);
            if (normalizedAggregation is not null)
            {
                samples = AggregateTimeSeriesSamples(samples, normalizedAggregation, bucketDurationMs!.Value);
            }

            if (reverse)
            {
                Array.Reverse(samples);
            }

            if (count is 0)
            {
                samples = [];
            }
            else if (count is > 0 && samples.Length > count.Value)
            {
                samples = samples[..count.Value];
            }

            if (samples.Length == 0)
            {
                continue;
            }

            entries.Add(new TimeSeriesMRangeEntry(seriesEntry.Key, seriesEntry.State.Labels, samples));
        }

        return new TimeSeriesMRangeResult(TimeSeriesResultStatus.Ok, [.. entries]);
    }

    public async Task<TimeSeriesRangeResult> TimeSeriesRangeAsync(
        string key,
        long fromTimestamp,
        long toTimestamp,
        bool reverse,
        int? count,
        string? aggregationType,
        long? bucketDurationMs,
        CancellationToken token = default)
    {
        ValidateKey(key);
        if (!TryNormalizeTimeSeriesRangeArguments(fromTimestamp, toTimestamp, count, aggregationType, bucketDurationMs, out var normalizedAggregation))
        {
            return new TimeSeriesRangeResult(TimeSeriesResultStatus.InvalidArgument, []);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var loaded = await LoadTimeSeriesStateAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (loaded.Status != TimeSeriesResultStatus.Ok)
        {
            return new TimeSeriesRangeResult(loaded.Status, []);
        }

        var samples = await GetTimeSeriesSamplesInRangeAsync(connection, key, fromTimestamp, toTimestamp, token).ConfigureAwait(false);
        if (normalizedAggregation is not null)
        {
            samples = AggregateTimeSeriesSamples(samples, normalizedAggregation, bucketDurationMs!.Value);
        }

        if (reverse)
        {
            Array.Reverse(samples);
        }

        if (count is 0)
        {
            samples = [];
        }
        else if (count is > 0 && samples.Length > count.Value)
        {
            samples = samples[..count.Value];
        }

        return new TimeSeriesRangeResult(TimeSeriesResultStatus.Ok, samples);
    }

    private async Task<(TimeSeriesResultStatus Status, TimeSeriesState? State, DateTimeOffset? ExpiresAt)> LoadTimeSeriesStateAsync(
        NpgsqlConnection connection,
        string key,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return (TimeSeriesResultStatus.NotFound, null, null);
        }

        if (existing.Kind != TimeSeriesKind)
        {
            return (TimeSeriesResultStatus.WrongType, null, existing.ExpiresAt);
        }

        if (!TryDeserializeTimeSeriesState(existing.Value, out var state))
        {
            return (TimeSeriesResultStatus.InvalidArgument, null, existing.ExpiresAt);
        }

        return (TimeSeriesResultStatus.Ok, state, existing.ExpiresAt);
    }

    private async Task SaveTimeSeriesStateAsync(
        NpgsqlConnection connection,
        string key,
        TimeSeriesState state,
        DateTimeOffset? expiresAt,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await UpsertKeyMetadataAsync(
            connection,
            key,
            TimeSeriesKind,
            JsonSerializer.SerializeToUtf8Bytes(state),
            expiresAt,
            transaction,
            token).ConfigureAwait(false);
    }

    private async Task<TimeSeriesSample?> GetLatestTimeSeriesSampleAsync(
        NpgsqlConnection connection,
        string key,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT timestamp_ms, value
            FROM {_qualifiedTimeSeriesSampleTableName}
            WHERE key = @key
            ORDER BY timestamp_ms DESC
            LIMIT 1;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return null;
        }

        return new TimeSeriesSample(reader.GetInt64(0), reader.GetDouble(1));
    }

    private async Task<TimeSeriesSample?> GetTimeSeriesSampleAsync(
        NpgsqlConnection connection,
        string key,
        long timestamp,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT value
            FROM {_qualifiedTimeSeriesSampleTableName}
            WHERE key = @key
              AND timestamp_ms = @timestamp
            LIMIT 1;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("timestamp", timestamp);
        var result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        return result is double existingValue ? new TimeSeriesSample(timestamp, existingValue) : null;
    }

    private async Task UpsertTimeSeriesSampleAsync(
        NpgsqlConnection connection,
        string key,
        long timestamp,
        double value,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            INSERT INTO {_qualifiedTimeSeriesSampleTableName} (key, timestamp_ms, value)
            VALUES (@key, @timestamp, @value)
            ON CONFLICT (key, timestamp_ms) DO UPDATE
            SET value = EXCLUDED.value;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("timestamp", timestamp);
        command.Parameters.AddWithValue("value", value);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task TrimRetainedTimeSeriesSamplesAsync(
        NpgsqlConnection connection,
        string key,
        long maxTimestamp,
        long retentionTimeMs,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        if (retentionTimeMs <= 0)
        {
            return;
        }

        var threshold = maxTimestamp - retentionTimeMs;
        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedTimeSeriesSampleTableName}
            WHERE key = @key
              AND timestamp_ms < @threshold;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("threshold", threshold);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task<TimeSeriesSample[]> GetTimeSeriesSamplesInRangeAsync(
        NpgsqlConnection connection,
        string key,
        long fromTimestamp,
        long toTimestamp,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT timestamp_ms, value
            FROM {_qualifiedTimeSeriesSampleTableName}
            WHERE key = @key
              AND timestamp_ms >= @fromTimestamp
              AND timestamp_ms <= @toTimestamp
            ORDER BY timestamp_ms;
            """);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("fromTimestamp", fromTimestamp);
        command.Parameters.AddWithValue("toTimestamp", toTimestamp);

        var samples = new List<TimeSeriesSample>();
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            samples.Add(new TimeSeriesSample(reader.GetInt64(0), reader.GetDouble(1)));
        }

        return [.. samples];
    }

    private static bool TryDeserializeTimeSeriesState(byte[]? bytes, out TimeSeriesState? state)
    {
        state = null;
        if (bytes is null)
        {
            return false;
        }

        try
        {
            state = JsonSerializer.Deserialize<TimeSeriesState>(bytes);
            if (state is null)
            {
                return false;
            }

            state.Labels ??= [];
            return Enum.IsDefined(state.DuplicatePolicy) && state.RetentionTimeMs >= 0;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static TimeSeriesState CreateDefaultTimeSeriesState() =>
        new()
        {
            RetentionTimeMs = 0,
            DuplicatePolicy = TimeSeriesDuplicatePolicy.Block,
            Labels = []
        };

    private static bool IsValidTimeSeriesValue(double value) => !double.IsNaN(value) && !double.IsInfinity(value);

    private static bool IsOutsideTimeSeriesRetention(long timestamp, long maxTimestamp, long retentionTimeMs) =>
        retentionTimeMs > 0 && timestamp < maxTimestamp - retentionTimeMs;

    private static bool TryNormalizeTimeSeriesRangeArguments(
        long fromTimestamp,
        long toTimestamp,
        int? count,
        string? aggregationType,
        long? bucketDurationMs,
        out string? normalizedAggregation)
    {
        normalizedAggregation = null;
        if (fromTimestamp > toTimestamp || count is < 0)
        {
            return false;
        }

        if (aggregationType is null)
        {
            return bucketDurationMs is null;
        }

        if (bucketDurationMs is null || bucketDurationMs <= 0)
        {
            return false;
        }

        normalizedAggregation = aggregationType.ToUpperInvariant();
        return normalizedAggregation is "AVG" or "SUM" or "MIN" or "MAX" or "COUNT";
    }

    private static bool MatchesTimeSeriesFilters(TimeSeriesState state, KeyValuePair<string, string>[] filters)
    {
        foreach (var filter in filters)
        {
            if (!state.Labels.Any(label =>
                string.Equals(label.Key, filter.Key, StringComparison.Ordinal) &&
                string.Equals(label.Value, filter.Value, StringComparison.Ordinal)))
            {
                return false;
            }
        }

        return true;
    }

    private static TimeSeriesSample[] AggregateTimeSeriesSamples(TimeSeriesSample[] samples, string aggregationType, long bucketDurationMs)
    {
        if (samples.Length == 0)
        {
            return [];
        }

        var aggregated = new List<TimeSeriesSample>();
        var currentBucketStart = GetTimeSeriesBucketStart(samples[0].Timestamp, bucketDurationMs);
        var bucketSamples = new List<TimeSeriesSample>();
        foreach (var sample in samples)
        {
            var bucketStart = GetTimeSeriesBucketStart(sample.Timestamp, bucketDurationMs);
            if (bucketStart != currentBucketStart)
            {
                aggregated.Add(new TimeSeriesSample(currentBucketStart, ComputeTimeSeriesBucketValue(bucketSamples, aggregationType)));
                bucketSamples.Clear();
                currentBucketStart = bucketStart;
            }

            bucketSamples.Add(sample);
        }

        aggregated.Add(new TimeSeriesSample(currentBucketStart, ComputeTimeSeriesBucketValue(bucketSamples, aggregationType)));
        return [.. aggregated];
    }

    private static long GetTimeSeriesBucketStart(long timestamp, long bucketDurationMs)
    {
        var remainder = timestamp % bucketDurationMs;
        if (remainder < 0)
        {
            remainder += bucketDurationMs;
        }

        return timestamp - remainder;
    }

    private static double ComputeTimeSeriesBucketValue(List<TimeSeriesSample> samples, string aggregationType) =>
        aggregationType switch
        {
            "AVG" => samples.Average(static sample => sample.Value),
            "SUM" => samples.Sum(static sample => sample.Value),
            "MIN" => samples.Min(static sample => sample.Value),
            "MAX" => samples.Max(static sample => sample.Value),
            "COUNT" => samples.Count,
            _ => throw new InvalidOperationException("Unsupported aggregation type.")
        };

    private sealed class TimeSeriesState
    {
        public long RetentionTimeMs { get; set; }

        public TimeSeriesDuplicatePolicy DuplicatePolicy { get; set; } = TimeSeriesDuplicatePolicy.Block;

        public KeyValuePair<string, string>[] Labels { get; set; } = [];
    }
}
