using System.Globalization;
using System.Text;
using Dredis.Abstractions.Storage;
using Npgsql;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore
{
    public async Task<string?> StreamAddAsync(
        string key,
        string id,
        KeyValuePair<string, byte[]>[] fields,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(id);
        ValidateStreamFields(fields);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != StreamKind)
        {
            throw CreateWrongTypeException();
        }

        var lastGeneratedId = existing.Exists
            ? await GetStoredOrComputedStreamLastIdAsync(connection, key, existing.Value, transaction, token).ConfigureAwait(false)
            : (StreamId?)null;

        StreamId streamId;
        if (id == "*")
        {
            streamId = GenerateStreamId(lastGeneratedId);
        }
        else if (!TryParseStreamId(id, out streamId) ||
                 (lastGeneratedId is { } currentLast && streamId.CompareTo(currentLast) <= 0))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return null;
        }

        await UpsertStreamMetadataAsync(
            connection,
            key,
            streamId,
            existing.Exists ? existing.ExpiresAt : null,
            transaction,
            token).ConfigureAwait(false);
        await InsertStreamEntryAsync(connection, key, streamId, fields, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return streamId.ToString();
    }

    public async Task<long> StreamDeleteAsync(string key, string[] ids, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(ids);

        if (ids.Length == 0)
        {
            return 0;
        }

        var parsedIds = new StreamId[ids.Length];
        for (var i = 0; i < ids.Length; i++)
        {
            if (!TryParseStreamId(ids[i], out parsedIds[i]))
            {
                throw new ArgumentException("All stream ids must use the form 'ms-seq'.", nameof(ids));
            }
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return 0;
        }

        if (existing.Kind != StreamKind)
        {
            throw CreateWrongTypeException();
        }

        long removed = 0;
        foreach (var streamId in parsedIds)
        {
            removed += await DeleteStreamEntryAsync(connection, key, streamId, transaction, token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return removed;
    }

    public async Task<long> StreamLengthAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return 0;
        }

        if (existing.Kind != StreamKind)
        {
            throw CreateWrongTypeException();
        }

        await using var command = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*)
            FROM {_qualifiedStreamEntryTableName}
            WHERE key = @key;
            """);

        command.Parameters.AddWithValue("key", key);
        return (long)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);
    }

    public async Task<string?> StreamLastIdAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return null;
        }

        if (existing.Kind != StreamKind)
        {
            throw CreateWrongTypeException();
        }

        var streamId = await GetStoredOrComputedStreamLastIdAsync(connection, key, existing.Value, transaction: null, token).ConfigureAwait(false);
        return streamId?.ToString();
    }

    public async Task<StreamReadResult[]> StreamReadAsync(
        string[] keys,
        string[] ids,
        int? count,
        CancellationToken token = default)
    {
        ValidateStreamReadArguments(keys, ids, count);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var results = new StreamReadResult[keys.Length];

        for (var i = 0; i < keys.Length; i++)
        {
            var key = keys[i];
            var id = ids[i];
            var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
            if (!existing.Exists)
            {
                results[i] = new StreamReadResult(key, []);
                continue;
            }

            if (existing.Kind != StreamKind)
            {
                throw CreateWrongTypeException();
            }

            if (id == "$")
            {
                results[i] = new StreamReadResult(key, []);
                continue;
            }

            if (!TryParseStreamId(id, out var afterId))
            {
                throw new ArgumentException("All stream ids must use the form 'ms-seq' or '$'.", nameof(ids));
            }

            var entries = await LoadStreamEntriesAsync(connection, key, reverse: false, transaction: null, token).ConfigureAwait(false);
            var filtered = entries
                .Where(entry => entry.Id.CompareTo(afterId) > 0)
                .Take(count ?? int.MaxValue)
                .Select(static entry => entry.ToPublicEntry())
                .ToArray();

            results[i] = new StreamReadResult(key, filtered);
        }

        return results;
    }

    public async Task<StreamEntry[]> StreamRangeAsync(
        string key,
        string start,
        string end,
        int? count,
        CancellationToken token = default)
    {
        return await GetStreamRangeAsync(key, start, end, count, reverse: false, token).ConfigureAwait(false);
    }

    public async Task<StreamEntry[]> StreamRangeReverseAsync(
        string key,
        string start,
        string end,
        int? count,
        CancellationToken token = default)
    {
        return await GetStreamRangeAsync(key, start, end, count, reverse: true, token).ConfigureAwait(false);
    }

    public async Task<long> StreamTrimAsync(
        string key,
        int? maxLength = default,
        string? minId = default,
        bool approximate = default,
        CancellationToken token = default)
    {
        ValidateKey(key);

        if (maxLength is null && minId is null)
        {
            return 0;
        }

        if (maxLength is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxLength), "Maximum length cannot be negative.");
        }

        StreamId? minimumId = null;
        if (minId is not null)
        {
            if (!TryParseStreamId(minId, out var parsedMinId))
            {
                throw new ArgumentException("Stream trim min id must use the form 'ms-seq'.", nameof(minId));
            }

            minimumId = parsedMinId;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return 0;
        }

        if (existing.Kind != StreamKind)
        {
            throw CreateWrongTypeException();
        }

        var entries = await LoadStreamEntriesAsync(connection, key, reverse: false, transaction, token).ConfigureAwait(false);
        var toRemove = new HashSet<StreamId>();

        if (maxLength is { } requestedLength && entries.Count > requestedLength)
        {
            foreach (var entry in entries.Take(entries.Count - requestedLength))
            {
                toRemove.Add(entry.Id);
            }
        }

        if (minimumId is { } lowerBound)
        {
            foreach (var entry in entries)
            {
                if (entry.Id.CompareTo(lowerBound) < 0)
                {
                    toRemove.Add(entry.Id);
                }
            }
        }

        long removed = 0;
        foreach (var streamId in toRemove)
        {
            removed += await DeleteStreamEntryAsync(connection, key, streamId, transaction, token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return removed;
    }

    public async Task<StreamInfoResult> StreamInfoAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new StreamInfoResult(StreamInfoResultStatus.NoStream, info: null);
        }

        if (existing.Kind != StreamKind)
        {
            return new StreamInfoResult(StreamInfoResultStatus.WrongType, info: null);
        }

        var entries = await LoadStreamEntriesAsync(connection, key, reverse: false, transaction: null, token).ConfigureAwait(false);
        var lastGeneratedId = await GetStoredOrComputedStreamLastIdAsync(connection, key, existing.Value, transaction: null, token).ConfigureAwait(false);
        var info = new StreamInfo(
            entries.Count,
            lastGeneratedId?.ToString(),
            entries.Count == 0 ? null : entries[0].ToPublicEntry(),
            entries.Count == 0 ? null : entries[^1].ToPublicEntry());

        return new StreamInfoResult(StreamInfoResultStatus.Ok, info);
    }

    public async Task<StreamSetIdResultStatus> StreamSetIdAsync(
        string key,
        string lastId,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(lastId);

        if (!TryParseStreamId(lastId, out var streamId))
        {
            return StreamSetIdResultStatus.InvalidId;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != StreamKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamSetIdResultStatus.WrongType;
        }

        var maxEntryId = await GetMaxStreamEntryIdAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (maxEntryId is { } currentMaxEntryId && streamId.CompareTo(currentMaxEntryId) < 0)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamSetIdResultStatus.InvalidId;
        }

        await UpsertStreamMetadataAsync(
            connection,
            key,
            streamId,
            existing.Exists ? existing.ExpiresAt : null,
            transaction,
            token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return StreamSetIdResultStatus.Ok;
    }

    public async Task<StreamGroupCreateResult> StreamGroupCreateAsync(
        string key,
        string group,
        string startId,
        bool mkStream,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateStreamName(group, nameof(group));
        ArgumentNullException.ThrowIfNull(startId);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != StreamKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupCreateResult.WrongType;
        }

        if (!existing.Exists && !mkStream)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupCreateResult.NoStream;
        }

        var streamLastId = existing.Exists
            ? await GetStoredOrComputedStreamLastIdAsync(connection, key, existing.Value, transaction, token).ConfigureAwait(false) ?? StreamId.Zero
            : StreamId.Zero;
        if (!TryResolveGroupStreamId(startId, streamLastId, out var groupStartId))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupCreateResult.InvalidId;
        }

        if (!existing.Exists)
        {
            await UpsertStreamMetadataAsync(connection, key, StreamId.Zero, expiresAt: null, transaction, token).ConfigureAwait(false);
        }

        if (await StreamGroupExistsAsync(connection, key, group, transaction, token).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupCreateResult.Exists;
        }

        await using var command = CreateCommand(
            connection,
            $"""
            INSERT INTO {_qualifiedStreamGroupTableName} (key, group_name, last_delivered_ms, last_delivered_seq)
            VALUES (@key, @group, @lastDeliveredMs, @lastDeliveredSeq);
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        command.Parameters.AddWithValue("lastDeliveredMs", groupStartId.Milliseconds);
        command.Parameters.AddWithValue("lastDeliveredSeq", groupStartId.Sequence);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return StreamGroupCreateResult.Ok;
    }

    public async Task<StreamGroupDestroyResult> StreamGroupDestroyAsync(
        string key,
        string group,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateStreamName(group, nameof(group));

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return StreamGroupDestroyResult.NotFound;
        }

        if (existing.Kind != StreamKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupDestroyResult.WrongType;
        }

        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedStreamGroupTableName}
            WHERE key = @key
              AND group_name = @group;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        var removed = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return removed > 0 ? StreamGroupDestroyResult.Removed : StreamGroupDestroyResult.NotFound;
    }

    public async Task<StreamGroupSetIdResultStatus> StreamGroupSetIdAsync(
        string key,
        string group,
        string lastId,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateStreamName(group, nameof(group));
        ArgumentNullException.ThrowIfNull(lastId);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupSetIdResultStatus.NoStream;
        }

        if (existing.Kind != StreamKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupSetIdResultStatus.WrongType;
        }

        if (!await StreamGroupExistsAsync(connection, key, group, transaction, token).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupSetIdResultStatus.NoGroup;
        }

        var streamLastId = await GetStoredOrComputedStreamLastIdAsync(connection, key, existing.Value, transaction, token).ConfigureAwait(false) ?? StreamId.Zero;
        if (!TryResolveGroupStreamId(lastId, streamLastId, out var resolvedId))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return StreamGroupSetIdResultStatus.InvalidId;
        }

        await UpdateStreamGroupLastDeliveredIdAsync(connection, key, group, resolvedId, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return StreamGroupSetIdResultStatus.Ok;
    }

    public async Task<StreamGroupDelConsumerResult> StreamGroupDelConsumerAsync(
        string key,
        string group,
        string consumer,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateStreamName(group, nameof(group));
        ValidateStreamName(consumer, nameof(consumer));

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.NoStream, 0);
        }

        if (existing.Kind != StreamKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.WrongType, 0);
        }

        if (!await StreamGroupExistsAsync(connection, key, group, transaction, token).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.NoGroup, 0);
        }

        var removedPending = await CountPendingEntriesAsync(connection, key, group, consumer, transaction, token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedStreamConsumerTableName}
            WHERE key = @key
              AND group_name = @group
              AND consumer_name = @consumer;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        command.Parameters.AddWithValue("consumer", consumer);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new StreamGroupDelConsumerResult(StreamGroupDelConsumerResultStatus.Ok, removedPending);
    }

    public async Task<StreamGroupReadResult> StreamGroupReadAsync(
        string group,
        string consumer,
        string[] keys,
        string[] ids,
        int? count,
        TimeSpan? block,
        CancellationToken token = default)
    {
        ValidateStreamName(group, nameof(group));
        ValidateStreamName(consumer, nameof(consumer));
        ValidateStreamReadArguments(keys, ids, count);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);

        for (var attempt = 0; attempt < 2; attempt++)
        {
            await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);
            var validationStatus = await ValidateStreamGroupTargetsAsync(connection, group, keys, transaction, token).ConfigureAwait(false);
            if (validationStatus is not null)
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return new StreamGroupReadResult(validationStatus.Value, []);
            }

            var now = DateTimeOffset.UtcNow;
            var results = new StreamReadResult[keys.Length];
            var anyEntries = false;

            for (var i = 0; i < keys.Length; i++)
            {
                var readEntries = ids[i] == ">"
                    ? await ReadNewGroupEntriesAsync(connection, keys[i], group, consumer, count, now, transaction, token).ConfigureAwait(false)
                    : await ReadPendingGroupEntriesAsync(connection, keys[i], group, consumer, ids[i], count, now, transaction, token).ConfigureAwait(false);

                if (readEntries is null)
                {
                    await transaction.RollbackAsync(token).ConfigureAwait(false);
                    return new StreamGroupReadResult(StreamGroupReadResultStatus.InvalidId, []);
                }

                anyEntries |= readEntries.Count > 0;
                results[i] = new StreamReadResult(keys[i], readEntries.Select(static entry => entry.ToPublicEntry()).ToArray());
            }

            if (!anyEntries && attempt == 0 && block.HasValue)
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                if (block.Value > TimeSpan.Zero)
                {
                    await Task.Delay(block.Value, token).ConfigureAwait(false);
                }

                continue;
            }

            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new StreamGroupReadResult(StreamGroupReadResultStatus.Ok, results);
        }

        return new StreamGroupReadResult(StreamGroupReadResultStatus.Ok, []);
    }

    public async Task<StreamAckResult> StreamAckAsync(
        string key,
        string group,
        string[] ids,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateStreamName(group, nameof(group));
        ArgumentNullException.ThrowIfNull(ids);

        var parsedIds = new List<StreamId>(ids.Length);
        foreach (var id in ids)
        {
            if (!TryParseStreamId(id, out var streamId))
            {
                throw new ArgumentException("All stream ids must use the form 'ms-seq'.", nameof(ids));
            }

            parsedIds.Add(streamId);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var validation = await ValidateStreamGroupOperationAsync(connection, key, group, transaction, token).ConfigureAwait(false);
        if (validation is not null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new StreamAckResult(validation.Value, 0);
        }

        long removed = 0;
        foreach (var streamId in parsedIds)
        {
            await using var command = CreateCommand(
                connection,
                $"""
                DELETE FROM {_qualifiedStreamPendingTableName}
                WHERE key = @key
                  AND group_name = @group
                  AND id_ms = @idMs
                  AND id_seq = @idSeq;
                """,
                transaction);

            command.Parameters.AddWithValue("key", key);
            command.Parameters.AddWithValue("group", group);
            command.Parameters.AddWithValue("idMs", streamId.Milliseconds);
            command.Parameters.AddWithValue("idSeq", streamId.Sequence);
            removed += await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new StreamAckResult(StreamAckResultStatus.Ok, removed);
    }

    public async Task<StreamPendingResult> StreamPendingAsync(
        string key,
        string group,
        long? minIdleTimeMs = default,
        string? start = default,
        string? end = default,
        int? count = default,
        string? consumer = default,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateStreamName(group, nameof(group));

        if (count is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count cannot be negative.");
        }

        if (consumer is not null)
        {
            ValidateStreamName(consumer, nameof(consumer));
        }

        StreamId? startId = null;
        StreamId? endId = null;
        if (start is not null)
        {
            if (!TryParseStreamId(start, out var parsedStart))
            {
                throw new ArgumentException("Start id must use the form 'ms-seq'.", nameof(start));
            }

            startId = parsedStart;
        }

        if (end is not null)
        {
            if (!TryParseStreamId(end, out var parsedEnd))
            {
                throw new ArgumentException("End id must use the form 'ms-seq'.", nameof(end));
            }

            endId = parsedEnd;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var validation = await ValidateStreamGroupOperationAsync(connection, key, group, transaction, token).ConfigureAwait(false);
        if (validation is not null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return validation.Value switch
            {
                StreamAckResultStatus.NoGroup => new StreamPendingResult(StreamPendingResultStatus.NoGroup, 0, null, null, [], []),
                StreamAckResultStatus.NoStream => new StreamPendingResult(StreamPendingResultStatus.NoStream, 0, null, null, [], []),
                _ => new StreamPendingResult(StreamPendingResultStatus.WrongType, 0, null, null, [], [])
            };
        }

        var pending = await LoadPendingRecordsAsync(connection, key, group, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);

        if (count is null && startId is null && endId is null && minIdleTimeMs is null && consumer is null)
        {
            return CreatePendingSummary(pending);
        }

        var now = DateTimeOffset.UtcNow;
        var filtered = pending
            .Where(record => minIdleTimeMs is null || GetIdleTimeMilliseconds(record.LastDeliveredAt, now) >= minIdleTimeMs.Value)
            .Where(record => startId is null || record.Id.CompareTo(startId.Value) >= 0)
            .Where(record => endId is null || record.Id.CompareTo(endId.Value) <= 0)
            .Where(record => consumer is null || string.Equals(record.Consumer, consumer, StringComparison.Ordinal))
            .Take(count ?? int.MaxValue)
            .Select(record => new StreamPendingEntry(
                record.Id.ToString(),
                record.Consumer,
                GetIdleTimeMilliseconds(record.LastDeliveredAt, now),
                record.DeliveryCount))
            .ToArray();

        return new StreamPendingResult(StreamPendingResultStatus.Ok, filtered.Length, null, null, [], filtered);
    }

    public async Task<StreamClaimResult> StreamClaimAsync(
        string key,
        string group,
        string consumer,
        long minIdleTimeMs,
        string[] ids,
        long? idleMs = default,
        long? timeMs = default,
        long? retryCount = default,
        bool force = default,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateStreamName(group, nameof(group));
        ValidateStreamName(consumer, nameof(consumer));
        ArgumentNullException.ThrowIfNull(ids);

        if (minIdleTimeMs < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(minIdleTimeMs), "Minimum idle time cannot be negative.");
        }

        if (idleMs is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(idleMs), "Idle time cannot be negative.");
        }

        if (retryCount is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(retryCount), "Retry count cannot be negative.");
        }

        var parsedIds = new List<StreamId>(ids.Length);
        foreach (var id in ids)
        {
            if (!TryParseStreamId(id, out var streamId))
            {
                throw new ArgumentException("All stream ids must use the form 'ms-seq'.", nameof(ids));
            }

            parsedIds.Add(streamId);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var validation = await ValidateStreamGroupOperationAsync(connection, key, group, transaction, token).ConfigureAwait(false);
        if (validation is not null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return validation.Value switch
            {
                StreamAckResultStatus.NoGroup => new StreamClaimResult(StreamClaimResultStatus.NoGroup, []),
                StreamAckResultStatus.NoStream => new StreamClaimResult(StreamClaimResultStatus.NoStream, []),
                _ => new StreamClaimResult(StreamClaimResultStatus.WrongType, [])
            };
        }

        var now = DateTimeOffset.UtcNow;
        var claimedEntries = new List<StreamEntryRecord>();
        foreach (var streamId in parsedIds)
        {
            var pending = await GetPendingRecordAsync(connection, key, group, streamId, transaction, token).ConfigureAwait(false);
            if (pending is null)
            {
                if (!force)
                {
                    continue;
                }

                var streamEntry = await LoadStreamEntryAsync(connection, key, streamId, transaction, token).ConfigureAwait(false);
                if (streamEntry is null)
                {
                    continue;
                }

                await UpsertStreamConsumerAsync(connection, key, group, consumer, now, transaction, token).ConfigureAwait(false);
                await SetPendingEntryAsync(
                    connection,
                    key,
                    group,
                    streamId,
                    consumer,
                    ResolvePendingTimestamp(now, idleMs, timeMs),
                    retryCount ?? 1,
                    transaction,
                    token).ConfigureAwait(false);
                claimedEntries.Add(streamEntry);
                continue;
            }

            if (GetIdleTimeMilliseconds(pending.LastDeliveredAt, now) < minIdleTimeMs)
            {
                continue;
            }

            var claimedEntry = await LoadStreamEntryAsync(connection, key, streamId, transaction, token).ConfigureAwait(false);
            if (claimedEntry is null)
            {
                continue;
            }

            await UpsertStreamConsumerAsync(connection, key, group, consumer, now, transaction, token).ConfigureAwait(false);
            await SetPendingEntryAsync(
                connection,
                key,
                group,
                streamId,
                consumer,
                ResolvePendingTimestamp(now, idleMs, timeMs),
                retryCount ?? (pending.DeliveryCount + 1),
                transaction,
                token).ConfigureAwait(false);
            claimedEntries.Add(claimedEntry);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new StreamClaimResult(StreamClaimResultStatus.Ok, claimedEntries.Select(static entry => entry.ToPublicEntry()).ToArray());
    }

    public async Task<StreamGroupsInfoResult> StreamGroupsInfoAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new StreamGroupsInfoResult(StreamInfoResultStatus.NoStream, []);
        }

        if (existing.Kind != StreamKind)
        {
            return new StreamGroupsInfoResult(StreamInfoResultStatus.WrongType, []);
        }

        var groups = new List<StreamGroupInfo>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT g.group_name,
                   (
                       SELECT COUNT(*)
                       FROM {_qualifiedStreamConsumerTableName} AS c
                       WHERE c.key = g.key
                         AND c.group_name = g.group_name
                   ) AS consumer_count,
                   (
                       SELECT COUNT(*)
                       FROM {_qualifiedStreamPendingTableName} AS p
                       WHERE p.key = g.key
                         AND p.group_name = g.group_name
                   ) AS pending_count,
                   g.last_delivered_ms,
                   g.last_delivered_seq
            FROM {_qualifiedStreamGroupTableName} AS g
            WHERE g.key = @key
            ORDER BY g.group_name;
            """);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            groups.Add(new StreamGroupInfo(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                new StreamId(reader.GetInt64(3), reader.GetInt64(4)).ToString()));
        }

        return new StreamGroupsInfoResult(StreamInfoResultStatus.Ok, [.. groups]);
    }

    public async Task<StreamConsumersInfoResult> StreamConsumersInfoAsync(
        string key,
        string group,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateStreamName(group, nameof(group));

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new StreamConsumersInfoResult(StreamInfoResultStatus.NoStream, []);
        }

        if (existing.Kind != StreamKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new StreamConsumersInfoResult(StreamInfoResultStatus.WrongType, []);
        }

        if (!await StreamGroupExistsAsync(connection, key, group, transaction, token).ConfigureAwait(false))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new StreamConsumersInfoResult(StreamInfoResultStatus.NoGroup, []);
        }

        var consumers = new List<StreamConsumerInfo>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT c.consumer_name,
                   (
                       SELECT COUNT(*)
                       FROM {_qualifiedStreamPendingTableName} AS p
                       WHERE p.key = c.key
                         AND p.group_name = c.group_name
                         AND p.consumer_name = c.consumer_name
                   ) AS pending_count,
                   GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (NOW() - c.last_seen_at)) * 1000))::bigint AS idle_ms
            FROM {_qualifiedStreamConsumerTableName} AS c
            WHERE c.key = @key
              AND c.group_name = @group
            ORDER BY c.consumer_name;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            consumers.Add(new StreamConsumerInfo(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2)));
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new StreamConsumersInfoResult(StreamInfoResultStatus.Ok, [.. consumers]);
    }

    private async Task<StreamEntry[]> GetStreamRangeAsync(
        string key,
        string start,
        string end,
        int? count,
        bool reverse,
        CancellationToken token)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(start);
        ArgumentNullException.ThrowIfNull(end);

        if (count is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count cannot be negative.");
        }

        if (!TryResolveStreamRangeBound(start, out var startBound) ||
            !TryResolveStreamRangeBound(end, out var endBound))
        {
            throw new ArgumentException("Stream range bounds must be '-', '+', or 'ms-seq'.");
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return [];
        }

        if (existing.Kind != StreamKind)
        {
            throw CreateWrongTypeException();
        }

        var entries = await LoadStreamEntriesAsync(connection, key, reverse, transaction: null, token).ConfigureAwait(false);
        var lowerBound = reverse ? endBound : startBound;
        var upperBound = reverse ? startBound : endBound;
        var filtered = entries
            .Where(entry => IsWithinRange(entry.Id, lowerBound, upperBound))
            .Take(count ?? int.MaxValue)
            .Select(static entry => entry.ToPublicEntry())
            .ToArray();

        return filtered;
    }

    private async Task<List<StreamEntryRecord>> ReadNewGroupEntriesAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        string consumer,
        int? count,
        DateTimeOffset now,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        var lastDeliveredId = await GetStreamGroupLastDeliveredIdAsync(connection, key, group, transaction, token).ConfigureAwait(false) ?? StreamId.Zero;
        var entries = await LoadStreamEntriesAsync(connection, key, reverse: false, transaction, token).ConfigureAwait(false);
        var selected = entries
            .Where(entry => entry.Id.CompareTo(lastDeliveredId) > 0)
            .Take(count ?? int.MaxValue)
            .ToList();

        if (selected.Count == 0)
        {
            return selected;
        }

        await UpsertStreamConsumerAsync(connection, key, group, consumer, now, transaction, token).ConfigureAwait(false);
        foreach (var entry in selected)
        {
            var pending = await GetPendingRecordAsync(connection, key, group, entry.Id, transaction, token).ConfigureAwait(false);
            await SetPendingEntryAsync(
                connection,
                key,
                group,
                entry.Id,
                consumer,
                now,
                pending?.DeliveryCount + 1 ?? 1,
                transaction,
                token).ConfigureAwait(false);
        }

        await UpdateStreamGroupLastDeliveredIdAsync(connection, key, group, selected[^1].Id, transaction, token).ConfigureAwait(false);
        return selected;
    }

    private async Task<List<StreamEntryRecord>?> ReadPendingGroupEntriesAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        string consumer,
        string id,
        int? count,
        DateTimeOffset now,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        if (!TryParseStreamId(id, out var afterId))
        {
            return null;
        }

        var pending = await LoadPendingRecordsAsync(connection, key, group, transaction, token).ConfigureAwait(false);
        var pendingIds = pending
            .Where(record => string.Equals(record.Consumer, consumer, StringComparison.Ordinal))
            .Where(record => record.Id.CompareTo(afterId) > 0)
            .Take(count ?? int.MaxValue)
            .ToList();

        if (pendingIds.Count == 0)
        {
            return [];
        }

        await UpsertStreamConsumerAsync(connection, key, group, consumer, now, transaction, token).ConfigureAwait(false);
        var entries = new List<StreamEntryRecord>(pendingIds.Count);
        foreach (var pendingRecord in pendingIds)
        {
            await SetPendingEntryAsync(
                connection,
                key,
                group,
                pendingRecord.Id,
                consumer,
                now,
                pendingRecord.DeliveryCount + 1,
                transaction,
                token).ConfigureAwait(false);

            var entry = await LoadStreamEntryAsync(connection, key, pendingRecord.Id, transaction, token).ConfigureAwait(false);
            if (entry is not null)
            {
                entries.Add(entry);
            }
        }

        return entries;
    }

    private async Task<StreamGroupReadResultStatus?> ValidateStreamGroupTargetsAsync(
        NpgsqlConnection connection,
        string group,
        IEnumerable<string> keys,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        foreach (var key in keys)
        {
            var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
            if (!existing.Exists)
            {
                return StreamGroupReadResultStatus.NoStream;
            }

            if (existing.Kind != StreamKind)
            {
                return StreamGroupReadResultStatus.WrongType;
            }

            if (!await StreamGroupExistsAsync(connection, key, group, transaction, token).ConfigureAwait(false))
            {
                return StreamGroupReadResultStatus.NoGroup;
            }
        }

        return null;
    }

    private async Task<StreamAckResultStatus?> ValidateStreamGroupOperationAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return StreamAckResultStatus.NoStream;
        }

        if (existing.Kind != StreamKind)
        {
            return StreamAckResultStatus.WrongType;
        }

        if (!await StreamGroupExistsAsync(connection, key, group, transaction, token).ConfigureAwait(false))
        {
            return StreamAckResultStatus.NoGroup;
        }

        return null;
    }

    private static void ValidateStreamReadArguments(string[] keys, string[] ids, int? count)
    {
        ValidateKeys(keys);
        ArgumentNullException.ThrowIfNull(ids);

        if (keys.Length != ids.Length)
        {
            throw new ArgumentException("Keys and ids must contain the same number of items.");
        }

        if (count is < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(count), "Count cannot be negative.");
        }

        foreach (var id in ids)
        {
            ArgumentNullException.ThrowIfNull(id);
        }
    }

    private static void ValidateStreamFields(KeyValuePair<string, byte[]>[] fields)
    {
        ArgumentNullException.ThrowIfNull(fields);
        if (fields.Length == 0)
        {
            throw new ArgumentException("Stream entries must include at least one field/value pair.", nameof(fields));
        }

        foreach (var field in fields)
        {
            ArgumentNullException.ThrowIfNull(field.Key);
            ArgumentNullException.ThrowIfNull(field.Value, nameof(fields));
        }
    }

    private static void ValidateStreamName(string value, string paramName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            throw new ArgumentException("Value cannot be null or whitespace.", paramName);
        }
    }

    private async Task UpsertStreamMetadataAsync(
        NpgsqlConnection connection,
        string key,
        StreamId lastGeneratedId,
        DateTimeOffset? expiresAt,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await UpsertKeyMetadataAsync(
            connection,
            key,
            StreamKind,
            Encoding.UTF8.GetBytes(lastGeneratedId.ToString()),
            expiresAt,
            transaction,
            token).ConfigureAwait(false);
    }

    private async Task InsertStreamEntryAsync(
        NpgsqlConnection connection,
        string key,
        StreamId streamId,
        IReadOnlyList<KeyValuePair<string, byte[]>> fields,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await using var entryCommand = CreateCommand(
            connection,
            $"""
            INSERT INTO {_qualifiedStreamEntryTableName} (key, id_ms, id_seq)
            VALUES (@key, @idMs, @idSeq);
            """,
            transaction);

        entryCommand.Parameters.AddWithValue("key", key);
        entryCommand.Parameters.AddWithValue("idMs", streamId.Milliseconds);
        entryCommand.Parameters.AddWithValue("idSeq", streamId.Sequence);
        await entryCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);

        for (var index = 0; index < fields.Count; index++)
        {
            await using var fieldCommand = CreateCommand(
                connection,
                $"""
                INSERT INTO {_qualifiedStreamFieldTableName} (key, id_ms, id_seq, field_index, field, value)
                VALUES (@key, @idMs, @idSeq, @fieldIndex, @field, @value);
                """,
                transaction);

            fieldCommand.Parameters.AddWithValue("key", key);
            fieldCommand.Parameters.AddWithValue("idMs", streamId.Milliseconds);
            fieldCommand.Parameters.AddWithValue("idSeq", streamId.Sequence);
            fieldCommand.Parameters.AddWithValue("fieldIndex", index);
            fieldCommand.Parameters.AddWithValue("field", fields[index].Key);
            fieldCommand.Parameters.AddWithValue("value", fields[index].Value);
            await fieldCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }
    }

    private async Task<long> DeleteStreamEntryAsync(
        NpgsqlConnection connection,
        string key,
        StreamId streamId,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedStreamEntryTableName}
            WHERE key = @key
              AND id_ms = @idMs
              AND id_seq = @idSeq;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("idMs", streamId.Milliseconds);
        command.Parameters.AddWithValue("idSeq", streamId.Sequence);
        return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task<List<StreamEntryRecord>> LoadStreamEntriesAsync(
        NpgsqlConnection connection,
        string key,
        bool reverse,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        var orderDirection = reverse ? "DESC" : "ASC";
        var entries = new List<StreamEntryRecord>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT e.id_ms,
                   e.id_seq,
                   f.field_index,
                   f.field,
                   f.value
            FROM {_qualifiedStreamEntryTableName} AS e
            LEFT JOIN {_qualifiedStreamFieldTableName} AS f
              ON f.key = e.key
             AND f.id_ms = e.id_ms
             AND f.id_seq = e.id_seq
            WHERE e.key = @key
            ORDER BY e.id_ms {orderDirection}, e.id_seq {orderDirection}, f.field_index ASC;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);

        StreamId? currentId = null;
        List<KeyValuePair<string, byte[]>> currentFields = [];
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var streamId = new StreamId(reader.GetInt64(0), reader.GetInt64(1));
            if (currentId is null || streamId.CompareTo(currentId.Value) != 0)
            {
                if (currentId is not null)
                {
                    entries.Add(new StreamEntryRecord(currentId.Value, [.. currentFields]));
                }

                currentId = streamId;
                currentFields = [];
            }

            if (!reader.IsDBNull(2))
            {
                currentFields.Add(new KeyValuePair<string, byte[]>(reader.GetString(3), reader.GetFieldValue<byte[]>(4)));
            }
        }

        if (currentId is not null)
        {
            entries.Add(new StreamEntryRecord(currentId.Value, [.. currentFields]));
        }

        return entries;
    }

    private async Task<StreamEntryRecord?> LoadStreamEntryAsync(
        NpgsqlConnection connection,
        string key,
        StreamId streamId,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        var entries = await LoadStreamEntriesAsync(connection, key, reverse: false, transaction, token).ConfigureAwait(false);
        return entries.FirstOrDefault(entry => entry.Id.CompareTo(streamId) == 0);
    }

    private async Task<StreamId?> GetStoredOrComputedStreamLastIdAsync(
        NpgsqlConnection connection,
        string key,
        byte[]? storedValue,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        if (TryDecodeStoredStreamId(storedValue, out var streamId))
        {
            return streamId;
        }

        return await GetMaxStreamEntryIdAsync(connection, key, transaction, token).ConfigureAwait(false);
    }

    private async Task<StreamId?> GetMaxStreamEntryIdAsync(
        NpgsqlConnection connection,
        string key,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT id_ms, id_seq
            FROM {_qualifiedStreamEntryTableName}
            WHERE key = @key
            ORDER BY id_ms DESC, id_seq DESC
            LIMIT 1;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return null;
        }

        return new StreamId(reader.GetInt64(0), reader.GetInt64(1));
    }

    private async Task<bool> StreamGroupExistsAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT EXISTS (
                SELECT 1
                FROM {_qualifiedStreamGroupTableName}
                WHERE key = @key
                  AND group_name = @group
            );
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        return (bool)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? false);
    }

    private async Task<StreamId?> GetStreamGroupLastDeliveredIdAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT last_delivered_ms, last_delivered_seq
            FROM {_qualifiedStreamGroupTableName}
            WHERE key = @key
              AND group_name = @group
            LIMIT 1;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return null;
        }

        return new StreamId(reader.GetInt64(0), reader.GetInt64(1));
    }

    private async Task UpdateStreamGroupLastDeliveredIdAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        StreamId streamId,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            UPDATE {_qualifiedStreamGroupTableName}
            SET last_delivered_ms = @lastDeliveredMs,
                last_delivered_seq = @lastDeliveredSeq
            WHERE key = @key
              AND group_name = @group;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        command.Parameters.AddWithValue("lastDeliveredMs", streamId.Milliseconds);
        command.Parameters.AddWithValue("lastDeliveredSeq", streamId.Sequence);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task UpsertStreamConsumerAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        string consumer,
        DateTimeOffset lastSeenAt,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            INSERT INTO {_qualifiedStreamConsumerTableName} (key, group_name, consumer_name, last_seen_at)
            VALUES (@key, @group, @consumer, @lastSeenAt)
            ON CONFLICT (key, group_name, consumer_name) DO UPDATE
            SET last_seen_at = EXCLUDED.last_seen_at;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        command.Parameters.AddWithValue("consumer", consumer);
        command.Parameters.AddWithValue("lastSeenAt", lastSeenAt);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task SetPendingEntryAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        StreamId streamId,
        string consumer,
        DateTimeOffset lastDeliveredAt,
        long deliveryCount,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            INSERT INTO {_qualifiedStreamPendingTableName}
                (key, group_name, id_ms, id_seq, consumer_name, last_delivered_at, delivery_count)
            VALUES
                (@key, @group, @idMs, @idSeq, @consumer, @lastDeliveredAt, @deliveryCount)
            ON CONFLICT (key, group_name, id_ms, id_seq) DO UPDATE
            SET consumer_name = EXCLUDED.consumer_name,
                last_delivered_at = EXCLUDED.last_delivered_at,
                delivery_count = EXCLUDED.delivery_count;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        command.Parameters.AddWithValue("idMs", streamId.Milliseconds);
        command.Parameters.AddWithValue("idSeq", streamId.Sequence);
        command.Parameters.AddWithValue("consumer", consumer);
        command.Parameters.AddWithValue("lastDeliveredAt", lastDeliveredAt);
        command.Parameters.AddWithValue("deliveryCount", deliveryCount);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task<StreamPendingRecord?> GetPendingRecordAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        StreamId streamId,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT consumer_name, last_delivered_at, delivery_count
            FROM {_qualifiedStreamPendingTableName}
            WHERE key = @key
              AND group_name = @group
              AND id_ms = @idMs
              AND id_seq = @idSeq
            LIMIT 1;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        command.Parameters.AddWithValue("idMs", streamId.Milliseconds);
        command.Parameters.AddWithValue("idSeq", streamId.Sequence);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return null;
        }

        return new StreamPendingRecord(
            streamId,
            reader.GetString(0),
            reader.GetFieldValue<DateTimeOffset>(1),
            reader.GetInt64(2));
    }

    private async Task<List<StreamPendingRecord>> LoadPendingRecordsAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        var records = new List<StreamPendingRecord>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT id_ms, id_seq, consumer_name, last_delivered_at, delivery_count
            FROM {_qualifiedStreamPendingTableName}
            WHERE key = @key
              AND group_name = @group
            ORDER BY id_ms ASC, id_seq ASC;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            records.Add(new StreamPendingRecord(
                new StreamId(reader.GetInt64(0), reader.GetInt64(1)),
                reader.GetString(2),
                reader.GetFieldValue<DateTimeOffset>(3),
                reader.GetInt64(4)));
        }

        return records;
    }

    private async Task<long> CountPendingEntriesAsync(
        NpgsqlConnection connection,
        string key,
        string group,
        string consumer,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*)
            FROM {_qualifiedStreamPendingTableName}
            WHERE key = @key
              AND group_name = @group
              AND consumer_name = @consumer;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("group", group);
        command.Parameters.AddWithValue("consumer", consumer);
        return (long)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);
    }

    private static StreamPendingResult CreatePendingSummary(IReadOnlyList<StreamPendingRecord> pending)
    {
        if (pending.Count == 0)
        {
            return new StreamPendingResult(StreamPendingResultStatus.Ok, 0, null, null, [], []);
        }

        var consumers = pending
            .GroupBy(static record => record.Consumer, StringComparer.Ordinal)
            .Select(static group => new StreamPendingConsumerInfo(group.Key, group.LongCount()))
            .OrderBy(static info => info.Name, StringComparer.Ordinal)
            .ToArray();

        return new StreamPendingResult(
            StreamPendingResultStatus.Ok,
            pending.Count,
            pending[0].Id.ToString(),
            pending[^1].Id.ToString(),
            consumers,
            []);
    }

    private static DateTimeOffset ResolvePendingTimestamp(DateTimeOffset now, long? idleMs, long? timeMs)
    {
        if (idleMs is { } idle)
        {
            return now.AddMilliseconds(-idle);
        }

        if (timeMs is { } unixMilliseconds)
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(unixMilliseconds);
        }

        return now;
    }

    private static long GetIdleTimeMilliseconds(DateTimeOffset lastDeliveredAt, DateTimeOffset now) =>
        Math.Max(0L, (long)Math.Floor((now - lastDeliveredAt).TotalMilliseconds));

    private static bool TryResolveStreamRangeBound(string text, out StreamId? streamId)
    {
        if (text == "-")
        {
            streamId = null;
            return true;
        }

        if (text == "+")
        {
            streamId = null;
            return true;
        }

        if (TryParseStreamId(text, out var parsed))
        {
            streamId = parsed;
            return true;
        }

        streamId = null;
        return false;
    }

    private static bool IsWithinRange(StreamId streamId, StreamId? lowerBound, StreamId? upperBound)
    {
        if (lowerBound is { } lower && streamId.CompareTo(lower) < 0)
        {
            return false;
        }

        if (upperBound is { } upper && streamId.CompareTo(upper) > 0)
        {
            return false;
        }

        return true;
    }

    private static bool TryResolveGroupStreamId(string text, StreamId lastGeneratedId, out StreamId streamId)
    {
        if (text == "-")
        {
            streamId = StreamId.Zero;
            return true;
        }

        if (text == "$")
        {
            streamId = lastGeneratedId;
            return true;
        }

        return TryParseStreamId(text, out streamId);
    }

    private static bool TryParseStreamId(string text, out StreamId streamId)
    {
        streamId = default;
        var parts = text.Split('-', StringSplitOptions.None);
        if (parts.Length != 2)
        {
            return false;
        }

        if (!long.TryParse(parts[0], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var milliseconds) ||
            !long.TryParse(parts[1], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var sequence))
        {
            return false;
        }

        streamId = new StreamId(milliseconds, sequence);
        return true;
    }

    private static bool TryDecodeStoredStreamId(byte[]? value, out StreamId streamId)
    {
        streamId = default;
        return value is not null && TryParseStreamId(Encoding.UTF8.GetString(value), out streamId);
    }

    private static StreamId GenerateStreamId(StreamId? lastGeneratedId)
    {
        var nowMilliseconds = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (lastGeneratedId is { } currentLast && currentLast.Milliseconds >= nowMilliseconds)
        {
            return new StreamId(currentLast.Milliseconds, currentLast.Sequence + 1);
        }

        return new StreamId(nowMilliseconds, 0);
    }

    private readonly record struct StreamId(long Milliseconds, long Sequence) : IComparable<StreamId>
    {
        public static StreamId Zero => new(0, 0);

        public int CompareTo(StreamId other)
        {
            var millisecondsComparison = Milliseconds.CompareTo(other.Milliseconds);
            return millisecondsComparison != 0 ? millisecondsComparison : Sequence.CompareTo(other.Sequence);
        }

        public override string ToString() =>
            string.Create(
                CultureInfo.InvariantCulture,
                $"{Milliseconds.ToString(CultureInfo.InvariantCulture)}-{Sequence.ToString(CultureInfo.InvariantCulture)}");
    }

    private sealed class StreamEntryRecord(StreamId id, KeyValuePair<string, byte[]>[] fields)
    {
        public StreamId Id { get; } = id;

        public KeyValuePair<string, byte[]>[] Fields { get; } = fields;

        public StreamEntry ToPublicEntry() => new(Id.ToString(), Fields);
    }

    private sealed class StreamPendingRecord(
        StreamId id,
        string consumer,
        DateTimeOffset lastDeliveredAt,
        long deliveryCount)
    {
        public StreamId Id { get; } = id;

        public string Consumer { get; } = consumer;

        public DateTimeOffset LastDeliveredAt { get; } = lastDeliveredAt;

        public long DeliveryCount { get; } = deliveryCount;
    }
}
