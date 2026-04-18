using Dredis.Abstractions.Storage;
using Npgsql;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore
{
    public async Task<SortedSetCountResult> SortedSetAddAsync(
        string key,
        SortedSetEntry[] entries,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(entries);

        foreach (var entry in entries)
        {
            ArgumentNullException.ThrowIfNull(entry);
            ArgumentNullException.ThrowIfNull(entry.Member);
        }

        if (entries.Length == 0)
        {
            return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != SortedSetKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new SortedSetCountResult(SortedSetResultStatus.WrongType, 0);
        }

        if (!existing.Exists)
        {
            await UpsertKeyMetadataAsync(connection, key, SortedSetKind, value: null, expiresAt: null, transaction, token).ConfigureAwait(false);
        }

        long added = 0;
        foreach (var entry in entries)
        {
            await using var insertCommand = CreateCommand(
                connection,
                $"""
                INSERT INTO {_qualifiedSortedSetTableName} (key, member, score)
                VALUES (@key, @member, @score)
                ON CONFLICT DO NOTHING;
                """,
                transaction);

            insertCommand.Parameters.AddWithValue("key", key);
            insertCommand.Parameters.AddWithValue("member", entry.Member);
            insertCommand.Parameters.AddWithValue("score", entry.Score);

            var inserted = await insertCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false) > 0;
            if (inserted)
            {
                added++;
                continue;
            }

            await using var updateCommand = CreateCommand(
                connection,
                $"""
                UPDATE {_qualifiedSortedSetTableName}
                SET score = @score
                WHERE key = @key
                  AND member = @member;
                """,
                transaction);

            updateCommand.Parameters.AddWithValue("key", key);
            updateCommand.Parameters.AddWithValue("member", entry.Member);
            updateCommand.Parameters.AddWithValue("score", entry.Score);
            await updateCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new SortedSetCountResult(SortedSetResultStatus.Ok, added);
    }

    public async Task<SortedSetCountResult> SortedSetRemoveAsync(
        string key,
        byte[][] members,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(members);
        foreach (var member in members)
        {
            ArgumentNullException.ThrowIfNull(member);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
        }

        if (existing.Kind != SortedSetKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new SortedSetCountResult(SortedSetResultStatus.WrongType, 0);
        }

        long removed = 0;
        foreach (var member in members)
        {
            await using var command = CreateCommand(
                connection,
                $"""
                DELETE FROM {_qualifiedSortedSetTableName}
                WHERE key = @key
                  AND member = @member;
                """,
                transaction);

            command.Parameters.AddWithValue("key", key);
            command.Parameters.AddWithValue("member", member);
            removed += await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        if (removed > 0)
        {
            await DeleteParentIfChildTableEmptyAsync(connection, _qualifiedSortedSetTableName, key, transaction, token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new SortedSetCountResult(SortedSetResultStatus.Ok, removed);
    }

    public async Task<SortedSetRangeResult> SortedSetRangeAsync(
        string key,
        int start,
        int stop,
        CancellationToken token = default)
    {
        return await GetSortedSetRangeByRankAsync(key, start, stop, reverse: false, token).ConfigureAwait(false);
    }

    public async Task<SortedSetCountResult> SortedSetCardinalityAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
        }

        if (existing.Kind != SortedSetKind)
        {
            return new SortedSetCountResult(SortedSetResultStatus.WrongType, 0);
        }

        await using var command = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*)
            FROM {_qualifiedSortedSetTableName}
            WHERE key = @key;
            """);

        command.Parameters.AddWithValue("key", key);
        var count = (long)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);
        return new SortedSetCountResult(SortedSetResultStatus.Ok, count);
    }

    public async Task<SortedSetScoreResult> SortedSetScoreAsync(string key, byte[] member, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(member);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new SortedSetScoreResult(SortedSetResultStatus.Ok, null);
        }

        if (existing.Kind != SortedSetKind)
        {
            return new SortedSetScoreResult(SortedSetResultStatus.WrongType, null);
        }

        await using var command = CreateCommand(
            connection,
            $"""
            SELECT score
            FROM {_qualifiedSortedSetTableName}
            WHERE key = @key
              AND member = @member
            LIMIT 1;
            """);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("member", member);

        var result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        return new SortedSetScoreResult(
            SortedSetResultStatus.Ok,
            result is null or DBNull ? null : (double)result);
    }

    public async Task<SortedSetRangeResult> SortedSetRangeByScoreAsync(
        string key,
        double minScore,
        double maxScore,
        CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new SortedSetRangeResult(SortedSetResultStatus.Ok, []);
        }

        if (existing.Kind != SortedSetKind)
        {
            return new SortedSetRangeResult(SortedSetResultStatus.WrongType, []);
        }

        var entries = new List<SortedSetEntry>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT member, score
            FROM {_qualifiedSortedSetTableName}
            WHERE key = @key
              AND score >= @minScore
              AND score <= @maxScore
            ORDER BY score ASC, member ASC;
            """);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("minScore", minScore);
        command.Parameters.AddWithValue("maxScore", maxScore);

        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            entries.Add(new SortedSetEntry(reader.GetFieldValue<byte[]>(0), reader.GetDouble(1)));
        }

        return new SortedSetRangeResult(SortedSetResultStatus.Ok, [.. entries]);
    }

    public async Task<SortedSetScoreResult> SortedSetIncrementAsync(
        string key,
        double increment,
        byte[] member,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(member);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != SortedSetKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new SortedSetScoreResult(SortedSetResultStatus.WrongType, null);
        }

        if (!existing.Exists)
        {
            await UpsertKeyMetadataAsync(connection, key, SortedSetKind, value: null, expiresAt: null, transaction, token).ConfigureAwait(false);
        }

        await using var currentScoreCommand = CreateCommand(
            connection,
            $"""
            SELECT score
            FROM {_qualifiedSortedSetTableName}
            WHERE key = @key
              AND member = @member
            LIMIT 1
            FOR UPDATE;
            """,
            transaction);

        currentScoreCommand.Parameters.AddWithValue("key", key);
        currentScoreCommand.Parameters.AddWithValue("member", member);
        var current = await currentScoreCommand.ExecuteScalarAsync(token).ConfigureAwait(false);
        var nextScore = (current is null or DBNull ? 0d : (double)current) + increment;

        await using var command = CreateCommand(
            connection,
            $"""
            INSERT INTO {_qualifiedSortedSetTableName} (key, member, score)
            VALUES (@key, @member, @score)
            ON CONFLICT (key, member) DO UPDATE
            SET score = EXCLUDED.score;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("member", member);
        command.Parameters.AddWithValue("score", nextScore);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new SortedSetScoreResult(SortedSetResultStatus.Ok, nextScore);
    }

    public async Task<SortedSetCountResult> SortedSetCountByScoreAsync(
        string key,
        double minScore,
        double maxScore,
        CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new SortedSetCountResult(SortedSetResultStatus.Ok, 0);
        }

        if (existing.Kind != SortedSetKind)
        {
            return new SortedSetCountResult(SortedSetResultStatus.WrongType, 0);
        }

        await using var command = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*)
            FROM {_qualifiedSortedSetTableName}
            WHERE key = @key
              AND score >= @minScore
              AND score <= @maxScore;
            """);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("minScore", minScore);
        command.Parameters.AddWithValue("maxScore", maxScore);

        var count = (long)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);
        return new SortedSetCountResult(SortedSetResultStatus.Ok, count);
    }

    public async Task<SortedSetRankResult> SortedSetRankAsync(
        string key,
        byte[] member,
        CancellationToken token = default)
    {
        return await GetSortedSetRankAsync(key, member, reverse: false, token).ConfigureAwait(false);
    }

    public async Task<SortedSetRankResult> SortedSetReverseRankAsync(
        string key,
        byte[] member,
        CancellationToken token = default)
    {
        return await GetSortedSetRankAsync(key, member, reverse: true, token).ConfigureAwait(false);
    }

    public async Task<SortedSetRemoveRangeResult> SortedSetRemoveRangeByScoreAsync(
        string key,
        double minScore,
        double maxScore,
        CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new SortedSetRemoveRangeResult(SortedSetResultStatus.Ok, 0);
        }

        if (existing.Kind != SortedSetKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new SortedSetRemoveRangeResult(SortedSetResultStatus.WrongType, 0);
        }

        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedSortedSetTableName}
            WHERE key = @key
              AND score >= @minScore
              AND score <= @maxScore;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("minScore", minScore);
        command.Parameters.AddWithValue("maxScore", maxScore);
        var removed = await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);

        if (removed > 0)
        {
            await DeleteParentIfChildTableEmptyAsync(connection, _qualifiedSortedSetTableName, key, transaction, token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new SortedSetRemoveRangeResult(SortedSetResultStatus.Ok, removed);
    }

    private async Task<SortedSetRangeResult> GetSortedSetRangeByRankAsync(
        string key,
        int start,
        int stop,
        bool reverse,
        CancellationToken token)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new SortedSetRangeResult(SortedSetResultStatus.Ok, []);
        }

        if (existing.Kind != SortedSetKind)
        {
            return new SortedSetRangeResult(SortedSetResultStatus.WrongType, []);
        }

        var entries = await LoadSortedSetEntriesAsync(connection, key, reverse, transaction: null, token).ConfigureAwait(false);
        return new SortedSetRangeResult(SortedSetResultStatus.Ok, SliceSortedSet(entries, start, stop));
    }

    private async Task<SortedSetRankResult> GetSortedSetRankAsync(
        string key,
        byte[] member,
        bool reverse,
        CancellationToken token)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(member);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new SortedSetRankResult(SortedSetResultStatus.Ok, null);
        }

        if (existing.Kind != SortedSetKind)
        {
            return new SortedSetRankResult(SortedSetResultStatus.WrongType, null);
        }

        var entries = await LoadSortedSetEntriesAsync(connection, key, reverse, transaction: null, token).ConfigureAwait(false);
        for (var i = 0; i < entries.Count; i++)
        {
            if (entries[i].Member.AsSpan().SequenceEqual(member))
            {
                return new SortedSetRankResult(SortedSetResultStatus.Ok, i);
            }
        }

        return new SortedSetRankResult(SortedSetResultStatus.Ok, null);
    }

    private async Task<List<SortedSetEntry>> LoadSortedSetEntriesAsync(
        NpgsqlConnection connection,
        string key,
        bool reverse,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        var orderDirection = reverse ? "DESC" : "ASC";
        var entries = new List<SortedSetEntry>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT member, score
            FROM {_qualifiedSortedSetTableName}
            WHERE key = @key
            ORDER BY score {orderDirection}, member {orderDirection};
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            entries.Add(new SortedSetEntry(reader.GetFieldValue<byte[]>(0), reader.GetDouble(1)));
        }

        return entries;
    }

    private static SortedSetEntry[] SliceSortedSet(IReadOnlyList<SortedSetEntry> entries, int start, int stop)
    {
        if (entries.Count == 0)
        {
            return [];
        }

        var normalizedStart = start < 0 ? entries.Count + start : start;
        var normalizedStop = stop < 0 ? entries.Count + stop : stop;
        normalizedStart = Math.Max(0, normalizedStart);
        normalizedStop = Math.Min(entries.Count - 1, normalizedStop);

        if (normalizedStart >= entries.Count || normalizedStart > normalizedStop)
        {
            return [];
        }

        var length = normalizedStop - normalizedStart + 1;
        var slice = new SortedSetEntry[length];
        for (var i = 0; i < length; i++)
        {
            slice[i] = entries[normalizedStart + i];
        }

        return slice;
    }
}
