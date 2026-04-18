using Dredis.Abstractions.Storage;
using Npgsql;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore
{
    public async Task<bool> HashSetAsync(string key, string field, byte[] value, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(field);
        ArgumentNullException.ThrowIfNull(value);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != HashKind)
        {
            throw CreateWrongTypeException();
        }

        if (!existing.Exists)
        {
            await UpsertKeyMetadataAsync(connection, key, HashKind, value: null, expiresAt: null, transaction, token).ConfigureAwait(false);
        }

        await using var insertCommand = CreateCommand(
            connection,
            $"""
            INSERT INTO {_qualifiedHashTableName} (key, field, value)
            VALUES (@key, @field, @value)
            ON CONFLICT DO NOTHING;
            """,
            transaction);

        insertCommand.Parameters.AddWithValue("key", key);
        insertCommand.Parameters.AddWithValue("field", field);
        insertCommand.Parameters.AddWithValue("value", value);

        var inserted = await insertCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false) > 0;
        if (!inserted)
        {
            await using var updateCommand = CreateCommand(
                connection,
                $"""
                UPDATE {_qualifiedHashTableName}
                SET value = @value
                WHERE key = @key
                  AND field = @field;
                """,
                transaction);

            updateCommand.Parameters.AddWithValue("key", key);
            updateCommand.Parameters.AddWithValue("field", field);
            updateCommand.Parameters.AddWithValue("value", value);
            await updateCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return inserted;
    }

    public async Task<byte[]?> HashGetAsync(string key, string field, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(field);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return null;
        }

        if (existing.Kind != HashKind)
        {
            throw CreateWrongTypeException();
        }

        await using var command = CreateCommand(
            connection,
            $"""
            SELECT value
            FROM {_qualifiedHashTableName}
            WHERE key = @key
              AND field = @field
            LIMIT 1;
            """);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("field", field);

        var result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        return result is null or DBNull ? null : (byte[])result;
    }

    public async Task<long> HashDeleteAsync(string key, string[] fields, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(fields);

        foreach (var field in fields)
        {
            ArgumentNullException.ThrowIfNull(field);
        }

        if (fields.Length == 0)
        {
            return 0;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return 0;
        }

        if (existing.Kind != HashKind)
        {
            throw CreateWrongTypeException();
        }

        long removed = 0;
        foreach (var field in fields)
        {
            await using var command = CreateCommand(
                connection,
                $"""
                DELETE FROM {_qualifiedHashTableName}
                WHERE key = @key
                  AND field = @field;
                """,
                transaction);

            command.Parameters.AddWithValue("key", key);
            command.Parameters.AddWithValue("field", field);
            removed += await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        if (removed > 0)
        {
            await DeleteParentIfChildTableEmptyAsync(connection, _qualifiedHashTableName, key, transaction, token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return removed;
    }

    public async Task<KeyValuePair<string, byte[]>[]> HashGetAllAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return [];
        }

        if (existing.Kind != HashKind)
        {
            throw CreateWrongTypeException();
        }

        var results = new List<KeyValuePair<string, byte[]>>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT field, value
            FROM {_qualifiedHashTableName}
            WHERE key = @key
            ORDER BY field;
            """);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            results.Add(new KeyValuePair<string, byte[]>(reader.GetString(0), reader.GetFieldValue<byte[]>(1)));
        }

        return [.. results];
    }

    public async Task<ListPushResult> ListPushAsync(string key, byte[][] values, bool left, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(values);
        foreach (var value in values)
        {
            ArgumentNullException.ThrowIfNull(value);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != ListKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ListPushResult(ListResultStatus.WrongType, 0);
        }

        if (!existing.Exists)
        {
            await UpsertKeyMetadataAsync(connection, key, ListKind, value: null, expiresAt: null, transaction, token).ConfigureAwait(false);
        }

        var items = await LoadListValuesAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (left)
        {
            foreach (var value in values)
            {
                items.Insert(0, value);
            }
        }
        else
        {
            items.AddRange(values);
        }

        await SaveListValuesAsync(connection, key, items, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new ListPushResult(ListResultStatus.Ok, items.Count);
    }

    public async Task<ListPopResult> ListPopAsync(string key, bool left, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new ListPopResult(ListResultStatus.Ok, null);
        }

        if (existing.Kind != ListKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ListPopResult(ListResultStatus.WrongType, null);
        }

        var items = await LoadListValuesAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (items.Count == 0)
        {
            await DeleteByKeyAsync(connection, key, transaction, token).ConfigureAwait(false);
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new ListPopResult(ListResultStatus.Ok, null);
        }

        var index = left ? 0 : items.Count - 1;
        var value = items[index];
        items.RemoveAt(index);

        await SaveListValuesAsync(connection, key, items, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new ListPopResult(ListResultStatus.Ok, value);
    }

    public async Task<ListRangeResult> ListRangeAsync(string key, int start, int stop, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new ListRangeResult(ListResultStatus.Ok, []);
        }

        if (existing.Kind != ListKind)
        {
            return new ListRangeResult(ListResultStatus.WrongType, []);
        }

        var items = await LoadListValuesAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        return new ListRangeResult(ListResultStatus.Ok, SliceList(items, start, stop));
    }

    public async Task<ListLengthResult> ListLengthAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new ListLengthResult(ListResultStatus.Ok, 0);
        }

        if (existing.Kind != ListKind)
        {
            return new ListLengthResult(ListResultStatus.WrongType, 0);
        }

        await using var command = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*)
            FROM {_qualifiedListTableName}
            WHERE key = @key;
            """);

        command.Parameters.AddWithValue("key", key);
        var length = (long)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);
        return new ListLengthResult(ListResultStatus.Ok, length);
    }

    public async Task<ListIndexResult> ListIndexAsync(string key, int index, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new ListIndexResult(ListResultStatus.Ok, null);
        }

        if (existing.Kind != ListKind)
        {
            return new ListIndexResult(ListResultStatus.WrongType, null);
        }

        var items = await LoadListValuesAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        var normalized = NormalizeIndex(index, items.Count);
        return new ListIndexResult(ListResultStatus.Ok, normalized is null ? null : items[normalized.Value]);
    }

    public async Task<ListSetResult> ListSetAsync(string key, int index, byte[] value, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(value);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ListSetResult(ListSetResultStatus.OutOfRange);
        }

        if (existing.Kind != ListKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ListSetResult(ListSetResultStatus.WrongType);
        }

        var items = await LoadListValuesAsync(connection, key, transaction, token).ConfigureAwait(false);
        var normalized = NormalizeIndex(index, items.Count);
        if (normalized is null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new ListSetResult(ListSetResultStatus.OutOfRange);
        }

        items[normalized.Value] = value;
        await SaveListValuesAsync(connection, key, items, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new ListSetResult(ListSetResultStatus.Ok);
    }

    public async Task<ListResultStatus> ListTrimAsync(string key, int start, int stop, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return ListResultStatus.Ok;
        }

        if (existing.Kind != ListKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return ListResultStatus.WrongType;
        }

        var items = await LoadListValuesAsync(connection, key, transaction, token).ConfigureAwait(false);
        await SaveListValuesAsync(connection, key, [.. SliceList(items, start, stop)], transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return ListResultStatus.Ok;
    }

    public async Task<SetCountResult> SetAddAsync(string key, byte[][] members, CancellationToken token = default)
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
        if (existing.Exists && existing.Kind != SetKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new SetCountResult(SetResultStatus.WrongType, 0);
        }

        if (!existing.Exists)
        {
            await UpsertKeyMetadataAsync(connection, key, SetKind, value: null, expiresAt: null, transaction, token).ConfigureAwait(false);
        }

        long added = 0;
        foreach (var member in members)
        {
            await using var command = CreateCommand(
                connection,
                $"""
                INSERT INTO {_qualifiedSetTableName} (key, member)
                VALUES (@key, @member)
                ON CONFLICT DO NOTHING;
                """,
                transaction);

            command.Parameters.AddWithValue("key", key);
            command.Parameters.AddWithValue("member", member);
            added += await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new SetCountResult(SetResultStatus.Ok, added);
    }

    public async Task<SetCountResult> SetRemoveAsync(string key, byte[][] members, CancellationToken token = default)
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
            return new SetCountResult(SetResultStatus.Ok, 0);
        }

        if (existing.Kind != SetKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new SetCountResult(SetResultStatus.WrongType, 0);
        }

        long removed = 0;
        foreach (var member in members)
        {
            await using var command = CreateCommand(
                connection,
                $"""
                DELETE FROM {_qualifiedSetTableName}
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
            await DeleteParentIfChildTableEmptyAsync(connection, _qualifiedSetTableName, key, transaction, token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new SetCountResult(SetResultStatus.Ok, removed);
    }

    public async Task<SetMembersResult> SetMembersAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new SetMembersResult(SetResultStatus.Ok, []);
        }

        if (existing.Kind != SetKind)
        {
            return new SetMembersResult(SetResultStatus.WrongType, []);
        }

        var members = new List<byte[]>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT member
            FROM {_qualifiedSetTableName}
            WHERE key = @key
            ORDER BY encode(member, 'hex');
            """);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            members.Add(reader.GetFieldValue<byte[]>(0));
        }

        return new SetMembersResult(SetResultStatus.Ok, [.. members]);
    }

    public async Task<SetCountResult> SetCardinalityAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new SetCountResult(SetResultStatus.Ok, 0);
        }

        if (existing.Kind != SetKind)
        {
            return new SetCountResult(SetResultStatus.WrongType, 0);
        }

        await using var command = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*)
            FROM {_qualifiedSetTableName}
            WHERE key = @key;
            """);

        command.Parameters.AddWithValue("key", key);
        var count = (long)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);
        return new SetCountResult(SetResultStatus.Ok, count);
    }

    private async Task<List<byte[]>> LoadListValuesAsync(
        NpgsqlConnection connection,
        string key,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        var items = new List<byte[]>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT value
            FROM {_qualifiedListTableName}
            WHERE key = @key
            ORDER BY position;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            items.Add(reader.GetFieldValue<byte[]>(0));
        }

        return items;
    }

    private async Task SaveListValuesAsync(
        NpgsqlConnection connection,
        string key,
        IReadOnlyList<byte[]> items,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        if (items.Count == 0)
        {
            await DeleteByKeyAsync(connection, key, transaction, token).ConfigureAwait(false);
            return;
        }

        await using var deleteCommand = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedListTableName}
            WHERE key = @key;
            """,
            transaction);

        deleteCommand.Parameters.AddWithValue("key", key);
        await deleteCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);

        for (var i = 0; i < items.Count; i++)
        {
            await using var insertCommand = CreateCommand(
                connection,
                $"""
                INSERT INTO {_qualifiedListTableName} (key, position, value)
                VALUES (@key, @position, @value);
                """,
                transaction);

            insertCommand.Parameters.AddWithValue("key", key);
            insertCommand.Parameters.AddWithValue("position", (long)i);
            insertCommand.Parameters.AddWithValue("value", items[i]);
            await insertCommand.ExecuteNonQueryAsync(token).ConfigureAwait(false);
        }
    }

    private static byte[][] SliceList(IReadOnlyList<byte[]> items, int start, int stop)
    {
        if (items.Count == 0)
        {
            return [];
        }

        var normalizedStart = start < 0 ? items.Count + start : start;
        var normalizedStop = stop < 0 ? items.Count + stop : stop;

        normalizedStart = Math.Max(0, normalizedStart);
        normalizedStop = Math.Min(items.Count - 1, normalizedStop);

        if (normalizedStart >= items.Count || normalizedStart > normalizedStop)
        {
            return [];
        }

        var length = normalizedStop - normalizedStart + 1;
        var slice = new byte[length][];
        for (var i = 0; i < length; i++)
        {
            slice[i] = items[normalizedStart + i];
        }

        return slice;
    }

    private static int? NormalizeIndex(int index, int count)
    {
        var normalized = index < 0 ? count + index : index;
        return normalized < 0 || normalized >= count ? null : normalized;
    }

    private async Task DeleteParentIfChildTableEmptyAsync(
        NpgsqlConnection connection,
        string childTableName,
        string key,
        NpgsqlTransaction transaction,
        CancellationToken token)
    {
        await using var countCommand = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*)
            FROM {childTableName}
            WHERE key = @key;
            """,
            transaction);

        countCommand.Parameters.AddWithValue("key", key);
        var remaining = (long)(await countCommand.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);
        if (remaining == 0)
        {
            await DeleteByKeyAsync(connection, key, transaction, token).ConfigureAwait(false);
        }
    }
}
