using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Dredis.Abstractions.Storage;
using Npgsql;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore : IKeyValueStore, IDisposable, IAsyncDisposable
{
    private const string DefaultTableName = "dredis_key_values";
    private const string StringKind = "string";
    private const string HashKind = "hash";
    private const string ListKind = "list";
    private const string SetKind = "set";
    private const string SortedSetKind = "sorted-set";
    private static readonly Regex IdentifierPartPattern = new("^[A-Za-z_][A-Za-z0-9_]*$", RegexOptions.Compiled);

    private readonly NpgsqlDataSource _dataSource;
    private readonly bool _ownsDataSource;
    private readonly SemaphoreSlim _schemaLock = new(1, 1);
    private readonly string _qualifiedTableName;
    private readonly string _qualifiedHashTableName;
    private readonly string _qualifiedListTableName;
    private readonly string _qualifiedSetTableName;
    private readonly string _qualifiedSortedSetTableName;
    private readonly string _expiryIndexName;

    private bool _schemaEnsured;
    private bool _disposed;

    public PostgresKeyValueStore(string connectionString, string tableName = DefaultTableName)
        : this(CreateDataSource(connectionString), tableName, ownsDataSource: true)
    {
    }

    public PostgresKeyValueStore(NpgsqlDataSource dataSource, string tableName = DefaultTableName)
        : this(dataSource, tableName, ownsDataSource: false)
    {
    }

    private PostgresKeyValueStore(NpgsqlDataSource dataSource, string tableName, bool ownsDataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _ownsDataSource = ownsDataSource;

        var identifierParts = ParseIdentifierParts(tableName);
        _qualifiedTableName = string.Join(".", identifierParts.Select(QuoteIdentifier));
        _qualifiedHashTableName = BuildQualifiedObjectName(identifierParts, "_hash_entries");
        _qualifiedListTableName = BuildQualifiedObjectName(identifierParts, "_list_items");
        _qualifiedSetTableName = BuildQualifiedObjectName(identifierParts, "_set_members");
        _qualifiedSortedSetTableName = BuildQualifiedObjectName(identifierParts, "_sorted_set_members");
        _expiryIndexName = QuoteIdentifier(BuildIndexName(identifierParts, "expires_at_idx"));
    }

    public async Task<long> CleanUpExpiredKeysAsync(CancellationToken token = default)
    {
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedTableName}
            WHERE expires_at IS NOT NULL
              AND expires_at <= NOW();
            """);

        return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT value
            FROM {_qualifiedTableName}
            WHERE key = @key
              AND kind = @kind
              AND (expires_at IS NULL OR expires_at > NOW())
            LIMIT 1;
            """);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("kind", StringKind);

        var result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        return result is DBNull or null ? null : (byte[])result;
    }

    public async Task<bool> SetAsync(
        string key,
        byte[] value,
        TimeSpan? expiration,
        SetCondition condition,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(value);

        if (expiration is { } ttl && ttl <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(expiration), "Expiration must be greater than zero.");
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);

        if (condition == SetCondition.Nx && existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return false;
        }

        if (condition == SetCondition.Xx && !existing.Exists)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return false;
        }

        if (existing.Exists && existing.Kind != StringKind)
        {
            await DeleteByKeyAsync(connection, key, transaction, token).ConfigureAwait(false);
        }

        await UpsertKeyMetadataAsync(
            connection,
            key,
            StringKind,
            value,
            expiration is null ? null : DateTimeOffset.UtcNow.Add(expiration.Value),
            transaction,
            token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return true;
    }

    public async Task<byte[]?[]> GetManyAsync(string[] keys, CancellationToken token = default)
    {
        ValidateKeys(keys);

        if (keys.Length == 0)
        {
            return [];
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT input.ordinality, store.value
            FROM unnest(@keys) WITH ORDINALITY AS input(key, ordinality)
            LEFT JOIN {_qualifiedTableName} AS store
              ON store.key = input.key
             AND store.kind = @kind
             AND (store.expires_at IS NULL OR store.expires_at > NOW())
            ORDER BY input.ordinality;
            """);

        command.Parameters.AddWithValue("keys", keys);
        command.Parameters.AddWithValue("kind", StringKind);

        var results = new byte[]?[keys.Length];
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);

        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var ordinal = checked((int)reader.GetInt64(0) - 1);
            results[ordinal] = reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1);
        }

        return results;
    }

    public async Task<bool> SetManyAsync(KeyValuePair<string, byte[]>[] items, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(items);

        foreach (var item in items)
        {
            ValidateKey(item.Key);
            ArgumentNullException.ThrowIfNull(item.Value, nameof(items));
        }

        if (items.Length == 0)
        {
            return true;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        foreach (var item in items)
        {
            var existing = await TryGetLiveEntryAsync(connection, item.Key, transaction, token).ConfigureAwait(false);
            if (existing.Exists && existing.Kind != StringKind)
            {
                await DeleteByKeyAsync(connection, item.Key, transaction, token).ConfigureAwait(false);
            }

            await UpsertKeyMetadataAsync(
                connection,
                item.Key,
                StringKind,
                item.Value,
                expiresAt: null,
                transaction,
                token).ConfigureAwait(false);
        }

        await transaction.CommitAsync(token).ConfigureAwait(false);
        return true;
    }

    public async Task<long> DeleteAsync(string[] keys, CancellationToken token = default)
    {
        ValidateKeys(keys);

        if (keys.Length == 0)
        {
            return 0;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedTableName}
            WHERE key = ANY(@keys);
            """);

        command.Parameters.AddWithValue("keys", keys);
        return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    public async Task<bool> ExistsAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT EXISTS (
                SELECT 1
                FROM {_qualifiedTableName}
                WHERE key = @key
                  AND (expires_at IS NULL OR expires_at > NOW())
            );
            """);

        command.Parameters.AddWithValue("key", key);
        return (bool)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? false);
    }

    public async Task<long> ExistsAsync(string[] keys, CancellationToken token = default)
    {
        ValidateKeys(keys);

        if (keys.Length == 0)
        {
            return 0;
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT COUNT(*)
            FROM unnest(@keys) AS input(key)
            JOIN {_qualifiedTableName} AS store
              ON store.key = input.key
             AND (store.expires_at IS NULL OR store.expires_at > NOW());
            """);

        command.Parameters.AddWithValue("keys", keys);
        return (long)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? 0L);
    }

    public async Task<long?> IncrByAsync(string key, long delta, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        long nextValue;
        DateTimeOffset? expiresAt = existing.ExpiresAt;

        if (!existing.Exists)
        {
            nextValue = delta;
        }
        else if (existing.Kind != StringKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return null;
        }
        else if (!TryParseInt64(existing.Value!, out var currentValue))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return null;
        }
        else
        {
            try
            {
                nextValue = checked(currentValue + delta);
            }
            catch (OverflowException)
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return null;
            }
        }

        await UpsertKeyMetadataAsync(
            connection,
            key,
            StringKind,
            Encoding.UTF8.GetBytes(nextValue.ToString(CultureInfo.InvariantCulture)),
            expiresAt,
            transaction,
            token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return nextValue;
    }

    public Task<bool> ExpireAsync(string key, TimeSpan expiration, CancellationToken token = default) =>
        SetExpiryAsync(key, expiration, token);

    public Task<bool> PExpireAsync(string key, TimeSpan expiration, CancellationToken token = default) =>
        SetExpiryAsync(key, expiration, token);

    public Task<long> TtlAsync(string key, CancellationToken token = default) =>
        GetTimeToLiveAsync(key, returnMilliseconds: false, token);

    public Task<long> PttlAsync(string key, CancellationToken token = default) =>
        GetTimeToLiveAsync(key, returnMilliseconds: true, token);

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        if (_ownsDataSource)
        {
            _dataSource.Dispose();
        }

        _schemaLock.Dispose();
        _disposed = true;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        if (_ownsDataSource)
        {
            await _dataSource.DisposeAsync().ConfigureAwait(false);
        }

        _schemaLock.Dispose();
        _disposed = true;
    }

    private async Task<bool> SetExpiryAsync(string key, TimeSpan expiration, CancellationToken token)
    {
        ValidateKey(key);

        if (expiration <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(expiration), "Expiration must be greater than zero.");
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            UPDATE {_qualifiedTableName}
            SET expires_at = NOW() + @expiration
            WHERE key = @key
              AND (expires_at IS NULL OR expires_at > NOW());
            """);

        command.Parameters.AddWithValue("expiration", expiration);
        command.Parameters.AddWithValue("key", key);
        return await command.ExecuteNonQueryAsync(token).ConfigureAwait(false) > 0;
    }

    private async Task<long> GetTimeToLiveAsync(string key, bool returnMilliseconds, CancellationToken token)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT expires_at
            FROM {_qualifiedTableName}
            WHERE key = @key
            LIMIT 1;
            """);

        command.Parameters.AddWithValue("key", key);

        var result = await command.ExecuteScalarAsync(token).ConfigureAwait(false);
        if (result is null or DBNull)
        {
            var exists = await ExistsRegardlessOfExpiryAsync(connection, key, token).ConfigureAwait(false);
            return exists ? -1 : -2;
        }

        var expiresAt = ((DateTimeOffset)result).ToUniversalTime();
        var remaining = expiresAt - DateTimeOffset.UtcNow;

        if (remaining <= TimeSpan.Zero)
        {
            await DeleteIfExpiredAsync(connection, key, token).ConfigureAwait(false);
            return -2;
        }

        var ttl = returnMilliseconds
            ? Math.Floor(remaining.TotalMilliseconds)
            : Math.Floor(remaining.TotalSeconds);

        return (long)Math.Max(0, ttl);
    }

    private async Task EnsureSchemaAsync(CancellationToken token)
    {
        if (_schemaEnsured)
        {
            return;
        }

        await _schemaLock.WaitAsync(token).ConfigureAwait(false);
        try
        {
            if (_schemaEnsured)
            {
                return;
            }

            await using var connection = await _dataSource.OpenConnectionAsync(token).ConfigureAwait(false);
            await using var command = CreateCommand(
                connection,
                $"""
                CREATE TABLE IF NOT EXISTS {_qualifiedTableName}
                (
                    key text PRIMARY KEY,
                    kind text NOT NULL DEFAULT '{StringKind}',
                    value bytea NULL,
                    expires_at timestamptz NULL
                );

                ALTER TABLE {_qualifiedTableName}
                    ADD COLUMN IF NOT EXISTS kind text;

                ALTER TABLE {_qualifiedTableName}
                    ALTER COLUMN kind SET DEFAULT '{StringKind}';

                UPDATE {_qualifiedTableName}
                SET kind = '{StringKind}'
                WHERE kind IS NULL;

                ALTER TABLE {_qualifiedTableName}
                    ALTER COLUMN kind SET NOT NULL;

                ALTER TABLE {_qualifiedTableName}
                    ALTER COLUMN value DROP NOT NULL;

                CREATE INDEX IF NOT EXISTS {_expiryIndexName}
                    ON {_qualifiedTableName} (expires_at)
                    WHERE expires_at IS NOT NULL;

                CREATE TABLE IF NOT EXISTS {_qualifiedHashTableName}
                (
                    key text NOT NULL REFERENCES {_qualifiedTableName}(key) ON DELETE CASCADE,
                    field text NOT NULL,
                    value bytea NOT NULL,
                    PRIMARY KEY (key, field)
                );

                CREATE TABLE IF NOT EXISTS {_qualifiedListTableName}
                (
                    key text NOT NULL REFERENCES {_qualifiedTableName}(key) ON DELETE CASCADE,
                    position bigint NOT NULL,
                    value bytea NOT NULL,
                    PRIMARY KEY (key, position)
                );

                CREATE TABLE IF NOT EXISTS {_qualifiedSetTableName}
                (
                    key text NOT NULL REFERENCES {_qualifiedTableName}(key) ON DELETE CASCADE,
                    member bytea NOT NULL,
                    PRIMARY KEY (key, member)
                );

                CREATE TABLE IF NOT EXISTS {_qualifiedSortedSetTableName}
                (
                    key text NOT NULL REFERENCES {_qualifiedTableName}(key) ON DELETE CASCADE,
                    member bytea NOT NULL,
                    score double precision NOT NULL,
                    PRIMARY KEY (key, member)
                );
                """);

            await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
            _schemaEnsured = true;
        }
        finally
        {
            _schemaLock.Release();
        }
    }

    private async Task<NpgsqlConnection> OpenConnectionAsync(CancellationToken token)
    {
        ThrowIfDisposed();
        await EnsureSchemaAsync(token).ConfigureAwait(false);
        return await _dataSource.OpenConnectionAsync(token).ConfigureAwait(false);
    }

    private static NpgsqlDataSource CreateDataSource(string connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("Connection string cannot be null or whitespace.", nameof(connectionString));
        }

        return NpgsqlDataSource.Create(connectionString);
    }

    private static string[] ParseIdentifierParts(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name cannot be null or whitespace.", nameof(tableName));
        }

        var parts = tableName.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0 || parts.Any(static part => !IdentifierPartPattern.IsMatch(part)))
        {
            throw new ArgumentException(
                "Table name must use unquoted PostgreSQL identifiers containing only letters, digits, and underscores.",
                nameof(tableName));
        }

        return parts;
    }

    private static string QuoteIdentifier(string identifierPart) => $"\"{identifierPart}\"";

    private static string BuildQualifiedObjectName(IReadOnlyList<string> identifierParts, string suffix)
    {
        var childParts = identifierParts.ToArray();
        childParts[^1] = childParts[^1] + suffix;
        return string.Join(".", childParts.Select(QuoteIdentifier));
    }

    private static string BuildIndexName(IEnumerable<string> identifierParts, string suffix)
    {
        var prefix = string.Join("_", identifierParts).ToLowerInvariant();
        if (prefix.Length > 40)
        {
            prefix = prefix[..40];
        }

        return $"{prefix}_{suffix}";
    }

    private static void ValidateKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Key cannot be null or whitespace.", nameof(key));
        }
    }

    private static void ValidateKeys(string[] keys)
    {
        ArgumentNullException.ThrowIfNull(keys);

        foreach (var key in keys)
        {
            ValidateKey(key);
        }
    }

    private static bool TryParseInt64(byte[] valueBytes, out long value)
    {
        var text = Encoding.UTF8.GetString(valueBytes);
        return long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value);
    }

    private static NpgsqlCommand CreateCommand(NpgsqlConnection connection, string commandText, NpgsqlTransaction? transaction = null)
    {
        var command = connection.CreateCommand();
        command.CommandText = commandText;
        command.Transaction = transaction;
        return command;
    }

    private async Task<(bool Exists, string? Kind, byte[]? Value, DateTimeOffset? ExpiresAt)> TryGetLiveEntryAsync(
        NpgsqlConnection connection,
        string key,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT kind, value, expires_at
            FROM {_qualifiedTableName}
            WHERE key = @key
            FOR UPDATE;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);

        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        if (!await reader.ReadAsync(token).ConfigureAwait(false))
        {
            return (false, null, null, null);
        }

        var kind = reader.GetString(0);
        var value = reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1);
        var expiresAt = reader.IsDBNull(2) ? (DateTimeOffset?)null : reader.GetFieldValue<DateTimeOffset>(2);
        await reader.DisposeAsync().ConfigureAwait(false);

        if (expiresAt is { } actualExpiry && actualExpiry <= DateTimeOffset.UtcNow)
        {
            await DeleteByKeyAsync(connection, key, transaction, token).ConfigureAwait(false);
            return (false, null, null, null);
        }

        return (true, kind, value, expiresAt);
    }

    private async Task UpsertKeyMetadataAsync(
        NpgsqlConnection connection,
        string key,
        string kind,
        byte[]? value,
        DateTimeOffset? expiresAt,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            INSERT INTO {_qualifiedTableName} (key, kind, value, expires_at)
            VALUES (@key, @kind, @value, @expiresAt)
            ON CONFLICT (key) DO UPDATE
            SET kind = EXCLUDED.kind,
                value = EXCLUDED.value,
                expires_at = EXCLUDED.expires_at;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("kind", kind);
        command.Parameters.AddWithValue("value", value is null ? DBNull.Value : value);
        command.Parameters.AddWithValue("expiresAt", expiresAt is null ? DBNull.Value : expiresAt.Value);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private async Task DeleteByKeyAsync(
        NpgsqlConnection connection,
        string key,
        NpgsqlTransaction? transaction,
        CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedTableName}
            WHERE key = @key;
            """,
            transaction);

        command.Parameters.AddWithValue("key", key);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private static InvalidOperationException CreateWrongTypeException() =>
        new("WRONGTYPE Operation against a key holding the wrong kind of value");

    private async Task<bool> ExistsRegardlessOfExpiryAsync(NpgsqlConnection connection, string key, CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT EXISTS (
                SELECT 1
                FROM {_qualifiedTableName}
                WHERE key = @key
            );
            """);

        command.Parameters.AddWithValue("key", key);
        return (bool)(await command.ExecuteScalarAsync(token).ConfigureAwait(false) ?? false);
    }

    private async Task DeleteIfExpiredAsync(NpgsqlConnection connection, string key, CancellationToken token)
    {
        await using var command = CreateCommand(
            connection,
            $"""
            DELETE FROM {_qualifiedTableName}
            WHERE key = @key
              AND expires_at IS NOT NULL
              AND expires_at <= NOW();
            """);

        command.Parameters.AddWithValue("key", key);
        await command.ExecuteNonQueryAsync(token).ConfigureAwait(false);
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}
