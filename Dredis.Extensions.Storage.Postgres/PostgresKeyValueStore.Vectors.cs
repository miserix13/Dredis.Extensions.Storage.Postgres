using System.Text;
using System.Text.Json;
using Dredis.Abstractions.Storage;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore
{
    public async Task<VectorSetResult> VectorSetAsync(string key, double[] vector, CancellationToken token = default)
    {
        ValidateKey(key);
        if (!IsValidVector(vector))
        {
            return new VectorSetResult(VectorResultStatus.InvalidArgument);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != VectorKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new VectorSetResult(VectorResultStatus.WrongType);
        }

        await UpsertKeyMetadataAsync(
            connection,
            key,
            VectorKind,
            SerializeVector(vector),
            existing.ExpiresAt,
            transaction,
            token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new VectorSetResult(VectorResultStatus.Ok);
    }

    public async Task<VectorGetResult> VectorGetAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return new VectorGetResult(VectorResultStatus.NotFound, vector: null);
        }

        if (existing.Kind != VectorKind)
        {
            return new VectorGetResult(VectorResultStatus.WrongType, vector: null);
        }

        if (!TryDeserializeVector(existing.Value, out var vector))
        {
            return new VectorGetResult(VectorResultStatus.InvalidArgument, vector: null);
        }

        return new VectorGetResult(VectorResultStatus.Ok, vector);
    }

    public async Task<VectorSizeResult> VectorSizeAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        var result = await VectorGetAsync(key, token).ConfigureAwait(false);
        return result.Status switch
        {
            VectorResultStatus.Ok => new VectorSizeResult(VectorResultStatus.Ok, result.Vector!.LongLength),
            VectorResultStatus.WrongType => new VectorSizeResult(VectorResultStatus.WrongType, 0),
            VectorResultStatus.NotFound => new VectorSizeResult(VectorResultStatus.NotFound, 0),
            _ => new VectorSizeResult(VectorResultStatus.InvalidArgument, 0)
        };
    }

    public async Task<VectorSimilarityResult> VectorSimilarityAsync(
        string key,
        string otherKey,
        string metric,
        CancellationToken token = default)
    {
        ValidateKey(key);
        ValidateKey(otherKey);
        ArgumentNullException.ThrowIfNull(metric);

        var left = await VectorGetAsync(key, token).ConfigureAwait(false);
        if (left.Status != VectorResultStatus.Ok)
        {
            return new VectorSimilarityResult(left.Status, value: null);
        }

        var right = await VectorGetAsync(otherKey, token).ConfigureAwait(false);
        if (right.Status != VectorResultStatus.Ok)
        {
            return new VectorSimilarityResult(right.Status, value: null);
        }

        if (!TryComputeVectorMetric(left.Vector!, right.Vector!, metric, out var value))
        {
            return new VectorSimilarityResult(VectorResultStatus.InvalidArgument, value: null);
        }

        return new VectorSimilarityResult(VectorResultStatus.Ok, value);
    }

    public async Task<VectorDeleteResult> VectorDeleteAsync(string key, CancellationToken token = default)
    {
        ValidateKey(key);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new VectorDeleteResult(VectorResultStatus.NotFound, 0);
        }

        if (existing.Kind != VectorKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new VectorDeleteResult(VectorResultStatus.WrongType, 0);
        }

        await DeleteByKeyAsync(connection, key, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new VectorDeleteResult(VectorResultStatus.Ok, 1);
    }

    public async Task<VectorSearchResult> VectorSearchAsync(
        string keyPrefix,
        int topK,
        int offset,
        string metric,
        double[] queryVector,
        CancellationToken token = default)
    {
        if (string.IsNullOrEmpty(keyPrefix))
        {
            return new VectorSearchResult(VectorResultStatus.InvalidArgument, []);
        }

        if (topK <= 0 || offset < 0 || !IsValidVector(queryVector))
        {
            return new VectorSearchResult(VectorResultStatus.InvalidArgument, []);
        }

        ArgumentNullException.ThrowIfNull(metric);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var candidates = new List<(string Key, double[] Vector)>();
        await using var command = CreateCommand(
            connection,
            $"""
            SELECT key, kind, value
            FROM {_qualifiedTableName}
            WHERE key LIKE @pattern ESCAPE '\'
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY key;
            """);

        command.Parameters.AddWithValue("pattern", EscapeLikePattern(keyPrefix) + "%");
        await using var reader = await command.ExecuteReaderAsync(token).ConfigureAwait(false);
        while (await reader.ReadAsync(token).ConfigureAwait(false))
        {
            var key = reader.GetString(0);
            if (!key.StartsWith(keyPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            var kind = reader.GetString(1);
            if (kind != VectorKind)
            {
                return new VectorSearchResult(VectorResultStatus.WrongType, []);
            }

            if (!TryDeserializeVector(reader.IsDBNull(2) ? null : reader.GetFieldValue<byte[]>(2), out var vector))
            {
                return new VectorSearchResult(VectorResultStatus.InvalidArgument, []);
            }

            candidates.Add((key, vector));
        }

        var matches = new List<VectorSearchEntry>(candidates.Count);
        foreach (var candidate in candidates)
        {
            if (!TryComputeVectorMetric(queryVector, candidate.Vector, metric, out var score))
            {
                return new VectorSearchResult(VectorResultStatus.InvalidArgument, []);
            }

            matches.Add(new VectorSearchEntry(candidate.Key, score));
        }

        var ordered = OrderVectorSearchResults(matches, metric)
            .Skip(offset)
            .Take(topK)
            .ToArray();

        return new VectorSearchResult(VectorResultStatus.Ok, ordered);
    }

    private static byte[] SerializeVector(double[] vector) =>
        JsonSerializer.SerializeToUtf8Bytes(vector);

    private static bool TryDeserializeVector(byte[]? bytes, out double[] vector)
    {
        vector = [];
        if (bytes is null)
        {
            return false;
        }

        try
        {
            vector = JsonSerializer.Deserialize<double[]>(bytes) ?? [];
            return IsValidVector(vector);
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool IsValidVector(double[]? vector)
    {
        if (vector is null || vector.Length == 0)
        {
            return false;
        }

        foreach (var component in vector)
        {
            if (double.IsNaN(component) || double.IsInfinity(component))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryComputeVectorMetric(double[] left, double[] right, string metric, out double value)
    {
        value = default;
        if (left.Length != right.Length || left.Length == 0)
        {
            return false;
        }

        if (metric.Equals("DOT", StringComparison.OrdinalIgnoreCase))
        {
            value = ComputeDot(left, right);
            return true;
        }

        if (metric.Equals("COSINE", StringComparison.OrdinalIgnoreCase))
        {
            var leftMagnitude = Math.Sqrt(ComputeDot(left, left));
            var rightMagnitude = Math.Sqrt(ComputeDot(right, right));
            if (leftMagnitude == 0 || rightMagnitude == 0)
            {
                return false;
            }

            value = ComputeDot(left, right) / (leftMagnitude * rightMagnitude);
            return true;
        }

        if (metric.Equals("L2", StringComparison.OrdinalIgnoreCase))
        {
            var total = 0d;
            for (var i = 0; i < left.Length; i++)
            {
                var delta = left[i] - right[i];
                total += delta * delta;
            }

            value = Math.Sqrt(total);
            return true;
        }

        return false;
    }

    private static double ComputeDot(IReadOnlyList<double> left, IReadOnlyList<double> right)
    {
        var total = 0d;
        for (var i = 0; i < left.Count; i++)
        {
            total += left[i] * right[i];
        }

        return total;
    }

    private static IEnumerable<VectorSearchEntry> OrderVectorSearchResults(
        IEnumerable<VectorSearchEntry> entries,
        string metric)
    {
        return metric.Equals("L2", StringComparison.OrdinalIgnoreCase)
            ? entries.OrderBy(static entry => entry.Score).ThenBy(static entry => entry.Key, StringComparer.Ordinal)
            : entries.OrderByDescending(static entry => entry.Score).ThenBy(static entry => entry.Key, StringComparer.Ordinal);
    }

    private static string EscapeLikePattern(string value)
    {
        var builder = new StringBuilder(value.Length);
        foreach (var character in value)
        {
            if (character is '\\' or '%' or '_')
            {
                builder.Append('\\');
            }

            builder.Append(character);
        }

        return builder.ToString();
    }
}
