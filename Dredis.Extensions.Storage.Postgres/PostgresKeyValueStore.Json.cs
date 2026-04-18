using System.Text.Json;
using Dredis.Abstractions.Storage;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore
{
    public async Task<JsonGetResult> JsonGetAsync(string key, string[] paths, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(paths);

        if (paths.Length == 0)
        {
            paths = ["$"];
        }

        var documentResult = await LoadJsonDocumentAsync(key, token).ConfigureAwait(false);
        if (documentResult.Status != JsonResultStatus.Ok)
        {
            return new JsonGetResult(documentResult.Status);
        }

        var root = documentResult.Value!;
        if (paths.Length == 1)
        {
            var matches = GetPathMatches(root, paths[0], allowWildcards: true, out var status);
            if (status != JsonResultStatus.Ok || matches.Count == 0)
            {
                return new JsonGetResult(matches.Count == 0 ? JsonResultStatus.PathNotFound : status);
            }

            return new JsonGetResult(JsonResultStatus.Ok, value: SerializeJson(matches[0]));
        }

        var values = new byte[paths.Length][];
        for (var i = 0; i < paths.Length; i++)
        {
            var matches = GetPathMatches(root, paths[i], allowWildcards: true, out var status);
            if (status != JsonResultStatus.Ok || matches.Count == 0)
            {
                return new JsonGetResult(matches.Count == 0 ? JsonResultStatus.PathNotFound : status);
            }

            values[i] = SerializeJson(matches[0]);
        }

        return new JsonGetResult(JsonResultStatus.Ok, values: values);
    }

    public async Task<JsonSetResult> JsonSetAsync(string key, string path, byte[] value, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(value);

        if (!TryParseJson(value, out var parsedValue))
        {
            return new JsonSetResult(JsonResultStatus.InvalidJson);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (existing.Exists && existing.Kind != JsonKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonSetResult(JsonResultStatus.WrongType);
        }

        if (!TryParseJsonPath(path, allowWildcards: false, out var segments))
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonSetResult(JsonResultStatus.InvalidPath);
        }

        bool created;
        object? root;

        if (!existing.Exists)
        {
            if (segments.Count != 0)
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return new JsonSetResult(JsonResultStatus.PathNotFound);
            }

            root = parsedValue;
            created = true;
        }
        else
        {
            root = ParseJsonOrThrow(existing.Value!);
            created = false;

            if (segments.Count == 0)
            {
                root = parsedValue;
            }
            else if (!TrySetAtPath(root, segments, parsedValue))
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return new JsonSetResult(JsonResultStatus.PathNotFound);
            }
        }

        await UpsertKeyMetadataAsync(connection, key, JsonKind, SerializeJson(root), existing.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new JsonSetResult(JsonResultStatus.Ok, created);
    }

    public async Task<JsonDelResult> JsonDelAsync(string key, string[] paths, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(paths);

        if (paths.Length == 0)
        {
            paths = ["$"];
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new JsonDelResult(JsonResultStatus.Ok, 0);
        }

        if (existing.Kind != JsonKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonDelResult(JsonResultStatus.WrongType);
        }

        var root = ParseJsonOrThrow(existing.Value!);
        long deleted = 0;

        foreach (var path in paths)
        {
            if (!TryParseJsonPath(path, allowWildcards: false, out var segments))
            {
                await transaction.RollbackAsync(token).ConfigureAwait(false);
                return new JsonDelResult(JsonResultStatus.InvalidPath);
            }

            if (segments.Count == 0)
            {
                deleted++;
                await DeleteByKeyAsync(connection, key, transaction, token).ConfigureAwait(false);
                await transaction.CommitAsync(token).ConfigureAwait(false);
                return new JsonDelResult(JsonResultStatus.Ok, deleted);
            }

            if (TryDeleteAtPath(root, segments))
            {
                deleted++;
            }
        }

        await UpsertKeyMetadataAsync(connection, key, JsonKind, SerializeJson(root), existing.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new JsonDelResult(JsonResultStatus.Ok, deleted);
    }

    public async Task<JsonTypeResult> JsonTypeAsync(string key, string[] paths, CancellationToken token = default)
    {
        return await ProjectJsonPathsAsync(
            key,
            paths,
            allowWildcards: true,
            token,
            (matches, status) =>
            {
                if (status != JsonResultStatus.Ok)
                {
                    return new JsonTypeResult(status);
                }

                if (matches.Count == 0)
                {
                    return new JsonTypeResult(JsonResultStatus.PathNotFound);
                }

                return new JsonTypeResult(JsonResultStatus.Ok, [.. matches.Select(GetJsonTypeName)]);
            }).ConfigureAwait(false);
    }

    public async Task<JsonArrayResult> JsonStrlenAsync(string key, string[] paths, CancellationToken token = default)
    {
        return await ProjectJsonPathsAsync(
            key,
            paths,
            allowWildcards: true,
            token,
            (matches, status) =>
            {
                if (status != JsonResultStatus.Ok)
                {
                    return new JsonArrayResult(status);
                }

                if (matches.Count == 0 || matches.Any(static x => x is not string))
                {
                    return new JsonArrayResult(JsonResultStatus.PathNotFound);
                }

                var lengths = matches.Cast<string>().Select(static x => (long)x.Length).ToArray();
                return lengths.Length == 1
                    ? new JsonArrayResult(JsonResultStatus.Ok, count: lengths[0])
                    : new JsonArrayResult(JsonResultStatus.Ok, counts: lengths);
            }).ConfigureAwait(false);
    }

    public async Task<JsonArrayResult> JsonArrlenAsync(string key, string[] paths, CancellationToken token = default)
    {
        return await ProjectJsonPathsAsync(
            key,
            paths,
            allowWildcards: true,
            token,
            (matches, status) =>
            {
                if (status != JsonResultStatus.Ok)
                {
                    return new JsonArrayResult(status);
                }

                if (matches.Count == 0 || matches.Any(static x => x is not List<object?>))
                {
                    return new JsonArrayResult(JsonResultStatus.PathNotFound);
                }

                var lengths = matches.Cast<List<object?>>().Select(static x => (long)x.Count).ToArray();
                return lengths.Length == 1
                    ? new JsonArrayResult(JsonResultStatus.Ok, count: lengths[0])
                    : new JsonArrayResult(JsonResultStatus.Ok, counts: lengths);
            }).ConfigureAwait(false);
    }

    public async Task<JsonArrayResult> JsonArrappendAsync(string key, string path, byte[][] values, CancellationToken token = default)
    {
        return await MutateJsonArrayAsync(
            key,
            path,
            token,
            values,
            (list, parsedValues) =>
            {
                list.AddRange(parsedValues);
                return list.Count;
            }).ConfigureAwait(false);
    }

    public async Task<JsonGetResult> JsonArrindexAsync(string key, string path, byte[] value, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(value);

        if (!TryParseJson(value, out var needle))
        {
            return new JsonGetResult(JsonResultStatus.InvalidJson);
        }

        var documentResult = await LoadJsonDocumentAsync(key, token).ConfigureAwait(false);
        if (documentResult.Status != JsonResultStatus.Ok)
        {
            return new JsonGetResult(documentResult.Status);
        }

        var matches = GetPathMatches(documentResult.Value!, path, allowWildcards: false, out var status);
        if (status != JsonResultStatus.Ok)
        {
            return new JsonGetResult(status);
        }

        if (matches.Count != 1 || matches[0] is not List<object?> list)
        {
            return new JsonGetResult(JsonResultStatus.PathNotFound);
        }

        var index = -1;
        for (var i = 0; i < list.Count; i++)
        {
            if (JsonDeepEquals(list[i], needle))
            {
                index = i;
                break;
            }
        }

        return new JsonGetResult(JsonResultStatus.Ok, value: System.Text.Encoding.UTF8.GetBytes(index.ToString()));
    }

    public async Task<JsonArrayResult> JsonArrinsertAsync(string key, string path, int index, byte[][] values, CancellationToken token = default)
    {
        return await MutateJsonArrayAsync(
            key,
            path,
            token,
            values,
            (list, parsedValues) =>
            {
                var normalized = NormalizeInsertIndex(index, list.Count);
                list.InsertRange(normalized, parsedValues);
                return list.Count;
            }).ConfigureAwait(false);
    }

    public async Task<JsonArrayResult> JsonArrremAsync(string key, string path, int? index, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(path);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.PathNotFound);
        }

        if (existing.Kind != JsonKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.WrongType);
        }

        var root = ParseJsonOrThrow(existing.Value!);
        var (parent, segment, status) = TryResolveMutableParent(root, path);
        if (status != JsonResultStatus.Ok || segment is null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(status);
        }

        var array = GetChildAtPath(parent, segment) as List<object?>;
        if (array is null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.PathNotFound);
        }

        if (array.Count == 0)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.Ok, count: 0);
        }

        var normalized = NormalizeIndex(index ?? -1, array.Count);
        if (normalized is null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.Ok, count: 0);
        }

        array.RemoveAt(normalized.Value);
        await UpsertKeyMetadataAsync(connection, key, JsonKind, SerializeJson(root), existing.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new JsonArrayResult(JsonResultStatus.Ok, count: 1);
    }

    public async Task<JsonArrayResult> JsonArrtrimAsync(string key, string path, int start, int stop, CancellationToken token = default)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(path);

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.PathNotFound);
        }

        if (existing.Kind != JsonKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.WrongType);
        }

        var root = ParseJsonOrThrow(existing.Value!);
        var (parent, segment, status) = TryResolveMutableParent(root, path);
        if (status != JsonResultStatus.Ok || segment is null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(status);
        }

        var array = GetChildAtPath(parent, segment) as List<object?>;
        if (array is null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.PathNotFound);
        }

        var slice = SliceJsonArray(array, start, stop);
        array.Clear();
        array.AddRange(slice);
        await UpsertKeyMetadataAsync(connection, key, JsonKind, SerializeJson(root), existing.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new JsonArrayResult(JsonResultStatus.Ok, count: array.Count);
    }

    public async Task<JsonMGetResult> JsonMgetAsync(string[] keys, string path, CancellationToken token = default)
    {
        ValidateKeys(keys);
        ArgumentNullException.ThrowIfNull(path);

        var values = new byte[keys.Length][];
        for (var i = 0; i < keys.Length; i++)
        {
            var documentResult = await LoadJsonDocumentAsync(keys[i], token).ConfigureAwait(false);
            if (documentResult.Status == JsonResultStatus.WrongType)
            {
                return new JsonMGetResult(JsonResultStatus.WrongType);
            }

            if (documentResult.Status != JsonResultStatus.Ok)
            {
                values[i] = SerializeJson(null);
                continue;
            }

            var matches = GetPathMatches(documentResult.Value!, path, allowWildcards: true, out var status);
            if (status != JsonResultStatus.Ok || matches.Count == 0)
            {
                values[i] = SerializeJson(null);
            }
            else
            {
                values[i] = SerializeJson(matches[0]);
            }
        }

        return new JsonMGetResult(JsonResultStatus.Ok, values);
    }

    private async Task<(JsonResultStatus Status, object? Value)> LoadJsonDocumentAsync(string key, CancellationToken token)
    {
        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        var existing = await TryGetLiveEntryAsync(connection, key, transaction: null, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            return (JsonResultStatus.PathNotFound, null);
        }

        if (existing.Kind != JsonKind)
        {
            return (JsonResultStatus.WrongType, null);
        }

        return (JsonResultStatus.Ok, ParseJsonOrThrow(existing.Value!));
    }

    private async Task<JsonArrayResult> MutateJsonArrayAsync(
        string key,
        string path,
        CancellationToken token,
        byte[][] values,
        Func<List<object?>, List<object?>, long> mutator)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(values);

        var parsedValues = new List<object?>(values.Length);
        foreach (var value in values)
        {
            ArgumentNullException.ThrowIfNull(value);
            if (!TryParseJson(value, out var parsed))
            {
                return new JsonArrayResult(JsonResultStatus.InvalidJson);
            }

            parsedValues.Add(parsed);
        }

        await using var connection = await OpenConnectionAsync(token).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(token).ConfigureAwait(false);

        var existing = await TryGetLiveEntryAsync(connection, key, transaction, token).ConfigureAwait(false);
        if (!existing.Exists)
        {
            await transaction.CommitAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.PathNotFound);
        }

        if (existing.Kind != JsonKind)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.WrongType);
        }

        var root = ParseJsonOrThrow(existing.Value!);
        var (parent, segment, status) = TryResolveMutableParent(root, path);
        if (status != JsonResultStatus.Ok || segment is null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(status);
        }

        var array = GetChildAtPath(parent, segment) as List<object?>;
        if (array is null)
        {
            await transaction.RollbackAsync(token).ConfigureAwait(false);
            return new JsonArrayResult(JsonResultStatus.PathNotFound);
        }

        var count = mutator(array, parsedValues);
        await UpsertKeyMetadataAsync(connection, key, JsonKind, SerializeJson(root), existing.ExpiresAt, transaction, token).ConfigureAwait(false);
        await transaction.CommitAsync(token).ConfigureAwait(false);
        return new JsonArrayResult(JsonResultStatus.Ok, count: count);
    }

    private async Task<T> ProjectJsonPathsAsync<T>(
        string key,
        string[] paths,
        bool allowWildcards,
        CancellationToken token,
        Func<List<object?>, JsonResultStatus, T> projector)
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(paths);
        if (paths.Length == 0)
        {
            paths = ["$"];
        }

        var documentResult = await LoadJsonDocumentAsync(key, token).ConfigureAwait(false);
        if (documentResult.Status != JsonResultStatus.Ok)
        {
            return projector([], documentResult.Status);
        }

        var allMatches = new List<object?>();
        foreach (var path in paths)
        {
            var matches = GetPathMatches(documentResult.Value!, path, allowWildcards, out var status);
            if (status != JsonResultStatus.Ok)
            {
                return projector([], status);
            }

            if (matches.Count == 0)
            {
                return projector([], JsonResultStatus.PathNotFound);
            }

            allMatches.AddRange(matches);
        }

        return projector(allMatches, JsonResultStatus.Ok);
    }

    private static List<object?> GetPathMatches(object? root, string path, bool allowWildcards, out JsonResultStatus status)
    {
        if (!TryParseJsonPath(path, allowWildcards, out var segments))
        {
            status = JsonResultStatus.InvalidPath;
            return [];
        }

        var candidates = new List<object?> { root };
        foreach (var segment in segments)
        {
            var next = new List<object?>();
            foreach (var candidate in candidates)
            {
                switch (segment.Kind)
                {
                    case JsonPathSegmentKind.Property:
                        if (candidate is Dictionary<string, object?> obj && obj.TryGetValue(segment.Property!, out var propertyValue))
                        {
                            next.Add(propertyValue);
                        }
                        break;
                    case JsonPathSegmentKind.Index:
                        if (candidate is List<object?> array)
                        {
                            var index = segment.Index < 0 ? array.Count + segment.Index : segment.Index;
                            if (index >= 0 && index < array.Count)
                            {
                                next.Add(array[index]);
                            }
                        }
                        break;
                    case JsonPathSegmentKind.Wildcard:
                        if (candidate is Dictionary<string, object?> wildcardObject)
                        {
                            next.AddRange(wildcardObject.Values);
                        }
                        else if (candidate is List<object?> wildcardArray)
                        {
                            next.AddRange(wildcardArray);
                        }
                        break;
                }
            }

            candidates = next;
            if (candidates.Count == 0)
            {
                break;
            }
        }

        status = JsonResultStatus.Ok;
        return candidates;
    }

    private static (object? Parent, JsonPathSegment? Segment, JsonResultStatus Status) TryResolveMutableParent(object? root, string path)
    {
        if (!TryParseJsonPath(path, allowWildcards: false, out var segments))
        {
            return (null, null, JsonResultStatus.InvalidPath);
        }

        if (segments.Count == 0)
        {
            return (null, null, JsonResultStatus.InvalidPath);
        }

        object? current = root;
        for (var i = 0; i < segments.Count - 1; i++)
        {
            current = GetChildAtPath(current, segments[i]);
            if (current is null)
            {
                return (null, null, JsonResultStatus.PathNotFound);
            }
        }

        return (current, segments[^1], JsonResultStatus.Ok);
    }

    private static object? GetChildAtPath(object? parent, JsonPathSegment segment)
    {
        return segment.Kind switch
        {
            JsonPathSegmentKind.Property when parent is Dictionary<string, object?> obj && obj.TryGetValue(segment.Property!, out var value) => value,
            JsonPathSegmentKind.Index when parent is List<object?> array && NormalizeIndex(segment.Index, array.Count) is { } index => array[index],
            _ => null
        };
    }

    private static bool TrySetAtPath(object? root, IReadOnlyList<JsonPathSegment> segments, object? value)
    {
        var (parent, segment, status) = TryResolveMutableParent(root, BuildPathText(segments));
        if (status != JsonResultStatus.Ok || segment is null || parent is null)
        {
            return false;
        }

        switch (segment.Kind)
        {
            case JsonPathSegmentKind.Property when parent is Dictionary<string, object?> obj:
                obj[segment.Property!] = value;
                return true;
            case JsonPathSegmentKind.Index when parent is List<object?> array:
                var normalized = NormalizeIndex(segment.Index, array.Count);
                if (normalized is null)
                {
                    return false;
                }

                array[normalized.Value] = value;
                return true;
            default:
                return false;
        }
    }

    private static bool TryDeleteAtPath(object? root, IReadOnlyList<JsonPathSegment> segments)
    {
        var path = BuildPathText(segments);
        var (parent, segment, status) = TryResolveMutableParent(root, path);
        if (status != JsonResultStatus.Ok || segment is null || parent is null)
        {
            return false;
        }

        return segment.Kind switch
        {
            JsonPathSegmentKind.Property when parent is Dictionary<string, object?> obj => obj.Remove(segment.Property!),
            JsonPathSegmentKind.Index when parent is List<object?> array && NormalizeIndex(segment.Index, array.Count) is { } index =>
                RemoveAt(array, index),
            _ => false
        };
    }

    private static bool RemoveAt(List<object?> array, int index)
    {
        array.RemoveAt(index);
        return true;
    }

    private static bool TryParseJsonPath(string path, bool allowWildcards, out List<JsonPathSegment> segments)
    {
        segments = [];
        if (string.IsNullOrWhiteSpace(path) || !path.StartsWith('$'))
        {
            return false;
        }

        var current = 1;
        while (current < path.Length)
        {
            if (path[current] == '.')
            {
                current++;
                if (current >= path.Length)
                {
                    return false;
                }

                if (path[current] == '*')
                {
                    if (!allowWildcards)
                    {
                        return false;
                    }

                    segments.Add(JsonPathSegment.Wildcard());
                    current++;
                    continue;
                }

                var start = current;
                while (current < path.Length && path[current] != '.' && path[current] != '[')
                {
                    current++;
                }

                if (start == current)
                {
                    return false;
                }

                segments.Add(JsonPathSegment.PropertySegment(path[start..current]));
                continue;
            }

            if (path[current] == '[')
            {
                current++;
                var start = current;
                while (current < path.Length && path[current] != ']')
                {
                    current++;
                }

                if (current >= path.Length)
                {
                    return false;
                }

                var text = path[start..current];
                current++;
                if (text == "*")
                {
                    if (!allowWildcards)
                    {
                        return false;
                    }

                    segments.Add(JsonPathSegment.Wildcard());
                    continue;
                }

                if (!int.TryParse(text, out var index))
                {
                    return false;
                }

                segments.Add(JsonPathSegment.IndexSegment(index));
                continue;
            }

            return false;
        }

        return true;
    }

    private static bool TryParseJson(byte[] data, out object? value)
    {
        try
        {
            using var document = JsonDocument.Parse(data);
            value = ConvertJsonElement(document.RootElement);
            return true;
        }
        catch (JsonException)
        {
            value = null;
            return false;
        }
    }

    private static object? ParseJsonOrThrow(byte[] data)
    {
        if (!TryParseJson(data, out var value))
        {
            throw new InvalidOperationException("Stored JSON document is invalid.");
        }

        return value;
    }

    private static object? ConvertJsonElement(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.Object => element.EnumerateObject().ToDictionary(static x => x.Name, static x => ConvertJsonElement(x.Value)),
            JsonValueKind.Array => element.EnumerateArray().Select(ConvertJsonElement).ToList(),
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var longValue) ? longValue : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => null
        };
    }

    private static byte[] SerializeJson(object? value) => JsonSerializer.SerializeToUtf8Bytes(value);

    private static string GetJsonTypeName(object? value) => value switch
    {
        Dictionary<string, object?> => "object",
        List<object?> => "array",
        string => "string",
        bool => "boolean",
        null => "null",
        _ => "number"
    };

    private static bool JsonDeepEquals(object? left, object? right)
    {
        if (left is null || right is null)
        {
            return left is null && right is null;
        }

        if (left is Dictionary<string, object?> leftObject && right is Dictionary<string, object?> rightObject)
        {
            if (leftObject.Count != rightObject.Count)
            {
                return false;
            }

            foreach (var pair in leftObject)
            {
                if (!rightObject.TryGetValue(pair.Key, out var rightValue) || !JsonDeepEquals(pair.Value, rightValue))
                {
                    return false;
                }
            }

            return true;
        }

        if (left is List<object?> leftList && right is List<object?> rightList)
        {
            if (leftList.Count != rightList.Count)
            {
                return false;
            }

            for (var i = 0; i < leftList.Count; i++)
            {
                if (!JsonDeepEquals(leftList[i], rightList[i]))
                {
                    return false;
                }
            }

            return true;
        }

        if (IsNumeric(left) && IsNumeric(right))
        {
            return Convert.ToDouble(left) == Convert.ToDouble(right);
        }

        return Equals(left, right);
    }

    private static bool IsNumeric(object value) =>
        value is sbyte or byte or short or ushort or int or uint or long or ulong or float or double or decimal;

    private static int NormalizeInsertIndex(int index, int count)
    {
        var normalized = index < 0 ? count + index : index;
        return Math.Clamp(normalized, 0, count);
    }

    private static List<object?> SliceJsonArray(List<object?> array, int start, int stop)
    {
        if (array.Count == 0)
        {
            return [];
        }

        var normalizedStart = start < 0 ? array.Count + start : start;
        var normalizedStop = stop < 0 ? array.Count + stop : stop;
        normalizedStart = Math.Max(0, normalizedStart);
        normalizedStop = Math.Min(array.Count - 1, normalizedStop);

        if (normalizedStart >= array.Count || normalizedStart > normalizedStop)
        {
            return [];
        }

        return array.Skip(normalizedStart).Take(normalizedStop - normalizedStart + 1).ToList();
    }

    private static string BuildPathText(IReadOnlyList<JsonPathSegment> segments)
    {
        if (segments.Count == 0)
        {
            return "$";
        }

        var builder = new System.Text.StringBuilder("$");
        foreach (var segment in segments)
        {
            switch (segment.Kind)
            {
                case JsonPathSegmentKind.Property:
                    builder.Append('.').Append(segment.Property);
                    break;
                case JsonPathSegmentKind.Index:
                    builder.Append('[').Append(segment.Index).Append(']');
                    break;
                case JsonPathSegmentKind.Wildcard:
                    builder.Append("[*]");
                    break;
            }
        }

        return builder.ToString();
    }

    private enum JsonPathSegmentKind
    {
        Property,
        Index,
        Wildcard
    }

    private sealed record JsonPathSegment(JsonPathSegmentKind Kind, string? Property, int Index)
    {
        public static JsonPathSegment PropertySegment(string property) => new(JsonPathSegmentKind.Property, property, 0);
        public static JsonPathSegment IndexSegment(int index) => new(JsonPathSegmentKind.Index, null, index);
        public static JsonPathSegment Wildcard() => new(JsonPathSegmentKind.Wildcard, null, 0);
    }
}
