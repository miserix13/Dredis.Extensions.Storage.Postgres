using Dredis.Abstractions.Storage;

namespace Dredis.Extensions.Storage.Postgres;

public sealed partial class PostgresKeyValueStore
{
    public Task<TimeSeriesAddResult> TimeSeriesAddAsync(string key, long timestamp, double value, TimeSeriesDuplicatePolicy? onDuplicate, bool createIfMissing, CancellationToken token = default) => throw CreateNotSupported();
    public Task<TimeSeriesResultStatus> TimeSeriesCreateAsync(string key, long? retentionTimeMs, TimeSeriesDuplicatePolicy? duplicatePolicy, KeyValuePair<string, string>[]? labels, CancellationToken token = default) => throw CreateNotSupported();
    public Task<TimeSeriesDeleteResult> TimeSeriesDeleteAsync(string key, long fromTimestamp, long toTimestamp, CancellationToken token = default) => throw CreateNotSupported();
    public Task<TimeSeriesGetResult> TimeSeriesGetAsync(string key, CancellationToken token = default) => throw CreateNotSupported();
    public Task<TimeSeriesAddResult> TimeSeriesIncrementByAsync(string key, double increment, long? timestamp, bool createIfMissing, CancellationToken token = default) => throw CreateNotSupported();
    public Task<TimeSeriesInfoResult> TimeSeriesInfoAsync(string key, CancellationToken token = default) => throw CreateNotSupported();
    public Task<TimeSeriesMRangeResult> TimeSeriesMultiRangeAsync(long fromTimestamp, long toTimestamp, bool reverse, int? count, string? aggregationType, long? bucketDurationMs, KeyValuePair<string, string>[] filters, CancellationToken token = default) => throw CreateNotSupported();
    public Task<TimeSeriesRangeResult> TimeSeriesRangeAsync(string key, long fromTimestamp, long toTimestamp, bool reverse, int? count, string? aggregationType, long? bucketDurationMs, CancellationToken token = default) => throw CreateNotSupported();
    private static NotSupportedException CreateNotSupported([System.Runtime.CompilerServices.CallerMemberName] string? memberName = null) =>
        new($"{memberName} is not implemented in the current PostgreSQL store milestone.");
}
