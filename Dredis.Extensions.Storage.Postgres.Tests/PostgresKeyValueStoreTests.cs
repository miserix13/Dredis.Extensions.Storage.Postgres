using Dredis.Abstractions.Storage;
namespace Dredis.Extensions.Storage.Postgres.Tests;

public sealed class PostgresKeyValueStoreTests
{
    private const string ConnectionStringEnvironmentVariable = "DREDIS_POSTGRES_TEST_CONNECTION_STRING";

    [Fact]
    public void Constructor_rejects_invalid_table_name()
    {
        var dataSource = Npgsql.NpgsqlDataSource.Create("Host=localhost;Username=postgres;Password=postgres;Database=postgres");

        var exception = Assert.Throws<ArgumentException>(() => new PostgresKeyValueStore(dataSource, "invalid-table-name"));

        Assert.Contains("Table name", exception.Message);
        dataSource.Dispose();
    }

    [Fact]
    public async Task Unsupported_members_throw_not_supported()
    {
        var dataSource = Npgsql.NpgsqlDataSource.Create("Host=localhost;Username=postgres;Password=postgres;Database=postgres");
        await using var store = new PostgresKeyValueStore(dataSource, "dredis_test_store");

        await Assert.ThrowsAsync<NotSupportedException>(() => store.HyperLogLogCountAsync(["hll"]));
        await Assert.ThrowsAsync<NotSupportedException>(() => store.TimeSeriesGetAsync("metrics"));
        await Assert.ThrowsAsync<NotSupportedException>(() => store.VectorGetAsync("embedding"));
    }

    [Fact]
    public async Task Core_string_operations_work_against_postgres()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var tableName = $"dredis_test_{Guid.NewGuid():N}";
        await using var store = new PostgresKeyValueStore(connectionString, tableName);

        Assert.True(await store.SetAsync("alpha", "one"u8.ToArray(), expiration: null, SetCondition.None));
        Assert.Equal("one"u8.ToArray(), await store.GetAsync("alpha"));

        Assert.True(await store.SetAsync("beta", "1"u8.ToArray(), TimeSpan.FromMilliseconds(150), SetCondition.None));
        Assert.True(await store.ExistsAsync("beta"));
        Assert.True(await store.PttlAsync("beta") > 0);

        var many = await store.GetManyAsync(["alpha", "missing", "beta"]);
        Assert.Equal("one"u8.ToArray(), many[0]);
        Assert.Null(many[1]);
        Assert.Equal("1"u8.ToArray(), many[2]);

        Assert.True(await store.SetManyAsync(
        [
            new KeyValuePair<string, byte[]>("count", "10"u8.ToArray()),
            new KeyValuePair<string, byte[]>("gamma", "value"u8.ToArray())
        ]));

        Assert.Equal(2, await store.ExistsAsync(["count", "gamma", "missing"]));
        Assert.Equal(15, await store.IncrByAsync("count", 5));
        Assert.Equal("15"u8.ToArray(), await store.GetAsync("count"));

        Assert.False(await store.SetAsync("alpha", "replaced"u8.ToArray(), expiration: null, SetCondition.Nx));
        Assert.True(await store.SetAsync("alpha", "replaced"u8.ToArray(), expiration: null, SetCondition.Xx));
        Assert.Equal("replaced"u8.ToArray(), await store.GetAsync("alpha"));

        await Task.Delay(300);
        Assert.Null(await store.GetAsync("beta"));
        Assert.Equal(-2, await store.TtlAsync("beta"));

        Assert.Equal(2, await store.DeleteAsync(["alpha", "gamma"]));
        Assert.False(await store.ExistsAsync("alpha"));
        Assert.True(await store.CleanUpExpiredKeysAsync() >= 0);
    }

    [Fact]
    public async Task Hash_list_and_set_operations_work_against_postgres()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var tableName = $"dredis_structures_{Guid.NewGuid():N}";
        await using var store = new PostgresKeyValueStore(connectionString, tableName);

        Assert.True(await store.HashSetAsync("user:1", "name", "alice"u8.ToArray()));
        Assert.False(await store.HashSetAsync("user:1", "name", "alicia"u8.ToArray()));
        Assert.True(await store.HashSetAsync("user:1", "role", "admin"u8.ToArray()));
        Assert.Equal("alicia"u8.ToArray(), await store.HashGetAsync("user:1", "name"));
        Assert.Equal(2, (await store.HashGetAllAsync("user:1")).Length);
        Assert.Equal(1, await store.HashDeleteAsync("user:1", ["role"]));
        Assert.Null(await store.HashGetAsync("user:1", "role"));

        var leftPush = await store.ListPushAsync("jobs", ["a"u8.ToArray(), "b"u8.ToArray()], left: true);
        Assert.Equal(ListResultStatus.Ok, leftPush.Status);
        Assert.Equal(2, leftPush.Length);

        var rightPush = await store.ListPushAsync("jobs", ["c"u8.ToArray()], left: false);
        Assert.Equal(3, rightPush.Length);

        var range = await store.ListRangeAsync("jobs", 0, -1);
        Assert.Equal(ListResultStatus.Ok, range.Status);
        Assert.Equal(["b"u8.ToArray(), "a"u8.ToArray(), "c"u8.ToArray()], range.Values);

        var index = await store.ListIndexAsync("jobs", -1);
        Assert.Equal(ListResultStatus.Ok, index.Status);
        Assert.Equal("c"u8.ToArray(), index.Value);

        Assert.Equal(ListSetResultStatus.Ok, (await store.ListSetAsync("jobs", 1, "updated"u8.ToArray())).Status);
        Assert.Equal("updated"u8.ToArray(), (await store.ListIndexAsync("jobs", 1)).Value);

        Assert.Equal(ListResultStatus.Ok, await store.ListTrimAsync("jobs", 1, 2));
        Assert.Equal(2, (await store.ListLengthAsync("jobs")).Length);
        Assert.Equal("c"u8.ToArray(), (await store.ListPopAsync("jobs", left: false)).Value);

        var addMembers = await store.SetAddAsync("tags", ["red"u8.ToArray(), "blue"u8.ToArray(), "red"u8.ToArray()]);
        Assert.Equal(SetResultStatus.Ok, addMembers.Status);
        Assert.Equal(2, addMembers.Count);

        var members = await store.SetMembersAsync("tags");
        Assert.Equal(SetResultStatus.Ok, members.Status);
        Assert.Equal(2, members.Members.Length);
        Assert.Equal(2, (await store.SetCardinalityAsync("tags")).Count);
        Assert.Equal(1, (await store.SetRemoveAsync("tags", ["blue"u8.ToArray()])).Count);

        await store.SetAsync("plain", "value"u8.ToArray(), expiration: null, SetCondition.None);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.HashGetAsync("plain", "field"));
        Assert.Equal(ListResultStatus.WrongType, (await store.ListLengthAsync("plain")).Status);
        Assert.Equal(SetResultStatus.WrongType, (await store.SetCardinalityAsync("plain")).Status);
    }

    [Fact]
    public async Task Sorted_set_operations_work_against_postgres()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var tableName = $"dredis_sorted_{Guid.NewGuid():N}";
        await using var store = new PostgresKeyValueStore(connectionString, tableName);

        var add = await store.SortedSetAddAsync(
        "scores",
        [
            new SortedSetEntry("alice"u8.ToArray(), 10),
            new SortedSetEntry("bob"u8.ToArray(), 20),
            new SortedSetEntry("carol"u8.ToArray(), 15)
        ]);

        Assert.Equal(SortedSetResultStatus.Ok, add.Status);
        Assert.Equal(3, add.Count);

        var update = await store.SortedSetAddAsync(
            "scores",
            [new SortedSetEntry("alice"u8.ToArray(), 12)]);
        Assert.Equal(0, update.Count);

        var range = await store.SortedSetRangeAsync("scores", 0, -1);
        Assert.Equal(SortedSetResultStatus.Ok, range.Status);
        Assert.Equal(["alice"u8.ToArray(), "carol"u8.ToArray(), "bob"u8.ToArray()], range.Entries.Select(static x => x.Member).ToArray());

        var byScore = await store.SortedSetRangeByScoreAsync("scores", 12, 20);
        Assert.Equal(3, byScore.Entries.Length);
        Assert.Equal(3, (await store.SortedSetCardinalityAsync("scores")).Count);
        Assert.Equal(2, (await store.SortedSetCountByScoreAsync("scores", 13, 20)).Count);

        var score = await store.SortedSetScoreAsync("scores", "alice"u8.ToArray());
        Assert.Equal(12, score.Score);

        var incremented = await store.SortedSetIncrementAsync("scores", 10, "alice"u8.ToArray());
        Assert.Equal(22, incremented.Score);
        Assert.Equal(2, (await store.SortedSetRankAsync("scores", "alice"u8.ToArray())).Rank);
        Assert.Equal(0, (await store.SortedSetReverseRankAsync("scores", "alice"u8.ToArray())).Rank);

        var removedRange = await store.SortedSetRemoveRangeByScoreAsync("scores", 21, 30);
        Assert.Equal(SortedSetResultStatus.Ok, removedRange.Status);
        Assert.Equal(1, removedRange.Removed);

        var removedMember = await store.SortedSetRemoveAsync("scores", ["bob"u8.ToArray()]);
        Assert.Equal(1, removedMember.Count);
        Assert.Equal(1, (await store.SortedSetCardinalityAsync("scores")).Count);

        await store.SetAsync("plain-zset", "value"u8.ToArray(), expiration: null, SetCondition.None);
        Assert.Equal(SortedSetResultStatus.WrongType, (await store.SortedSetCardinalityAsync("plain-zset")).Status);
        Assert.Equal(SortedSetResultStatus.WrongType, (await store.SortedSetScoreAsync("plain-zset", "x"u8.ToArray())).Status);
    }

    [Fact]
    public async Task Json_operations_work_against_postgres()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var tableName = $"dredis_json_{Guid.NewGuid():N}";
        await using var store = new PostgresKeyValueStore(connectionString, tableName);

        var setRoot = await store.JsonSetAsync("doc", "$", """{"name":"alice","items":[1,2],"meta":{"active":true}}"""u8.ToArray());
        Assert.Equal(JsonResultStatus.Ok, setRoot.Status);
        Assert.True(setRoot.Created);

        var getRoot = await store.JsonGetAsync("doc", ["$"]);
        Assert.Equal(JsonResultStatus.Ok, getRoot.Status);
        Assert.Equal("""{"name":"alice","items":[1,2],"meta":{"active":true}}"""u8.ToArray(), getRoot.Value);

        var setNested = await store.JsonSetAsync("doc", "$.name", """"alicia""""u8.ToArray());
        Assert.Equal(JsonResultStatus.Ok, setNested.Status);
        Assert.Equal(""""alicia""""u8.ToArray(), (await store.JsonGetAsync("doc", ["$.name"])).Value);

        var type = await store.JsonTypeAsync("doc", ["$.items"]);
        Assert.Equal(JsonResultStatus.Ok, type.Status);
        Assert.NotNull(type.Types);
        Assert.Equal(["array"], type.Types);

        var append = await store.JsonArrappendAsync("doc", "$.items", ["3"u8.ToArray(), "4"u8.ToArray()]);
        Assert.Equal(JsonResultStatus.Ok, append.Status);
        Assert.Equal(4, append.Count);

        var arrLen = await store.JsonArrlenAsync("doc", ["$.items"]);
        Assert.Equal(4, arrLen.Count);

        var arrIndex = await store.JsonArrindexAsync("doc", "$.items", "3"u8.ToArray());
        Assert.Equal("2"u8.ToArray(), arrIndex.Value);

        var insert = await store.JsonArrinsertAsync("doc", "$.items", 1, ["99"u8.ToArray()]);
        Assert.Equal(5, insert.Count);

        var trim = await store.JsonArrtrimAsync("doc", "$.items", 1, 3);
        Assert.Equal(3, trim.Count);

        var rem = await store.JsonArrremAsync("doc", "$.items", null);
        Assert.Equal(1, rem.Count);

        var strlen = await store.JsonStrlenAsync("doc", ["$.name"]);
        Assert.Equal(6, strlen.Count);

        var mget = await store.JsonMgetAsync(["doc", "missing"], "$.name");
        Assert.Equal(JsonResultStatus.Ok, mget.Status);
        Assert.Equal(""""alicia""""u8.ToArray(), mget.Values![0]);
        Assert.Equal("null"u8.ToArray(), mget.Values[1]);

        var del = await store.JsonDelAsync("doc", ["$.meta"]);
        Assert.Equal(JsonResultStatus.Ok, del.Status);
        Assert.Equal(1, del.Deleted);

        var missingPath = await store.JsonGetAsync("doc", ["$.meta"]);
        Assert.Equal(JsonResultStatus.PathNotFound, missingPath.Status);

        await store.SetAsync("plain-json", "value"u8.ToArray(), expiration: null, SetCondition.None);
        Assert.Equal(JsonResultStatus.WrongType, (await store.JsonGetAsync("plain-json", ["$"])).Status);
    }

    [Fact]
    public async Task Stream_operations_work_against_postgres()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return;
        }

        var tableName = $"dredis_streams_{Guid.NewGuid():N}";
        await using var store = new PostgresKeyValueStore(connectionString, tableName);

        var firstId = await store.StreamAddAsync(
            "events",
            "*",
            [
                new KeyValuePair<string, byte[]>("type", "created"u8.ToArray()),
                new KeyValuePair<string, byte[]>("value", "1"u8.ToArray())
            ]);
        var secondId = await store.StreamAddAsync(
            "events",
            "*",
            [
                new KeyValuePair<string, byte[]>("type", "updated"u8.ToArray())
            ]);

        Assert.NotNull(firstId);
        Assert.NotNull(secondId);
        Assert.Equal(2, await store.StreamLengthAsync("events"));
        Assert.Equal(secondId, await store.StreamLastIdAsync("events"));

        var read = await store.StreamReadAsync(["events"], ["0-0"], count: null);
        Assert.Single(read);
        Assert.Equal(2, read[0].Entries.Length);
        Assert.Equal(firstId, read[0].Entries[0].Id);

        var range = await store.StreamRangeAsync("events", "-", "+", count: null);
        Assert.Equal(2, range.Length);
        Assert.Equal(firstId, range[0].Id);

        var reverseRange = await store.StreamRangeReverseAsync("events", "+", "-", count: 1);
        Assert.Single(reverseRange);
        Assert.Equal(secondId, reverseRange[0].Id);

        var info = await store.StreamInfoAsync("events");
        Assert.Equal(StreamInfoResultStatus.Ok, info.Status);
        Assert.NotNull(info.Info);
        Assert.Equal(2, info.Info!.Length);
        Assert.Equal(firstId, info.Info.FirstEntry!.Id);
        Assert.Equal(secondId, info.Info.LastEntry!.Id);

        Assert.Equal(1, await store.StreamTrimAsync("events", maxLength: 1));
        Assert.Equal(1, await store.StreamLengthAsync("events"));
        Assert.Equal(StreamSetIdResultStatus.InvalidId, await store.StreamSetIdAsync("events", "0-0"));
        Assert.Equal(StreamSetIdResultStatus.Ok, await store.StreamSetIdAsync("events", "9999999999999-0"));
        Assert.Equal("9999999999999-0", await store.StreamLastIdAsync("events"));
        Assert.Equal(1, await store.StreamDeleteAsync("events", [secondId!]));
        Assert.Equal(0, await store.StreamLengthAsync("events"));

        var orderOne = await store.StreamAddAsync(
            "orders",
            "*",
            [new KeyValuePair<string, byte[]>("id", "1"u8.ToArray())]);
        var orderTwo = await store.StreamAddAsync(
            "orders",
            "*",
            [new KeyValuePair<string, byte[]>("id", "2"u8.ToArray())]);

        Assert.Equal(StreamGroupCreateResult.Ok, await store.StreamGroupCreateAsync("orders", "workers", "-", mkStream: false));

        var firstRead = await store.StreamGroupReadAsync("workers", "alice", ["orders"], [">"], count: 1, block: null);
        Assert.Equal(StreamGroupReadResultStatus.Ok, firstRead.Status);
        Assert.Single(firstRead.Results);
        Assert.Single(firstRead.Results[0].Entries);
        Assert.Equal(orderOne, firstRead.Results[0].Entries[0].Id);

        var pendingSummary = await store.StreamPendingAsync("orders", "workers");
        Assert.Equal(StreamPendingResultStatus.Ok, pendingSummary.Status);
        Assert.Equal(1, pendingSummary.Count);
        Assert.Equal(orderOne, pendingSummary.SmallestId);
        Assert.Single(pendingSummary.Consumers);
        Assert.Equal("alice", pendingSummary.Consumers[0].Name);

        var ack = await store.StreamAckAsync("orders", "workers", [orderOne!]);
        Assert.Equal(StreamAckResultStatus.Ok, ack.Status);
        Assert.Equal(1, ack.Count);

        var secondRead = await store.StreamGroupReadAsync("workers", "alice", ["orders"], [">"], count: 1, block: null);
        Assert.Equal(StreamGroupReadResultStatus.Ok, secondRead.Status);
        Assert.Single(secondRead.Results[0].Entries);
        Assert.Equal(orderTwo, secondRead.Results[0].Entries[0].Id);

        var claimed = await store.StreamClaimAsync("orders", "workers", "bob", 0, [orderTwo!]);
        Assert.Equal(StreamClaimResultStatus.Ok, claimed.Status);
        Assert.Single(claimed.Entries);
        Assert.Equal(orderTwo, claimed.Entries[0].Id);

        var groups = await store.StreamGroupsInfoAsync("orders");
        Assert.Equal(StreamInfoResultStatus.Ok, groups.Status);
        Assert.Single(groups.Groups);
        Assert.Equal("workers", groups.Groups[0].Name);
        Assert.True(groups.Groups[0].Consumers >= 1);
        Assert.Equal(1, groups.Groups[0].Pending);

        var consumers = await store.StreamConsumersInfoAsync("orders", "workers");
        Assert.Equal(StreamInfoResultStatus.Ok, consumers.Status);
        Assert.Contains(consumers.Consumers, static consumer => consumer.Name == "bob" && consumer.Pending == 1);

        var delConsumer = await store.StreamGroupDelConsumerAsync("orders", "workers", "bob");
        Assert.Equal(StreamGroupDelConsumerResultStatus.Ok, delConsumer.Status);
        Assert.Equal(1, delConsumer.Removed);

        var pendingAfterDelete = await store.StreamPendingAsync("orders", "workers");
        Assert.Equal(StreamPendingResultStatus.Ok, pendingAfterDelete.Status);
        Assert.Equal(0, pendingAfterDelete.Count);

        Assert.Equal(StreamGroupSetIdResultStatus.Ok, await store.StreamGroupSetIdAsync("orders", "workers", "$"));
        Assert.Equal(StreamGroupDestroyResult.Removed, await store.StreamGroupDestroyAsync("orders", "workers"));

        await store.SetAsync("plain-stream", "value"u8.ToArray(), expiration: null, SetCondition.None);
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.StreamLengthAsync("plain-stream"));
        Assert.Equal(StreamInfoResultStatus.WrongType, (await store.StreamInfoAsync("plain-stream")).Status);
        Assert.Equal(StreamGroupCreateResult.WrongType, await store.StreamGroupCreateAsync("plain-stream", "workers", "-", mkStream: false));
    }
}
