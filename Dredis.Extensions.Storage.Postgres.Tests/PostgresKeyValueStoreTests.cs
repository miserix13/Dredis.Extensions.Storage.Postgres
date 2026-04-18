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

        await Assert.ThrowsAsync<NotSupportedException>(() => store.HashGetAsync("users", "alice"));
        await Assert.ThrowsAsync<NotSupportedException>(() => store.ListLengthAsync("items"));
        await Assert.ThrowsAsync<NotSupportedException>(() => store.JsonGetAsync("doc", ["$"]));
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
}
