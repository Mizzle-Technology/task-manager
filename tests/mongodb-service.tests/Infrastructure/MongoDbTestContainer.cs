using Microsoft.Extensions.Options;
using mongodb_service.Configuration;
using Testcontainers.MongoDb;
using Microsoft.Extensions.Configuration;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Configurations;
using MongoDB.Driver;

namespace mongodb_service.tests.Infrastructure;

public class MongoDbTestContainer : IAsyncDisposable
{
    private readonly MongoDbContainer _container;
    private string? _connectionString;
    private const string Username = "root";
    private const string Password = "example";

    public string ConnectionString
    {
        get
        {
            if (_connectionString == null)
                throw new InvalidOperationException("Container not initialized. Call InitializeAsync first.");
            return _connectionString;
        }
    }

    public MongoDbTestContainer()
    {
        _container = new MongoDbBuilder()
            .WithImage("mongo:6.0")
            .WithEnvironment("MONGO_INITDB_ROOT_USERNAME", Username)
            .WithEnvironment("MONGO_INITDB_ROOT_PASSWORD", Password)
            .WithPortBinding(27017, true)
            .WithDockerEndpoint("unix:///var/run/docker.sock")
            .Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();
        var baseConnectionString = _container.GetConnectionString();
        // Construct connection string with auth credentials
        var builder = new MongoUrlBuilder(baseConnectionString)
        {
            Username = Username,
            Password = Password,
            AuthenticationSource = "admin"  // Important for root user authentication
        };
        _connectionString = builder.ToString();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    public MongoDbSettings GetMongoDbSettings()
    {
        return new MongoDbSettings
        {
            ConnectionString = ConnectionString,
            DatabaseName = "test-db",
            StaleTaskTimeout = TimeSpan.FromMinutes(5)
        };
    }
}