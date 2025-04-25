using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using MongoDB.Driver;
using mongodb_service.Configuration;
using DotNet.Testcontainers.Configurations;
using MongoDB.Bson;

namespace MongoDB.Service.Tests.Infrastructure;

public class MongoDbTestContainer : IAsyncDisposable
{
    private readonly IContainer _container;
    private static readonly string DbName = Guid.NewGuid().ToString();

    public MongoDbTestContainer()
    {
        var mongoDbContainerBuilder = new ContainerBuilder()
            .WithName($"mongodb-{Guid.NewGuid()}")
            .WithImage("mongo:6.0")
            .WithPortBinding(27017, true)
            .WithEnvironment(new Dictionary<string, string>
            {
                ["MONGO_INITDB_ROOT_USERNAME"] = "admin",
                ["MONGO_INITDB_ROOT_PASSWORD"] = "password",
                ["MONGO_INITDB_DATABASE"] = DbName
            })
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Waiting for connections"));

        _container = mongoDbContainerBuilder.Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        // Simple connection test with retry
        var connected = false;
        var retries = 0;
        Exception? lastException = null;

        while (!connected && retries < 5)
        {
            try
            {
                var mongoClient = new MongoClient(ConnectionString);
                await mongoClient.GetDatabase("admin").RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1));
                connected = true;
            }
            catch (Exception ex)
            {
                lastException = ex;
                retries++;
                Console.WriteLine($"MongoDB connection attempt {retries} failed: {ex.Message}");
                await Task.Delay(1000); // Wait a second before retrying
            }
        }

        if (!connected)
        {
            throw new Exception("Failed to connect to MongoDB container after multiple attempts", lastException);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    public string Host => _container.Hostname;

    public int Port => _container.GetMappedPublicPort(27017);

    public string ConnectionString
    {
        get
        {
            var builder = new MongoUrlBuilder
            {
                Server = new MongoServerAddress(Host, Port),
                Username = "admin",
                Password = "password",
                DatabaseName = DbName,
                AuthenticationSource = "admin" // Explicitly set auth source
            };
            return builder.ToMongoUrl().ToString();
        }
    }

    public MongoDbSettings GetSettings()
    {
        return new MongoDbSettings
        {
            ConnectionString = ConnectionString,
            DatabaseName = DbName
        };
    }
}