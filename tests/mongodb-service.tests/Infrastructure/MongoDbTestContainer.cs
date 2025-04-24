using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using MongoDB.Driver;
using mongodb_service.Configuration;
using DotNet.Testcontainers.Configurations;

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
            .WithCommand("--replSet", "rs0")
            .WithPortBinding(27017, true)
            .WithEnvironment(new Dictionary<string, string>
            {
                ["MONGO_INITDB_ROOT_USERNAME"] = "admin",
                ["MONGO_INITDB_ROOT_PASSWORD"] = "password",
                ["MONGO_INITDB_DATABASE"] = DbName
            })
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(27017));

        _container = mongoDbContainerBuilder.Build();
    }

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        var mongoClient = new MongoClient(ConnectionString);
        await mongoClient.GetDatabase("admin").RunCommandAsync<dynamic>(new MongoDB.Bson.BsonDocument("ping", 1));
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
                DatabaseName = DbName
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