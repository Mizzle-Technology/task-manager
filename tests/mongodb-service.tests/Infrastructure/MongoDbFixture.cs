using System;
using System.Threading.Tasks;
using mongodb_service.Configuration;
using Testcontainers.MongoDb;
using MongoDB.Driver;
using Xunit;

namespace mongodb_service.tests.Infrastructure;

// Define collection fixture for MongoDB
public class MongoDbFixture : IAsyncLifetime
{
	private readonly MongoDbContainer _container;
	private string? _connectionString;

	public string ConnectionString => _connectionString ?? throw new InvalidOperationException("Container not initialized");
	public MongoClient MongoClient { get; private set; } = null!;

	public MongoDbFixture()
	{
		_container = new MongoDbBuilder()
				.WithImage("mongo:6.0")
				.WithEnvironment("MONGO_INITDB_ROOT_USERNAME", "root")
				.WithEnvironment("MONGO_INITDB_ROOT_PASSWORD", "example")
				.WithPortBinding(27017, true)
				.WithDockerEndpoint("unix:///var/run/docker.sock")
				.WithReuse(true) // Enable container reuse
				.Build();
	}

	public async Task InitializeAsync()
	{
		await _container.StartAsync();
		var baseConnectionString = _container.GetConnectionString();

		var builder = new MongoUrlBuilder(baseConnectionString)
		{
			Username = "root",
			Password = "example",
			AuthenticationSource = "admin"
		};
		_connectionString = builder.ToString();
		MongoClient = new MongoClient(_connectionString);
	}

	public async Task DisposeAsync()
	{
		await _container.DisposeAsync();
	}

	public MongoDbSettings GetMongoDbSettings()
	{
		return new MongoDbSettings
		{
			ConnectionString = ConnectionString,
			DatabaseName = "test-db",
			StaleTaskTimeout = TimeSpan.FromMinutes(5).ToString()
		};
	}
}

// Collection definition
[CollectionDefinition("MongoDB Collection", DisableParallelization = true)]
public class MongoDbCollection : ICollectionFixture<MongoDbFixture>
{
	// This class has no code, and is never created. Its purpose is to be the place
	// where we apply [CollectionDefinition] and all the ICollectionFixture<> interfaces.
}