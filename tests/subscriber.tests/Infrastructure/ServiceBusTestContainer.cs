using System;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.Logging;
using Testcontainers.ServiceBus;
using Xunit;

namespace subscriber.tests.Infrastructure;

/// <summary>
/// Fixture for testing with Azure Service Bus emulator in a Docker container
/// </summary>
public class ServiceBusTestContainer : IAsyncLifetime
{
	private readonly ServiceBusContainer _container;

	public ServiceBusTestContainer()
	{
		_container = new ServiceBusBuilder()
				.WithImage("mcr.microsoft.com/azure-messaging/servicebus-emulator:latest")
				.WithName($"servicebus-test-{Guid.NewGuid()}")
				.WithPortBinding(ServiceBusBuilder.ServiceBusPort, true) // AMQP port (5672)
				.WithPortBinding(ServiceBusBuilder.ServiceBusHttpPort, true) // HTTP port for health check (5300)
				.WithWaitStrategy(Wait.ForUnixContainer().UntilHttpRequestIsSucceeded(request =>
					request.ForPort(ServiceBusBuilder.ServiceBusHttpPort).ForPath("/health")))
				.Build();
	}

	public string ConnectionString => _container.GetConnectionString();

	public async Task InitializeAsync()
	{
		await _container.StartAsync();
		Console.WriteLine($"Service Bus container started. Connection string: {ConnectionString}");
	}

	public async Task DisposeAsync()
	{
		await _container.DisposeAsync();
	}

	/// <summary>
	/// Creates a configuration for use in tests
	/// </summary>
	public subscriber.Services.Queues.Azure.ServiceBusConfiguration GetServiceBusConfiguration(string[] queueNames)
	{
		return new subscriber.Services.Queues.Azure.ServiceBusConfiguration
		{
			ConnectionString = ConnectionString,
			QueueNames = queueNames,
			MaxLockDuration = TimeSpan.FromMinutes(5),
			MaxDeliveryCount = 10
		};
	}
}

[CollectionDefinition("ServiceBus Collection")]
public class ServiceBusCollection : ICollectionFixture<ServiceBusTestContainer>
{
	// This class has no code, and is never created.
	// Its purpose is to be the place to apply [CollectionDefinition] and all the
	// ICollectionFixture<> interfaces.
}