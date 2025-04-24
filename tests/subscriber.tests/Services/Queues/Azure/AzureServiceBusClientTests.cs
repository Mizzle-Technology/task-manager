using System;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using subscriber.Services.Queues;
using subscriber.Services.Queues.Azure;
using subscriber.Services.Queues.Exceptions;
using subscriber.tests.Infrastructure;
using Xunit;
using Moq;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace subscriber.tests.Services.Queues.Azure;

[Collection("ServiceBus Collection")]
public class AzureServiceBusClientTests : IAsyncDisposable
{
	private readonly ServiceBusTestContainer _fixture;
	private readonly ILogger<AzureServiceBusClient> _logger;
	private readonly ILoggerFactory _loggerFactory;
	private ServiceBusConfiguration _configuration;
	private string[] _queueNames = new[] { "test-queue" };

	public AzureServiceBusClientTests(ServiceBusTestContainer fixture)
	{
		_fixture = fixture;

		// Create the logger and factory
		var loggerMock = new Mock<ILogger<AzureServiceBusClient>>();
		_logger = loggerMock.Object;

		var loggerFactoryMock = new Mock<ILoggerFactory>();
		loggerFactoryMock
				.Setup(f => f.CreateLogger(It.IsAny<string>()))
				.Returns(new Mock<ILogger>().Object);
		_loggerFactory = loggerFactoryMock.Object;

		// Create the service configuration
		_configuration = _fixture.GetServiceBusConfiguration(_queueNames);

		// Create the test queue
		CreateTestQueue("test-queue").GetAwaiter().GetResult();
	}

	private async Task CreateTestQueue(string queueName)
	{
		try
		{
			// Create a service bus client to create the queue
			var client = new ServiceBusAdministrationClient(_configuration.ConnectionString);

			// Check if queue exists
			if (!await client.QueueExistsAsync(queueName))
			{
				// Create the queue
				await client.CreateQueueAsync(queueName);
				Console.WriteLine($"Queue {queueName} created for testing.");
			}
			else
			{
				Console.WriteLine($"Queue {queueName} already exists.");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error creating queue: {ex.Message}");
		}
	}

	[Fact]
	public void Initialize_ShouldCreateQueueInstances()
	{
		// Arrange
		var options = Options.Create(_configuration);
		var client = new AzureServiceBusClient(_logger, _loggerFactory, options);

		// Act
		client.Initialize(CancellationToken.None);

		// Assert
		var queue = client.GetQueue("test-queue");
		Assert.NotNull(queue);
	}

	[Fact]
	public void GetQueue_WhenNotInitialized_ShouldThrowException()
	{
		// Arrange
		var options = Options.Create(_configuration);
		var client = new AzureServiceBusClient(_logger, _loggerFactory, options);

		// Act & Assert
		Assert.Throws<InvalidOperationException>(() => client.GetQueue("test-queue"));
	}

	[Fact]
	public void GetQueue_WithNonExistentQueue_ShouldThrowException()
	{
		// Arrange
		var options = Options.Create(_configuration);
		var client = new AzureServiceBusClient(_logger, _loggerFactory, options);
		client.Initialize(CancellationToken.None);

		// Act & Assert
		Assert.Throws<QueueNotFoundException>(() => client.GetQueue("non-existent-queue"));
	}

	[Fact]
	public async Task GetQueueHealthAsync_ShouldReturnHealthStatus()
	{
		// Arrange
		var options = Options.Create(_configuration);
		var client = new AzureServiceBusClient(_logger, _loggerFactory, options);
		client.Initialize(CancellationToken.None);

		// Act
		var health = await client.GetQueueHealthAsync(CancellationToken.None);

		// Assert
		Assert.NotNull(health);
		Assert.True(health.IsHealthy);
	}

	public async ValueTask DisposeAsync()
	{
		// Clean up test resources
		try
		{
			var client = new ServiceBusAdministrationClient(_configuration.ConnectionString);

			// Delete test queues
			foreach (var queueName in _queueNames)
			{
				if (await client.QueueExistsAsync(queueName))
				{
					await client.DeleteQueueAsync(queueName);
					Console.WriteLine($"Cleaned up test queue: {queueName}");
				}
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error during cleanup: {ex.Message}");
		}
	}
}