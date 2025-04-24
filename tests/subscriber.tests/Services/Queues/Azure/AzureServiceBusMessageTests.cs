using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using subscriber.Services.Queues.Azure;
using subscriber.tests.Infrastructure;
using Xunit;

namespace subscriber.tests.Services.Queues.Azure;

[Collection("ServiceBus Collection")]
public class AzureServiceBusMessageTests : IAsyncDisposable
{
	private readonly ServiceBusTestContainer _fixture;
	private readonly string _queueName = "test-message-queue";
	private ServiceBusClient _serviceBusClient;
	private ServiceBusSender _sender;
	private ServiceBusReceiver _receiver;

	public AzureServiceBusMessageTests(ServiceBusTestContainer fixture)
	{
		_fixture = fixture;

		// Create the test resources
		var configuration = _fixture.GetServiceBusConfiguration(new[] { _queueName });
		_serviceBusClient = new ServiceBusClient(configuration.ConnectionString);

		// Create the test queue
		CreateTestQueueAsync().GetAwaiter().GetResult();

		_sender = _serviceBusClient.CreateSender(_queueName);
		_receiver = _serviceBusClient.CreateReceiver(_queueName);
	}

	private async Task CreateTestQueueAsync()
	{
		try
		{
			var client = new ServiceBusAdministrationClient(_fixture.ConnectionString);
			if (!await client.QueueExistsAsync(_queueName))
			{
				await client.CreateQueueAsync(_queueName);
				Console.WriteLine($"Queue {_queueName} created for testing.");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error creating queue: {ex.Message}");
		}
	}

	[Fact]
	public async Task Properties_ShouldMapFromServiceBusMessage()
	{
		// Arrange
		var messageId = "test-message-id";
		var body = "Test message body";

		// Create and send a test message
		var message = new ServiceBusMessage(BinaryData.FromString(body))
		{
			MessageId = messageId,
			ApplicationProperties =
			{
				{ "TestProperty1", "Value1" },
				{ "TestProperty2", 42 }
			}
		};

		await _sender.SendMessageAsync(message);

		// Receive the message
		var receivedMessage = await _receiver.ReceiveMessageAsync();

		// Act
		var azureMessage = new AzureServiceBusMessage(receivedMessage);

		// Assert
		Assert.Equal(messageId, azureMessage.MessageId);
		Assert.Equal(body, azureMessage.Body);
		Assert.NotNull(azureMessage.ReceiptHandle);
		Assert.NotEqual(default, azureMessage.EnqueuedTime);
		Assert.Equal(1u, azureMessage.DeliveryCount);
		Assert.Equal(2, azureMessage.Properties.Count);
		Assert.Equal("Value1", azureMessage.Properties["TestProperty1"]);
		Assert.Equal("42", azureMessage.Properties["TestProperty2"]);

		// Complete the message so it doesn't stay in the queue
		await _receiver.CompleteMessageAsync(receivedMessage);
	}

	[Fact]
	public async Task Properties_WhenNoProperties_ShouldReturnEmptyDictionary()
	{
		// Arrange
		var messageId = "test-message-no-props";
		var body = "Test message with no properties";

		// Create and send a test message
		var message = new ServiceBusMessage(BinaryData.FromString(body))
		{
			MessageId = messageId
			// No properties added
		};

		await _sender.SendMessageAsync(message);

		// Receive the message
		var receivedMessage = await _receiver.ReceiveMessageAsync();

		// Act
		var azureMessage = new AzureServiceBusMessage(receivedMessage);

		// Assert
		Assert.NotNull(azureMessage.Properties);
		Assert.Empty(azureMessage.Properties);

		// Complete the message so it doesn't stay in the queue
		await _receiver.CompleteMessageAsync(receivedMessage);
	}

	public async ValueTask DisposeAsync()
	{
		try
		{
			// Clean up receivers and senders
			if (_receiver != null)
				await _receiver.DisposeAsync();

			if (_sender != null)
				await _sender.DisposeAsync();

			if (_serviceBusClient != null)
				await _serviceBusClient.DisposeAsync();

			// Delete the test queue
			var client = new ServiceBusAdministrationClient(_fixture.ConnectionString);
			if (await client.QueueExistsAsync(_queueName))
			{
				await client.DeleteQueueAsync(_queueName);
				Console.WriteLine($"Cleaned up test queue: {_queueName}");
			}
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error during cleanup: {ex.Message}");
		}
	}
}