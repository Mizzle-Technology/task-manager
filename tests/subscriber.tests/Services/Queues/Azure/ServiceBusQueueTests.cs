using System;
using System.Linq;
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
using Testcontainers.ServiceBus;
using Testcontainers.Containers;

namespace subscriber.tests.Services.Queues.Azure;

[Collection("ServiceBus Collection")]
public class ServiceBusQueueTests : IAsyncDisposable
{
	private readonly ServiceBusTestContainer _fixture;
	private readonly ILogger<ServiceBusQueue> _logger;
	private readonly string _queueName = "test-messages-queue";
	private ServiceBusClient _serviceBusClient;
	private ServiceBusReceiver _receiver;
	private ServiceBusSender _sender;

	public ServiceBusQueueTests(ServiceBusTestContainer fixture)
	{
		_fixture = fixture;

		var loggerMock = new Mock<ILogger<ServiceBusQueue>>();
		_logger = loggerMock.Object;

		// Setup real service bus client with test container
		var configuration = _fixture.GetServiceBusConfiguration(new[] { _queueName });

		// Create the test queue
		CreateTestQueue(_queueName).GetAwaiter().GetResult();

		// Initialize the client
		_serviceBusClient = new ServiceBusClient(configuration.ConnectionString);
		_receiver = _serviceBusClient.CreateReceiver(_queueName);
		_sender = _serviceBusClient.CreateSender(_queueName);

		// Send some test messages
		SendTestMessagesAsync().GetAwaiter().GetResult();
	}

	private async Task CreateTestQueue(string queueName)
	{
		try
		{
			// Create a service bus client to create the queue
			var client = new ServiceBusAdministrationClient(_fixture.ConnectionString);

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

	private async Task SendTestMessagesAsync()
	{
		try
		{
			// Send test messages
			await _sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("This is test message 1"))
			{
				MessageId = "test-msg-1",
				ApplicationProperties = { { "TestProperty", "TestValue" } }
			});

			await _sender.SendMessageAsync(new ServiceBusMessage(BinaryData.FromString("This is test message 2"))
			{
				MessageId = "test-msg-2"
			});

			Console.WriteLine("Test messages sent to queue.");
		}
		catch (Exception ex)
		{
			Console.WriteLine($"Error sending test messages: {ex.Message}");
		}
	}

	[Fact]
	public async Task ReceiveMessagesAsync_ShouldReturnMessages()
	{
		// Arrange
		var queue = new ServiceBusQueue(_logger, _receiver);

		// Act
		var messages = await queue.ReceiveMessagesAsync(
				maxMessages: 10,
				visibilityTimeout: TimeSpan.FromSeconds(30),
				cancellationToken: CancellationToken.None);

		// Assert
		var messageList = messages.ToList();
		Assert.True(messageList.Count > 0, "Should receive at least one message");
		if (messageList.Count > 0)
		{
			// Messages may not come in the same order we sent them
			var firstMessage = messageList[0];
			Assert.NotNull(firstMessage.MessageId);
			Assert.NotEmpty(firstMessage.Body);
		}
	}

	[Fact]
	public async Task CompleteMessageAsync_ShouldRemoveMessageFromQueue()
	{
		// Arrange
		var queue = new ServiceBusQueue(_logger, _receiver);

		// Receive a message
		var messages = await queue.ReceiveMessagesAsync(1, TimeSpan.FromSeconds(5), CancellationToken.None);
		var message = messages.FirstOrDefault();

		if (message == null)
		{
			// No messages available, manually create a test message
			await SendTestMessagesAsync();
			messages = await queue.ReceiveMessagesAsync(1, TimeSpan.FromSeconds(5), CancellationToken.None);
			message = messages.FirstOrDefault();
		}

		// Skip test if no messages can be received
		Assert.True(message != null, "Failed to receive a message for testing");
		if (message == null) return;

		// Store the message ID for later verification
		var completedMessageId = message.MessageId;

		// Act
		await queue.CompleteMessageAsync(message, CancellationToken.None);

		// Assert - Attempt to receive the same message again
		// Wait a short period for the completion to take effect
		await Task.Delay(500);

		// Try to receive messages again, should not contain our completed message
		var remainingMessages = await queue.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(5), CancellationToken.None);
		var matchingMessage = remainingMessages.FirstOrDefault(m => m.MessageId == completedMessageId);

		Assert.Null(matchingMessage, $"Message {completedMessageId} should have been removed from the queue after completion");
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