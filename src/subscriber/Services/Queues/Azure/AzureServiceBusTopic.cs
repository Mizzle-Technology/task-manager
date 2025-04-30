using System.Text;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Microsoft.Extensions.Logging; // Assuming standard Microsoft logging
using subscriber.Services.Queues.Exceptions;
using subscriber.Configuration.ServiceBus;

namespace subscriber.Services.Queues.Azure
{
	/// <summary>
	/// Represents a message received from a Service Bus topic subscription
	/// Implements IQueueMessage and holds the original message for settlement.
	/// </summary>
	public class TopicSubscriptionMessage(ServiceBusReceivedMessage message, string subscriptionName) : IQueueMessage
	{
		// Keep the original message private but accessible internally if needed,
		// or provide a specific method/property for the client implementation
		// For simplicity here, we make it accessible via a property for the client.
		internal ServiceBusReceivedMessage OriginalMessage { get; } = message ?? throw new ArgumentNullException(nameof(message));

		public string MessageId => OriginalMessage.MessageId;

		// Provide Body as UTF8 string for convenience
		public string Body => Encoding.UTF8.GetString(OriginalMessage.Body.ToArray());

		// Expose raw bytes
		public ReadOnlyMemory<byte> BodyBytes => OriginalMessage.Body;

		public DateTime EnqueuedTime => OriginalMessage.EnqueuedTime.UtcDateTime;
		public string ReceiptHandle => OriginalMessage.LockToken;
		public uint DeliveryCount => (uint)OriginalMessage.DeliveryCount;
		public IDictionary<string, string> Properties =>
				OriginalMessage.ApplicationProperties?.ToDictionary(
						p => p.Key,
						p => p.Value?.ToString() ?? string.Empty)
				?? new Dictionary<string, string>();

		// Store the subscription name the message came from
		public string SubscriptionName { get; } = subscriptionName ?? throw new ArgumentNullException(nameof(subscriptionName));
	}

	/// <summary>
	/// Manages Azure Service Bus topics and subscriptions
	/// </summary>
	public interface ITopicClient
	{
		/// <summary>
		/// Initialize the topic client (create topic/subscriptions if needed)
		/// </summary>
		Task InitializeAsync(CancellationToken cancellationToken);

		/// <summary>
		/// Publish a message to the topic
		/// </summary>
		Task PublishMessageAsync(string messageBody, IDictionary<string, string>? properties = null, CancellationToken cancellationToken = default);

		/// <summary>
		/// Receive messages from a specific subscription.
		/// Messages received via this method must be explicitly completed, abandoned, or dead-lettered
		/// using the corresponding methods below before their lock expires.
		/// </summary>
		/// <param name="subscriptionName">The name of the subscription to receive from.</param>
		/// <param name="maxMessages">The maximum number of messages to receive.</param>
		/// <param name="maxWaitTime">The maximum time to wait for messages to arrive. Null means wait indefinitely (or until cancellation).</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		/// <returns>A collection of messages received.</returns>
		Task<IEnumerable<TopicSubscriptionMessage>> ReceiveMessagesFromSubscriptionAsync(
				string subscriptionName,
				int maxMessages,
				TimeSpan? maxWaitTime, // Renamed from visibilityTimeout, made nullable
				CancellationToken cancellationToken);

		/// <summary>
		/// Complete a message (mark as processed).
		/// </summary>
		/// <param name="message">The message received from ReceiveMessagesFromSubscriptionAsync.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		Task CompleteMessageAsync(TopicSubscriptionMessage message, CancellationToken cancellationToken);

		/// <summary>
		/// Abandon a message (return to subscription for later processing).
		/// </summary>
		/// <param name="message">The message received from ReceiveMessagesFromSubscriptionAsync.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		Task AbandonMessageAsync(TopicSubscriptionMessage message, CancellationToken cancellationToken);

		/// <summary>
		/// Dead-letter a message (move to Dead Letter Queue).
		/// </summary>
		/// <param name="message">The message received from ReceiveMessagesFromSubscriptionAsync.</param>
		/// <param name="reason">The reason for dead-lettering.</param>
		/// <param name="cancellationToken">Cancellation token.</param>
		Task DeadLetterMessageAsync(TopicSubscriptionMessage message, string reason, CancellationToken cancellationToken);
	}

	/// <summary>
	/// Implementation of Service Bus topic client
	/// </summary>
	public class AzureServiceBusTopic : ITopicClient, IAsyncDisposable
	{
		private readonly ILogger<AzureServiceBusTopic> _logger;
		private readonly ServiceBusClient _client;
		private readonly ServiceBusTopicConfiguration _config;
		private ServiceBusSender? _topicSender;
		// Store receivers, keyed by subscription name
		private readonly Dictionary<string, ServiceBusReceiver> _subscriptionReceivers = new();
		// REMOVED: No longer storing received messages state here
		// private readonly Dictionary<string, Dictionary<string, ServiceBusReceivedMessage>> _receivedMessages = new();
		private bool _isInitialized;

		public AzureServiceBusTopic(
				ILogger<AzureServiceBusTopic> logger,
				ServiceBusTopicConfiguration config)
		{
			_logger = logger ?? throw new ArgumentNullException(nameof(logger));
			_config = config ?? throw new ArgumentNullException(nameof(config));

			if (string.IsNullOrEmpty(_config.ConnectionString))
				throw new ArgumentException("Connection string cannot be empty", nameof(config));
			if (string.IsNullOrEmpty(_config.TopicName))
				throw new ArgumentException("Topic name cannot be empty", nameof(config));
			if (_config.SubscriptionNames == null || _config.SubscriptionNames.Length == 0)
				_logger.LogWarning("No subscription names provided in configuration for topic {TopicName}.", _config.TopicName);


			// Consider adding ServiceBusClientOptions here if needed (e.g., RetryOptions)
			_client = new ServiceBusClient(_config.ConnectionString);
		}

		public async Task InitializeAsync(CancellationToken cancellationToken)
		{
			if (_isInitialized) return;

			try
			{
				var adminClient = new ServiceBusAdministrationClient(_config.ConnectionString);

				// Create topic if it doesn't exist
				if (!await adminClient.TopicExistsAsync(_config.TopicName, cancellationToken))
				{
					_logger.LogInformation("Creating topic: {TopicName}", _config.TopicName);
					var topicOptions = new CreateTopicOptions(_config.TopicName)
					{
						DefaultMessageTimeToLive = _config.MessageTimeToLive,
						EnableBatchedOperations = true
					};
					await adminClient.CreateTopicAsync(topicOptions, cancellationToken);
				}
				else
				{
					_logger.LogInformation("Topic {TopicName} already exists.", _config.TopicName);
				}


				// Create subscriptions and receivers
				foreach (var subscriptionName in _config.SubscriptionNames ?? Enumerable.Empty<string>())
				{
					if (string.IsNullOrWhiteSpace(subscriptionName))
					{
						_logger.LogWarning("Skipping empty or whitespace subscription name.");
						continue;
					}

					if (!await adminClient.SubscriptionExistsAsync(_config.TopicName, subscriptionName, cancellationToken))
					{
						_logger.LogInformation("Creating subscription: {SubscriptionName} for topic: {TopicName}",
								subscriptionName, _config.TopicName);
						var subscriptionOptions = new CreateSubscriptionOptions(_config.TopicName, subscriptionName)
						{
							DefaultMessageTimeToLive = _config.SubscriptionMessageTimeToLive,
							LockDuration = _config.LockDuration, // Crucial setting
							EnableBatchedOperations = true,
							// Consider DeadLetteringOnMessageExpiration = true/false based on requirements
						};
						await adminClient.CreateSubscriptionAsync(subscriptionOptions, cancellationToken);
					}
					else
					{
						_logger.LogInformation("Subscription {SubscriptionName} for topic {TopicName} already exists.", subscriptionName, _config.TopicName);
					}


					// Create and store receiver for this subscription
					// Only create if not already added (e.g., if InitializeAsync is called multiple times, though guarded by _isInitialized)
					if (!_subscriptionReceivers.ContainsKey(subscriptionName))
					{
						// Consider adding ServiceBusReceiverOptions if needed (e.g., ReceiveMode, PrefetchCount)
						var receiver = _client.CreateReceiver(_config.TopicName, subscriptionName);
						_subscriptionReceivers[subscriptionName] = receiver;
						_logger.LogDebug("Receiver created for subscription {SubscriptionName}.", subscriptionName);
					}
				}

				// Create the topic sender if not already created
				if (_topicSender == null)
				{
					// Consider adding ServiceBusSenderOptions if needed
					_topicSender = _client.CreateSender(_config.TopicName);
					_logger.LogDebug("Sender created for topic {TopicName}.", _config.TopicName);
				}


				_isInitialized = true;
				_logger.LogInformation("Topic client initialized for topic: {TopicName} with {SubscriptionCount} configured subscriptions.",
						_config.TopicName, _subscriptionReceivers.Count);
			}
			// Catch specific Azure exceptions if needed for more granular logging/handling
			catch (ServiceBusException sbEx)
			{
				_logger.LogError(sbEx, "Service Bus error during initialization for topic {TopicName}: {Reason}", _config.TopicName, sbEx.Reason);
				throw new QueueOperationException($"Service Bus error during initialization: {sbEx.Reason}", sbEx);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to initialize Service Bus topic client for topic {TopicName}", _config.TopicName);
				throw new QueueOperationException("Failed to initialize Service Bus topic client", ex);
			}
		}

		public async Task PublishMessageAsync(
				string messageBody,
				IDictionary<string, string>? properties = null,
				CancellationToken cancellationToken = default)
		{
			EnsureInitialized();

			if (_topicSender == null) // Should not happen if initialized correctly, but good practice
				throw new InvalidOperationException("Topic sender is not available. Initialization might have failed partially.");


			try
			{
				var message = new ServiceBusMessage(messageBody)
				{
					MessageId = Guid.NewGuid().ToString() // Service Bus assigns one if not set
				};

				if (properties != null)
				{
					foreach (var prop in properties)
					{
						// Ensure value is not null, Service Bus properties cannot be null
						message.ApplicationProperties.Add(prop.Key, prop.Value ?? string.Empty);
					}
				}

				await _topicSender.SendMessageAsync(message, cancellationToken);
				_logger.LogDebug("Message {MessageId} published to topic {TopicName}", message.MessageId, _config.TopicName);
			}
			catch (ServiceBusException sbEx)
			{
				_logger.LogError(sbEx, "Failed to publish message to topic {TopicName}: {Reason}", _config.TopicName, sbEx.Reason);
				throw new QueueOperationException($"Failed to publish message to topic {_config.TopicName}: {sbEx.Reason}", sbEx);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to publish message to topic {TopicName}", _config.TopicName);
				throw new QueueOperationException($"Failed to publish message to topic {_config.TopicName}", ex);
			}
		}

		public async Task<IEnumerable<TopicSubscriptionMessage>> ReceiveMessagesFromSubscriptionAsync(
				string subscriptionName,
				int maxMessages,
				TimeSpan? maxWaitTime, // Use the renamed parameter
				CancellationToken cancellationToken)
		{
			EnsureInitialized();
			var receiver = GetReceiver(subscriptionName); // Gets receiver or throws

			try
			{
				// Receive raw messages
				var receivedMessages = await receiver.ReceiveMessagesAsync(
						maxMessages,
						maxWaitTime, // Pass the maxWaitTime
						cancellationToken);

				if (receivedMessages == null || !receivedMessages.Any())
				{
					return Enumerable.Empty<TopicSubscriptionMessage>();
				}

				_logger.LogDebug("Received {Count} messages from subscription {SubscriptionName}", receivedMessages.Count, subscriptionName);

				// Map to our TopicSubscriptionMessage wrapper, including the subscription name
				// The caller is now responsible for handling these messages and their settlement
				return receivedMessages.Select(m => new TopicSubscriptionMessage(m, subscriptionName)).ToList();
			}
			catch (ServiceBusException sbEx) when (sbEx.Reason == ServiceBusFailureReason.ServiceTimeout)
			{
				// This is expected if maxWaitTime is set and no messages arrive
				_logger.LogDebug("No messages received from subscription {SubscriptionName} within the specified wait time.", subscriptionName);
				return Enumerable.Empty<TopicSubscriptionMessage>();
			}
			catch (ServiceBusException sbEx)
			{
				_logger.LogError(sbEx, "Failed to receive messages from subscription {SubscriptionName}: {Reason}", subscriptionName, sbEx.Reason);
				throw new QueueOperationException($"Failed to receive messages from subscription {subscriptionName}: {sbEx.Reason}", sbEx);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to receive messages from subscription {SubscriptionName}", subscriptionName);
				throw new QueueOperationException($"Failed to receive messages from subscription {subscriptionName}", ex);
			}
		}

		// --- Settlement Methods ---

		public async Task CompleteMessageAsync(
				TopicSubscriptionMessage message, // Use the wrapper object
				CancellationToken cancellationToken)
		{
			EnsureInitialized();
			ArgumentNullException.ThrowIfNull(message);

			var receiver = GetReceiver(message.SubscriptionName);

			try
			{
				// Use the original message object stored in the wrapper
				await receiver.CompleteMessageAsync(message.OriginalMessage, cancellationToken);

				_logger.LogDebug("Message {MessageId} completed from subscription {SubscriptionName}",
						message.MessageId, message.SubscriptionName);
			}
			catch (ServiceBusException sbEx) when (IsLockLostException(sbEx))
			{
				_logger.LogError(sbEx, "Failed to complete message {MessageId} from subscription {SubscriptionName} due to lock lost: {Reason}",
					 message.MessageId, message.SubscriptionName, sbEx.Reason);
				// Lock lost means the message might be processed again by another consumer.
				// Throw a specific exception or handle appropriately based on idempotency requirements.
				throw new MessageLockLostException($"Lock lost for message {message.MessageId} on subscription {message.SubscriptionName}.", sbEx);
			}
			catch (ServiceBusException sbEx)
			{
				_logger.LogError(sbEx, "Failed to complete message {MessageId} from subscription {SubscriptionName}: {Reason}",
						message.MessageId, message.SubscriptionName, sbEx.Reason);
				throw new QueueOperationException($"Failed to complete message {message.MessageId}: {sbEx.Reason}", sbEx);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to complete message {MessageId} from subscription {SubscriptionName}",
						message.MessageId, message.SubscriptionName);
				throw new QueueOperationException($"Failed to complete message {message.MessageId}", ex);
			}
		}

		public async Task AbandonMessageAsync(
				TopicSubscriptionMessage message, // Use the wrapper object
				CancellationToken cancellationToken)
		{
			EnsureInitialized();
			ArgumentNullException.ThrowIfNull(message);

			var receiver = GetReceiver(message.SubscriptionName);

			try
			{
				await receiver.AbandonMessageAsync(message.OriginalMessage, cancellationToken: cancellationToken);

				_logger.LogDebug("Message {MessageId} abandoned from subscription {SubscriptionName}",
						message.MessageId, message.SubscriptionName);
			}
			catch (ServiceBusException sbEx) when (IsLockLostException(sbEx))
			{
				_logger.LogWarning(sbEx, "Failed to abandon message {MessageId} from subscription {SubscriptionName} due to lock lost: {Reason}. Message might be processed again.",
					 message.MessageId, message.SubscriptionName, sbEx.Reason);
				// Lock is already lost, abandoning might not be possible/necessary, but log it.
				// Depending on requirements, you might not need to re-throw here, or throw MessageLockLostException
				// throw new MessageLockLostException($"Lock lost for message {message.MessageId} on subscription {message.SubscriptionName}.", sbEx);
			}
			catch (ServiceBusException sbEx)
			{
				_logger.LogError(sbEx, "Failed to abandon message {MessageId} from subscription {SubscriptionName}: {Reason}",
						message.MessageId, message.SubscriptionName, sbEx.Reason);
				throw new QueueOperationException($"Failed to abandon message {message.MessageId}: {sbEx.Reason}", sbEx);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to abandon message {MessageId} from subscription {SubscriptionName}",
						message.MessageId, message.SubscriptionName);
				throw new QueueOperationException($"Failed to abandon message {message.MessageId}", ex);
			}
		}

		public async Task DeadLetterMessageAsync(
				TopicSubscriptionMessage message, // Use the wrapper object
				string reason,
				CancellationToken cancellationToken)
		{
			EnsureInitialized();
			ArgumentNullException.ThrowIfNull(message);

			var receiver = GetReceiver(message.SubscriptionName);

			try
			{
				// Optionally add more details to propertiesToModify if needed
				await receiver.DeadLetterMessageAsync(
						message.OriginalMessage,
						deadLetterReason: reason, // Use the provided reason
						cancellationToken: cancellationToken);

				_logger.LogWarning("Message {MessageId} dead-lettered from subscription {SubscriptionName}: {Reason}",
						message.MessageId, message.SubscriptionName, reason);
			}
			catch (ServiceBusException sbEx) when (IsLockLostException(sbEx))
			{
				_logger.LogError(sbEx, "Failed to dead-letter message {MessageId} from subscription {SubscriptionName} due to lock lost: {Reason}. Message might be processed again.",
					 message.MessageId, message.SubscriptionName, sbEx.Reason);
				// Lock lost means the message might be processed again by another consumer.
				// Throw a specific exception or handle appropriately.
				throw new MessageLockLostException($"Lock lost for message {message.MessageId} on subscription {message.SubscriptionName}.", sbEx);
			}
			catch (ServiceBusException sbEx)
			{
				_logger.LogError(sbEx, "Failed to dead-letter message {MessageId} from subscription {SubscriptionName}: {Reason}",
						message.MessageId, message.SubscriptionName, sbEx.Reason);
				throw new QueueOperationException($"Failed to dead-letter message {message.MessageId}: {sbEx.Reason}", sbEx);
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Failed to dead-letter message {MessageId} from subscription {SubscriptionName}",
						message.MessageId, message.SubscriptionName);
				throw new QueueOperationException($"Failed to dead-letter message {message.MessageId}", ex);
			}
		}

		// --- Helper Methods ---
		private void EnsureInitialized()
		{
			if (!_isInitialized)
				throw new InvalidOperationException("Topic client not initialized. Call InitializeAsync first.");
		}

		private ServiceBusReceiver GetReceiver(string subscriptionName)
		{
			if (string.IsNullOrWhiteSpace(subscriptionName))
				throw new ArgumentException("Subscription name cannot be empty.", nameof(subscriptionName));

			if (!_subscriptionReceivers.TryGetValue(subscriptionName, out var receiver))
				throw new QueueNotFoundException($"Subscription receiver for '{subscriptionName}' not found or not initialized. Ensure it's in the configuration and InitializeAsync has been called.");

			// Check if receiver is closed (can happen after DisposeAsync or certain errors)
			if (receiver.IsClosed)
			{
				_logger.LogWarning("Receiver for subscription {SubscriptionName} is closed. Recreating the receiver.", subscriptionName);

				// Recreate the receiver
				try
				{
					// Remove the old closed receiver
					_subscriptionReceivers.Remove(subscriptionName);

					// Create a new receiver
					var newReceiver = _client.CreateReceiver(_config.TopicName, subscriptionName);
					_subscriptionReceivers[subscriptionName] = newReceiver;

					_logger.LogInformation("Successfully recreated receiver for subscription {SubscriptionName}", subscriptionName);
					return newReceiver;
				}
				catch (Exception ex)
				{
					_logger.LogError(ex, "Failed to recreate receiver for subscription {SubscriptionName}", subscriptionName);
					throw new QueueOperationException($"Failed to recreate receiver for subscription {subscriptionName}", ex);
				}
			}

			return receiver;
		}

		private static bool IsLockLostException(ServiceBusException sbException)
		{
			return sbException.Reason == ServiceBusFailureReason.MessageLockLost ||
						 sbException.Reason == ServiceBusFailureReason.SessionLockLost;
		}

		// --- Disposal ---
		public async ValueTask DisposeAsync()
		{
			_logger.LogInformation("Disposing AzureServiceBusTopic client for {TopicName}...", _config.TopicName);
			if (_topicSender != null)
			{
				await _topicSender.DisposeAsync().ConfigureAwait(false);
				_logger.LogDebug("Topic sender disposed.");
			}

			// Use ToList to avoid modification issues while iterating if recreation logic were added
			foreach (var receiver in _subscriptionReceivers.Values.ToList())
			{
				if (!receiver.IsClosed) // Avoid disposing already closed receivers
				{
					await receiver.DisposeAsync().ConfigureAwait(false);
				}
			}
			_subscriptionReceivers.Clear(); // Clear the dictionary after disposing
			_logger.LogDebug("Subscription receivers disposed.");


			if (_client != null)
			{
				await _client.DisposeAsync().ConfigureAwait(false);
				_logger.LogDebug("Service Bus client disposed.");
			}
			_isInitialized = false; // Mark as not initialized after disposal
			_logger.LogInformation("AzureServiceBusTopic client disposed.");
		}
	}
}
