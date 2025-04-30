using Quartz;
using subscriber.Services.Queues;
using subscriber.Services.Queues.Azure;
using Microsoft.Extensions.Options;
using mongodb_service.Repositories;
using mongodb_service.Models;
using Polly;
using Polly.Retry;
using System.Diagnostics;
using subscriber.Configuration.Jobs;

namespace subscriber.Jobs;



public class TopicSubscriberJob : IJob
{
	private readonly ILogger<TopicSubscriberJob> _logger;
	private readonly ITopicClientFactory _topicFactory;
	private readonly IMongoDbRepository _mongoDb;
	private readonly TopicSubscriberJobConfiguration _config;
	private readonly ITopicClient _topicClient;
	private readonly AsyncRetryPolicy _retryPolicy;
	private static readonly TimeSpan MessageProcessingTimeout = TimeSpan.FromMinutes(5);

	// Metrics
	private int _totalProcessedCount;
	private int _successCount;
	private int _failureCount;

	public TopicSubscriberJob(
			ILogger<TopicSubscriberJob> logger,
			ITopicClientFactory topicFactory,
			IMongoDbRepository mongoDb,
			IOptions<TopicSubscriberJobConfiguration> config)
	{
		_logger = logger ?? throw new ArgumentNullException(nameof(logger));
		_topicFactory = topicFactory ?? throw new ArgumentNullException(nameof(topicFactory));
		_mongoDb = mongoDb ?? throw new ArgumentNullException(nameof(mongoDb));
		_config = config?.Value ?? throw new ArgumentNullException(nameof(config));

		// Get topic client
		_topicClient = _topicFactory.GetTopicClient();

		_retryPolicy = Policy
				.Handle<Exception>()
				.WaitAndRetryAsync(3,
						retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
						onRetry: (ex, timeSpan, retryCount, _) =>
						{
							_logger.LogWarning(ex,
											"Retry {RetryCount} after {Delay}s due to: {Error}",
											retryCount, timeSpan.TotalSeconds, ex.Message);
						});
	}

	public async Task Execute(IJobExecutionContext context)
	{
		var stopwatch = Stopwatch.StartNew();
		_logger.LogInformation("Starting topic subscriber job at: {time} for topic {topic} subscription {subscription}",
				DateTimeOffset.Now, _config.TopicName, _config.SubscriptionName);

		_totalProcessedCount = 0;
		_successCount = 0;
		_failureCount = 0;

		try
		{
			// Initialize client if needed
			await _topicClient.InitializeAsync(context.CancellationToken);

			var messages = await _topicClient.ReceiveMessagesFromSubscriptionAsync(
					_config.SubscriptionName,
					_config.BatchSize,
					TimeSpan.FromSeconds(_config.PollingWaitSeconds),
					context.CancellationToken);

			var messageList = messages.ToList();
			_totalProcessedCount = messageList.Count;

			if (messageList.Count == 0)
			{
				_logger.LogInformation("No messages pulled from subscription {SubscriptionName}", _config.SubscriptionName);
				return;
			}

			_logger.LogInformation("Pulled {Count} messages from subscription {SubscriptionName}",
					messageList.Count, _config.SubscriptionName);

			// Process messages in parallel
			var tasks = messageList.Select(message => ProcessMessageSafeAsync(message, context.CancellationToken));
			var results = await Task.WhenAll(tasks);

			_successCount = results.Count(r => r);
			_failureCount = _totalProcessedCount - _successCount;

			_logger.LogInformation(
					"Topic subscriber job completed in {ElapsedMs}ms. Processed: {TotalCount}, Success: {SuccessCount}, Failures: {FailureCount}",
					stopwatch.ElapsedMilliseconds,
					_totalProcessedCount,
					_successCount,
					_failureCount
			);
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Error executing topic subscriber job for subscription {SubscriptionName}", _config.SubscriptionName);
			throw;
		}
		finally
		{
			stopwatch.Stop();

			// Log metrics for monitoring
			_logger.LogInformation(
					"Topic subscriber job metrics: Topic={Topic}, Subscription={Subscription}, Elapsed={ElapsedMs}ms, " +
					"TotalMessages={TotalCount}, SuccessRate={SuccessRate}%, " +
					"AvgProcessingTimeMs={AvgProcessingTime}",
					_config.TopicName,
					_config.SubscriptionName,
					stopwatch.ElapsedMilliseconds,
					_totalProcessedCount,
					_totalProcessedCount > 0 ? (_successCount * 100.0 / _totalProcessedCount) : 0,
					_totalProcessedCount > 0 ? (stopwatch.ElapsedMilliseconds / _totalProcessedCount) : 0
			);
		}
	}

	private async Task<bool> ProcessMessageSafeAsync(TopicSubscriptionMessage message, CancellationToken jobCancellation)
	{
		var processingStopwatch = Stopwatch.StartNew();
		using var timeoutCts = new CancellationTokenSource(MessageProcessingTimeout);
		using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
				timeoutCts.Token,
				jobCancellation);

		var messageId = message.MessageId;
		_logger.LogDebug("Processing message {MessageId} from subscription {SubscriptionName}",
				messageId, _config.SubscriptionName);

		try
		{
			// Store the message in MongoDB before acknowledging it
			var task = new TaskEntity
			{
				TaskId = messageId,
				Body = message.Body,
				Status = JobTaskStatus.Processing,
				RetryCount = 0,
				// Add metadata about the source topic/subscription
				Metadata = new Dictionary<string, string>
				{
					["Source"] = "Topic",
					["TopicName"] = _config.TopicName,
					["SubscriptionName"] = _config.SubscriptionName
				}
			};

			// Add message properties to metadata
			foreach (var prop in message.Properties)
			{
				task.Metadata.Add(prop.Key, prop.Value);
			}

			// Insert the task first - before we complete the message from the subscription
			await _mongoDb.InsertOrUpdateTaskAsync(task);
			_logger.LogInformation("Task {TaskId} stored in MongoDB with status {Status}",
					task.TaskId, task.Status);

			// Now process using the retry policy
			var success = await _retryPolicy.ExecuteAsync(async () =>
			{
				// Process the task logic
				await _mongoDb.TryUpdateTaskStatusAsync(task.TaskId, JobTaskStatus.Processing);

				// Simulate actual processing work...
				// Your business logic would go here

				// For example, if you need to process different message types:
				// if (message.Properties.TryGetValue("MessageType", out var messageType))
				// {
				//     switch (messageType)
				//     {
				//         case "UserCreated":
				//             await ProcessUserCreatedMessage(message.Body);
				//             break;
				//         case "OrderPlaced":
				//             await ProcessOrderPlacedMessage(message.Body);
				//             break;
				//     }
				// }

				// Mark as completed in MongoDB
				await _mongoDb.TryUpdateTaskStatusAsync(task.TaskId, JobTaskStatus.Completed);
				return true;
			});

			if (success)
			{
				// Only complete the message from the subscription after successful processing
				await _topicClient.CompleteMessageAsync(message, linkedCts.Token);
				_logger.LogInformation("Message {MessageId} processed successfully in {ElapsedMs}ms",
						messageId, processingStopwatch.ElapsedMilliseconds);
				return true;
			}
			else
			{
				// This should not happen due to retry policy, but handle just in case
				_logger.LogError("Failed to process message {MessageId} after retries", messageId);
				await HandleFailedMessage(message, "Processing failed after retries", linkedCts.Token);
				return false;
			}
		}
		catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
		{
			_logger.LogError("Processing of message {MessageId} timed out after {Timeout}s",
					messageId, MessageProcessingTimeout.TotalSeconds);

			await _mongoDb.TryUpdateTaskStatusAsync(messageId, JobTaskStatus.Failed);

			await HandleFailedMessage(message, "Processing timeout", linkedCts.Token);
			return false;
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to process message {MessageId}", messageId);

			await _mongoDb.TryUpdateTaskStatusAsync(messageId, JobTaskStatus.Failed);

			await HandleFailedMessage(message, $"Exception: {ex.Message}", linkedCts.Token);
			return false;
		}
		finally
		{
			processingStopwatch.Stop();
			_logger.LogDebug("Message {MessageId} processing took {ElapsedMs}ms",
					messageId, processingStopwatch.ElapsedMilliseconds);
		}
	}

	private async Task HandleFailedMessage(TopicSubscriptionMessage message, string reason, CancellationToken cancellationToken)
	{
		try
		{
			if (_config.DeadLetterFailedMessages)
			{
				// Send to dead-letter queue for later investigation
				await _topicClient.DeadLetterMessageAsync(message, reason, cancellationToken);
				_logger.LogWarning("Message {MessageId} sent to dead-letter queue: {Reason}",
						message.MessageId, reason);
			}
			else
			{
				// Return to the queue for reprocessing
				await _topicClient.AbandonMessageAsync(message, cancellationToken);
				_logger.LogWarning("Message {MessageId} abandoned and returned to subscription: {Reason}",
						message.MessageId, reason);
			}
		}
		catch (Exception ex)
		{
			_logger.LogError(ex, "Failed to handle failed message {MessageId}", message.MessageId);
		}
	}
}