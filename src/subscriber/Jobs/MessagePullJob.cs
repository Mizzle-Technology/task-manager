using Quartz;
using subscriber.Services.Queues;

using Microsoft.Extensions.Options;
using mongodb_service.Repositories;
using mongodb_service.Models;
using Polly;
using Polly.Retry;
using System.Diagnostics;

namespace subscriber.Jobs;

public record Message(
    string Id,
    string Content,
    DateTime ReceivedAt,
    string ReceiptHandle
);

public record MessagePullJobConfiguration
{
    public QueueProvider Provider { get; set; } = QueueProvider.AliyunMNS;
    public string QueueName { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 10;
    public int PollingWaitSeconds { get; set; } = 30;
    public bool DeadLetterFailedMessages { get; set; } = true;
}

public class MessagePullJob : IJob
{
    private readonly ILogger<MessagePullJob> _logger;
    private readonly IQueueClientFactory _queueFactory;
    private readonly IMongoDbRepository _mongoDb;
    private readonly MessagePullJobConfiguration _config;
    private readonly IQueueClient _queueClient;
    private readonly AsyncRetryPolicy _retryPolicy;
    private static readonly TimeSpan MessageProcessingTimeout = TimeSpan.FromMinutes(5);

    // Metrics
    private int _totalProcessedCount;
    private int _successCount;
    private int _failureCount;

    public MessagePullJob(
        ILogger<MessagePullJob> logger,
        IQueueClientFactory queueFactory,
        IMongoDbRepository mongoDb,
        IOptions<MessagePullJobConfiguration> config)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _queueFactory = queueFactory ?? throw new ArgumentNullException(nameof(queueFactory));
        _mongoDb = mongoDb ?? throw new ArgumentNullException(nameof(mongoDb));
        _config = config?.Value ?? throw new ArgumentNullException(nameof(config));

        // Get client using the configured provider
        _queueClient = _queueFactory.GetClient(_config.Provider);

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
        _logger.LogInformation("Starting message pull job at: {time} for provider {provider}",
            DateTimeOffset.Now, _config.Provider);

        _totalProcessedCount = 0;
        _successCount = 0;
        _failureCount = 0;

        try
        {
            // Initialize client if needed
            _queueClient.Initialize(context.CancellationToken);

            // Verify queue health before processing
            var health = await _queueClient.GetQueueHealthAsync(context.CancellationToken);
            if (!health.IsHealthy)
            {
                _logger.LogError("Queue {QueueName} is not healthy. Status: {Status}",
                    _config.QueueName, health.Status);
                return;
            }

            var queue = _queueClient.GetQueue(_config.QueueName);
            if (queue == null)
            {
                _logger.LogError("Failed to get queue {QueueName}. Queue might not be initialized",
                    _config.QueueName);
                return;
            }

            var messages = await queue.ReceiveMessagesAsync(
                _config.BatchSize,
                TimeSpan.FromSeconds(_config.PollingWaitSeconds),
                context.CancellationToken);

            var messageList = messages.ToList();
            _totalProcessedCount = messageList.Count;

            if (!messageList.Any())
            {
                _logger.LogInformation("No messages pulled from queue {QueueName}", _config.QueueName);
                return;
            }

            _logger.LogInformation("Pulled {Count} messages from queue {QueueName}",
                messageList.Count, _config.QueueName);

            // Process messages in parallel
            var tasks = messageList.Select(message => ProcessMessageSafeAsync(message, queue, context.CancellationToken));
            var results = await Task.WhenAll(tasks);

            _successCount = results.Count(r => r);
            _failureCount = _totalProcessedCount - _successCount;

            _logger.LogInformation(
                "Message pull job completed in {ElapsedMs}ms. Processed: {TotalCount}, Success: {SuccessCount}, Failures: {FailureCount}",
                stopwatch.ElapsedMilliseconds,
                _totalProcessedCount,
                _successCount,
                _failureCount
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing message pull job for queue {QueueName}", _config.QueueName);
            throw;
        }
        finally
        {
            stopwatch.Stop();

            // Log metrics for monitoring
            _logger.LogInformation(
                "Message pull job metrics: Provider={Provider}, Queue={QueueName}, Elapsed={ElapsedMs}ms, " +
                "TotalMessages={TotalCount}, SuccessRate={SuccessRate}%, " +
                "AvgProcessingTimeMs={AvgProcessingTime}",
                _config.Provider,
                _config.QueueName,
                stopwatch.ElapsedMilliseconds,
                _totalProcessedCount,
                _totalProcessedCount > 0 ? (_successCount * 100.0 / _totalProcessedCount) : 0,
                _totalProcessedCount > 0 ? (stopwatch.ElapsedMilliseconds / _totalProcessedCount) : 0
            );
        }
    }

    private async Task<bool> ProcessMessageSafeAsync(IQueueMessage message, IMessageQueue queue, CancellationToken jobCancellation)
    {
        var processingStopwatch = Stopwatch.StartNew();
        using var timeoutCts = new CancellationTokenSource(MessageProcessingTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            timeoutCts.Token,
            jobCancellation);

        var messageId = message.MessageId;
        _logger.LogDebug("Processing message {MessageId}", messageId);

        try
        {
            // First, store the message in MongoDB before acknowledging it from the queue
            // This implements a transactional outbox pattern to prevent message loss
            var task = new TaskEntity
            {
                TaskId = messageId,
                Body = message.Body,
                Status = JobTaskStatus.Processing,
                RetryCount = 0
            };

            // Insert the task first - before we complete the message from the queue
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

                // Mark as completed in MongoDB
                await _mongoDb.TryUpdateTaskStatusAsync(task.TaskId, JobTaskStatus.Completed);
                return true;
            });

            if (success)
            {
                // Only complete the message from the queue after successful processing
                await queue.CompleteMessageAsync(message, linkedCts.Token);
                _logger.LogInformation("Message {MessageId} processed successfully in {ElapsedMs}ms",
                    messageId, processingStopwatch.ElapsedMilliseconds);
                return true;
            }
            else
            {
                // This should not happen due to retry policy, but handle just in case
                _logger.LogError("Failed to process message {MessageId} after retries", messageId);
                await HandleFailedMessage(message, queue, "Processing failed after retries", linkedCts.Token);
                return false;
            }
        }
        catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
        {
            _logger.LogError("Processing of message {MessageId} timed out after {Timeout}s",
                messageId, MessageProcessingTimeout.TotalSeconds);

            await _mongoDb.TryUpdateTaskStatusAsync(messageId, JobTaskStatus.Failed);

            await HandleFailedMessage(message, queue, "Processing timeout", linkedCts.Token);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process message {MessageId}", messageId);

            await _mongoDb.TryUpdateTaskStatusAsync(messageId, JobTaskStatus.Failed);

            await HandleFailedMessage(message, queue, $"Exception: {ex.Message}", linkedCts.Token);
            return false;
        }
        finally
        {
            processingStopwatch.Stop();
            _logger.LogDebug("Message {MessageId} processing took {ElapsedMs}ms",
                messageId, processingStopwatch.ElapsedMilliseconds);
        }
    }

    private async Task HandleFailedMessage(IQueueMessage message, IMessageQueue queue, string reason, CancellationToken cancellationToken)
    {
        try
        {
            if (_config.DeadLetterFailedMessages)
            {
                // Send to dead-letter queue for later investigation
                await queue.DeadLetterMessageAsync(message, reason, cancellationToken);
                _logger.LogWarning("Message {MessageId} sent to dead-letter queue: {Reason}",
                    message.MessageId, reason);
            }
            else
            {
                // Return to the queue for reprocessing
                await queue.AbandonMessageAsync(message, cancellationToken);
                _logger.LogWarning("Message {MessageId} abandoned and returned to queue: {Reason}",
                    message.MessageId, reason);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to handle failed message {MessageId}", message.MessageId);
        }
    }
}