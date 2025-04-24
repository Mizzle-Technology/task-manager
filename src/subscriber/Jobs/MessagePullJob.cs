using Quartz;
using subscriber.Services.Queues;
using subscriber.Services.Queues.Aliyun;
using Microsoft.Extensions.Options;
using mongodb_service.Repositories;
using mongodb_service.Models;
using Polly;
using Polly.Retry;

namespace subscriber.Jobs;

public record Message(
    string Id,
    string Content,
    DateTime ReceivedAt,
    string ReceiptHandle
);

public class MessagePullJob(
    ILogger<MessagePullJob> _logger,
    IQueueClientFactory _queueFactory,
    IMongoDbRepository _mongoDb,
    IOptions<AliyunMnsConfiguration> _config) : IJob
{
    private static readonly TimeSpan MessageProcessingTimeout = TimeSpan.FromMinutes(5);
    private readonly AliyunMnsClient _aliyunClient = (AliyunMnsClient)_queueFactory.GetClient(QueueProvider.AliyunMNS);
    private readonly AsyncRetryPolicy _retryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(3,
            retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
            onRetry: (ex, timeSpan, retryCount, _) =>
            {
                _logger.LogWarning(ex,
                    "Retry {RetryCount} after {Delay}s due to: {Error}",
                    retryCount, timeSpan.TotalSeconds, ex.Message);
            });

    public async Task Execute(IJobExecutionContext context)
    {
        _logger.LogInformation("Starting message pull job at: {time}", DateTimeOffset.Now);

        try
        {
            // Verify queue health before processing
            var health = await _aliyunClient.GetQueueHealthAsync(context.CancellationToken);
            if (!health.IsHealthy)
            {
                _logger.LogError("Queue is not healthy. Status: {Status}", health.Status);
                return;
            }

            var queue = _aliyunClient.GetQueue("your-queue-name");
            if (queue == null)
            {
                _logger.LogError("Failed to get queue. Queue might not be initialized");
                return;
            }

            var batchSize = _config.Value.BatchSize;
            var pollingWaitSeconds = _config.Value.PollingWaitSeconds;

            var messages = await queue.ReceiveMessagesAsync(
                batchSize,
                TimeSpan.FromSeconds(pollingWaitSeconds),
                context.CancellationToken);

            if (!messages.Any())
            {
                _logger.LogInformation("No messages pulled from the queue.");
                return;
            }

            _logger.LogInformation("Pulled {Count} messages from the queue", messages.Count());

            // Process messages in parallel
            var tasks = messages.Select(message => ProcessMessageSafeAsync(message, queue, context.CancellationToken));
            var results = await Task.WhenAll(tasks);

            var successCount = results.Count(r => r);
            var failureCount = results.Length - successCount;

            _logger.LogInformation(
                "Message pull job completed at: {time}. Success: {successCount}, Failures: {failureCount}",
                DateTimeOffset.Now,
                successCount,
                failureCount
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error executing message pull job");
            throw;
        }
    }

    private async Task<bool> ProcessMessageSafeAsync(IQueueMessage message, IMessageQueue queue, CancellationToken jobCancellation)
    {
        using var timeoutCts = new CancellationTokenSource(MessageProcessingTimeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            timeoutCts.Token,
            jobCancellation);

        try
        {
            await _retryPolicy.ExecuteAsync(async () =>
            {
                var task = new TaskEntity
                {
                    TaskId = message.MessageId,
                    Body = message.Body,
                    Status = JobTaskStatus.Created,
                    RetryCount = 0
                };

                await _mongoDb.InsertOrUpdateTaskAsync(task);
                _logger.LogInformation("Task {TaskId} created in MongoDB", task.TaskId);

                await queue.CompleteMessageAsync(message, linkedCts.Token);

                var currentTask = await _mongoDb.GetByTaskIdAsync(task.TaskId);
                if (currentTask != null)
                {
                    await _mongoDb.TryUpdateTaskStatusAsync(task.TaskId, JobTaskStatus.Completed);
                }
            });

            return true;
        }
        catch (OperationCanceledException) when (timeoutCts.Token.IsCancellationRequested)
        {
            _logger.LogError("Processing of message {MessageId} timed out after {Timeout}s",
                message.MessageId, MessageProcessingTimeout.TotalSeconds);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process message {MessageId} after retries", message.MessageId);
            return false;
        }
    }
}