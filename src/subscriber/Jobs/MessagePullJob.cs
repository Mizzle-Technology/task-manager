using Quartz;
using subscriber.Services.Queues;
using mongodb_service.Models;
using mongodb_service.Services;
using Polly;
using Polly.Retry;

namespace subscriber.Jobs;

public record Message(
    string Id,
    string Content,
    DateTime ReceivedAt,
    string ReceiptHandle
);

public class MessagePullJob : IJob
{
    private const int BatchSize = 10;
    private static readonly TimeSpan MessageProcessingTimeout = TimeSpan.FromMinutes(5);

    private readonly ILogger<MessagePullJob> _logger;
    private readonly IQueueClientFactory _queueFactory;
    private readonly IMongoDbRepository _mongoDb;
    private readonly QueueProvider _provider;
    private readonly AsyncRetryPolicy _retryPolicy;

    public MessagePullJob(
        ILogger<MessagePullJob> logger,
        IQueueClientFactory queueFactory,
        IMongoDbRepository mongoDb,
        IConfiguration config)
    {
        _logger = logger;
        _queueFactory = queueFactory;
        _mongoDb = mongoDb;
        _provider = config.GetValue<QueueProvider>("Queue:Provider");
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
        _logger.LogInformation("Starting message pull job at: {time}", DateTimeOffset.Now);

        try
        {
            var client = _queueFactory.GetClient(_provider);
            var queue = client.GetQueue("your-queue-name");

            var messages = await queue.ReceiveMessagesAsync(
                BatchSize,
                TimeSpan.FromSeconds(30),
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
            throw; // Let Quartz handle retries
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