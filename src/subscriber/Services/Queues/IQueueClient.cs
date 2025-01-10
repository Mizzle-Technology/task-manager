namespace subscriber.Services.Queues;

public interface IQueueClient : IAsyncDisposable
{
    Task InitializeAsync(CancellationToken cancellationToken);
    IMessageQueue GetQueue(string queueName);
    Task<QueueHealth> GetQueueHealthAsync(CancellationToken cancellationToken);
}

public record QueueHealth(
    bool IsHealthy,
    string Status,
    int ActiveMessageCount,
    int DeadLetterMessageCount);