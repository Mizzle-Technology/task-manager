namespace subscriber.Services.Queues;

public interface IQueueClient : IAsyncDisposable
{
    // Required synchronous initialization
    void Initialize(CancellationToken cancellationToken);

    IMessageQueue GetQueue(string queueName);
    Task<QueueHealth> GetQueueHealthAsync(CancellationToken cancellationToken);
}

public record QueueHealth(
    bool IsHealthy,
    string Status,
    long ActiveMessageCount,
    long DeadLetterMessageCount);