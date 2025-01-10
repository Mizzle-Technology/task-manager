namespace subscriber.Services.Queues;

public interface IMessageQueue
{
    Task<IEnumerable<IQueueMessage>> ReceiveMessagesAsync(
        int maxMessages,
        TimeSpan visibilityTimeout,
        CancellationToken cancellationToken);

    Task CompleteMessageAsync(
        IQueueMessage message,
        CancellationToken cancellationToken);

    Task AbandonMessageAsync(
        IQueueMessage message,
        CancellationToken cancellationToken);

    Task DeadLetterMessageAsync(
        IQueueMessage message,
        string reason,
        CancellationToken cancellationToken);
}