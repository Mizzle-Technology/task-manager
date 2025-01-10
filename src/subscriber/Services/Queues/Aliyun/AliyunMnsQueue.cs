using subscriber.Services.Queues.Exceptions;
using Aliyun.MNS;

namespace subscriber.Services.Queues.Aliyun;

public class AliyunMnsQueue : IMessageQueue
{
    private readonly ILogger<AliyunMnsQueue> _logger;
    private readonly Queue _queue;
    private readonly Queue _deadLetterQueue;

    public AliyunMnsQueue(
        ILogger<AliyunMnsQueue> logger,
        Queue queue,
        Queue deadLetterQueue)
    {
        _logger = logger;
        _queue = queue;
        _deadLetterQueue = deadLetterQueue;
    }

    public async Task<IEnumerable<IQueueMessage>> ReceiveMessagesAsync(
        int maxMessages,
        TimeSpan visibilityTimeout,
        CancellationToken cancellationToken)
    {
        try
        {
            var messages = await Task.Run(
                () => _queue.BatchReceiveMessage(
                    (uint)maxMessages,
                    (uint)visibilityTimeout.TotalSeconds),
                cancellationToken);

            return messages.Messages.Select(m => new AliyunMnsMessage(m));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to receive messages from MNS queue");
            throw new QueueOperationException("Failed to receive messages", ex);
        }
    }

    public async Task CompleteMessageAsync(
        IQueueMessage message,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Run(
                () => _queue.DeleteMessage(message.ReceiptHandle),
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to complete message {MessageId}", message.MessageId);
            throw new QueueOperationException($"Failed to complete message {message.MessageId}", ex);
        }
    }

    public async Task AbandonMessageAsync(
        IQueueMessage message,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Run(
                () => _queue.ChangeMessageVisibility(message.ReceiptHandle, 0),
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to abandon message {MessageId}", message.MessageId);
            throw new QueueOperationException($"Failed to abandon message {message.MessageId}", ex);
        }
    }

    public async Task DeadLetterMessageAsync(
        IQueueMessage message,
        string reason,
        CancellationToken cancellationToken)
    {
        try
        {
            await CompleteMessageAsync(message, cancellationToken);
            await Task.Run(
                () => _deadLetterQueue.SendMessage(message.Body),
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to dead-letter message {MessageId}", message.MessageId);
            throw new QueueOperationException($"Failed to dead-letter message {message.MessageId}", ex);
        }
    }
}