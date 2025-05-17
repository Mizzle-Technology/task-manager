using Aliyun.MNS;
using Aliyun.MNS.Model;
using infrastructure.Queues.Exceptions;
using Microsoft.Extensions.Logging;

namespace infrastructure.Queues.Aliyun;

public class AliyunMnsQueue(ILogger<AliyunMnsQueue> logger, Queue queue, Queue deadLetterQueue)
    : IMessageQueue
{
    private readonly ILogger<AliyunMnsQueue> _logger = logger;
    private readonly Queue _queue = queue;
    private readonly Queue _deadLetterQueue = deadLetterQueue;

    public async Task<IEnumerable<IQueueMessage>> ReceiveMessagesAsync(
        int maxMessages,
        TimeSpan visibilityTimeout,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            // Offload to a dedicated thread to avoid thread-pool starvation
            var batch = await Task
                .Factory.StartNew(
                    () =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        var m = _queue.BatchReceiveMessage(
                            (uint)maxMessages,
                            (uint)visibilityTimeout.TotalSeconds
                        );
                        cancellationToken.ThrowIfCancellationRequested();
                        return m;
                    },
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                )
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();

            // Guard against null or empty batch
            var msgs = batch?.Messages ?? new List<Message>();
            return msgs.Select(m => new AliyunMnsMessage(m));
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "MNS Receive failed on queue '{QueueName}' after requesting {MaxMessages} messages with visibility {VisibilityTimeout}s",
                _queue.QueueName,
                maxMessages,
                visibilityTimeout.TotalSeconds
            );
            throw new QueueOperationException("Failed to receive messages", ex);
        }
    }

    public async Task CompleteMessageAsync(
        IQueueMessage message,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            await Task
                .Factory.StartNew(
                    () =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        _queue.DeleteMessage(message.ReceiptHandle);
                    },
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                )
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to complete message {MessageId} on queue '{QueueName}'",
                message.MessageId,
                _queue.QueueName
            );
            throw new QueueOperationException(
                $"Failed to complete message {message.MessageId}",
                ex
            );
        }
    }

    public async Task AbandonMessageAsync(
        IQueueMessage message,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            await Task
                .Factory.StartNew(
                    () =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        _queue.ChangeMessageVisibility(message.ReceiptHandle, 0);
                    },
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                )
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to abandon message {MessageId} on queue '{QueueName}'",
                message.MessageId,
                _queue.QueueName
            );
            throw new QueueOperationException($"Failed to abandon message {message.MessageId}", ex);
        }
    }

    public async Task DeadLetterMessageAsync(
        IQueueMessage message,
        string reason,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            await CompleteMessageAsync(message, cancellationToken);
            await Task
                .Factory.StartNew(
                    () =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        _deadLetterQueue.SendMessage(message.Body);
                    },
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                )
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to dead-letter message {MessageId} on queue '{QueueName}'",
                message.MessageId,
                _queue.QueueName
            );
            throw new QueueOperationException(
                $"Failed to dead-letter message {message.MessageId}",
                ex
            );
        }
    }

    public async Task SendMessageAsync(
        IQueueMessage message,
        uint delaySeconds,
        uint priority,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        try
        {
            await Task
                .Factory.StartNew(
                    () =>
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        _queue.SendMessage(message.Body, delaySeconds, priority);
                    },
                    cancellationToken,
                    TaskCreationOptions.LongRunning,
                    TaskScheduler.Default
                )
                .ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to send message {MessageId} on queue '{QueueName}'",
                message.MessageId,
                _queue.QueueName
            );
            throw new QueueOperationException($"Failed to send message {message.MessageId}", ex);
        }
    }
}
