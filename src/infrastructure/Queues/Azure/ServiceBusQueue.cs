using System;
using Azure.Messaging.ServiceBus;
using infrastructure.Queues.Exceptions;
using Microsoft.Extensions.Logging;

namespace infrastructure.Queues.Azure;

public class ServiceBusQueue(
    ILogger<ServiceBusQueue> _logger,
    ServiceBusReceiver _receiver,
    ServiceBusSender _sender
) : IMessageQueue
{
    private readonly Dictionary<string, ServiceBusReceivedMessage> _receivedMessages = [];

    public async Task<IEnumerable<IQueueMessage>> ReceiveMessagesAsync(
        int maxMessages,
        TimeSpan visibilityTimeout,
        CancellationToken cancellationToken
    )
    {
        try
        {
            var messages = await _receiver.ReceiveMessagesAsync(
                maxMessages,
                visibilityTimeout,
                cancellationToken
            );

            // Store received messages for later operations
            foreach (var message in messages)
            {
                _receivedMessages[message.LockToken] = message;
            }

            return messages.Select(m => new AzureServiceBusMessage(m));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to receive messages from Service Bus queue");
            throw new QueueOperationException("Failed to receive messages", ex);
        }
    }

    public async Task CompleteMessageAsync(
        IQueueMessage message,
        CancellationToken cancellationToken
    )
    {
        try
        {
            if (!_receivedMessages.TryGetValue(message.ReceiptHandle, out var receivedMessage))
            {
                throw new QueueOperationException(
                    $"Message {message.MessageId} not found in received messages"
                );
            }

            await _receiver.CompleteMessageAsync(receivedMessage, cancellationToken);
            _receivedMessages.Remove(message.ReceiptHandle);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to complete message {MessageId}", message.MessageId);
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
        try
        {
            if (!_receivedMessages.TryGetValue(message.ReceiptHandle, out var receivedMessage))
            {
                throw new QueueOperationException(
                    $"Message {message.MessageId} not found in received messages"
                );
            }

            await _receiver.AbandonMessageAsync(
                receivedMessage,
                cancellationToken: cancellationToken
            );
            _receivedMessages.Remove(message.ReceiptHandle);
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
        CancellationToken cancellationToken
    )
    {
        try
        {
            if (!_receivedMessages.TryGetValue(message.ReceiptHandle, out var receivedMessage))
            {
                throw new QueueOperationException(
                    $"Message {message.MessageId} not found in received messages"
                );
            }

            await _receiver.DeadLetterMessageAsync(
                receivedMessage,
                reason,
                cancellationToken: cancellationToken
            );
            _receivedMessages.Remove(message.ReceiptHandle);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to dead-letter message {MessageId}", message.MessageId);
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
        try
        {
            var sbMessage = new ServiceBusMessage(message.Body)
            {
                MessageId = message.MessageId,
                ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(delaySeconds),
            };
            sbMessage.ApplicationProperties["Priority"] = priority;
            await _sender.SendMessageAsync(sbMessage, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send message {MessageId} to queue", message.MessageId);
            throw new QueueOperationException($"Failed to send message {message.MessageId}", ex);
        }
    }
}
