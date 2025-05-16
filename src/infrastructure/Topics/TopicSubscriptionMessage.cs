using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Azure.Messaging.ServiceBus;
using infrastructure.Queues;

namespace infrastructure.Topics;

/// <summary>
/// Represents a message received from a Service Bus topic subscription
/// Implements IQueueMessage and holds the original message for settlement.
/// </summary>
public class TopicSubscriptionMessage(ServiceBusReceivedMessage message, string subscriptionName)
    : IQueueMessage
{
    // Keep the original message private but accessible internally if needed,
    // or provide a specific method/property for the client implementation
    // For simplicity here, we make it accessible via a property for the client.
    internal ServiceBusReceivedMessage OriginalMessage { get; } =
        message ?? throw new ArgumentNullException(nameof(message));

    public string MessageId => OriginalMessage.MessageId;

    // Provide Body as UTF8 string for convenience
    public string Body => Encoding.UTF8.GetString(OriginalMessage.Body.ToArray());

    // Expose raw bytes
    public ReadOnlyMemory<byte> BodyBytes => OriginalMessage.Body;

    public DateTime EnqueuedTime => OriginalMessage.EnqueuedTime.UtcDateTime;
    public string ReceiptHandle => OriginalMessage.LockToken;
    public uint DeliveryCount => (uint)OriginalMessage.DeliveryCount;
    public IDictionary<string, string> Properties =>
        OriginalMessage.ApplicationProperties?.ToDictionary(
            p => p.Key,
            p => p.Value?.ToString() ?? string.Empty
        ) ?? new Dictionary<string, string>();

    // Store the subscription name the message came from
    public string SubscriptionName { get; } =
        subscriptionName ?? throw new ArgumentNullException(nameof(subscriptionName));
}
