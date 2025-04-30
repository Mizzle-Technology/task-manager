namespace subscriber.Services.Queues;

/// <summary>
/// General interface for a queue/subscription message
/// </summary>
public interface IQueueMessage
{
    string MessageId { get; }
    string Body { get; } // Keep for convenience, assumes UTF8
    ReadOnlyMemory<byte> BodyBytes { get; } // Add raw bytes
    DateTime EnqueuedTime { get; }
    string ReceiptHandle { get; } // Lock Token
    uint DeliveryCount { get; }
    IDictionary<string, string> Properties { get; }
    string SubscriptionName { get; } // Add subscription context
}