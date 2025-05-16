using Azure.Messaging.ServiceBus;

namespace infrastructure.Queues.Azure;

public class AzureServiceBusMessage(ServiceBusReceivedMessage _message) : IQueueMessage
{
    public string MessageId => _message.MessageId;
    public string Body => _message.Body.ToString();
    public ReadOnlyMemory<byte> BodyBytes => _message.Body.ToArray();
    public DateTime EnqueuedTime => _message.EnqueuedTime.UtcDateTime;
    public string ReceiptHandle => _message.LockToken;
    public uint DeliveryCount => (uint)_message.DeliveryCount;
    public IDictionary<string, string> Properties =>
        _message.ApplicationProperties?.ToDictionary(
            p => p.Key,
            p => p.Value?.ToString() ?? string.Empty
        ) ?? [];
    public string SubscriptionName => string.Empty;
}
