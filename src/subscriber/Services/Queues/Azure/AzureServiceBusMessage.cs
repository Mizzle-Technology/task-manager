using Azure.Messaging.ServiceBus;

namespace subscriber.Services.Queues.Azure;

public class AzureServiceBusMessage(ServiceBusReceivedMessage _message) : IQueueMessage
{
    public string MessageId => _message.MessageId;
    public string Body => _message.Body.ToString();
    public DateTime EnqueuedTime => _message.EnqueuedTime.UtcDateTime;
    public string ReceiptHandle => _message.LockToken;
    public uint DeliveryCount => (uint)_message.DeliveryCount;
    public IDictionary<string, string> Properties =>
        _message.ApplicationProperties?.ToDictionary(
            p => p.Key,
            p => p.Value?.ToString() ?? string.Empty)
        ?? new Dictionary<string, string>();
}