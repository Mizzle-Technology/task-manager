namespace subscriber.Services.Queues;

public interface IQueueMessage
{
    string MessageId { get; }
    string Body { get; }
    DateTime EnqueuedTime { get; }
    string ReceiptHandle { get; }
    uint DeliveryCount { get; }
    IDictionary<string, string> Properties { get; }
}