using Aliyun.MNS.Model;

namespace subscriber.Services.Queues.Aliyun;

public class AliyunMnsMessage(Message _message) : IQueueMessage
{
    public string MessageId => _message.Id;
    public string Body => _message.Body;
    public DateTime EnqueuedTime => _message.EnqueueTime;
    public string ReceiptHandle => _message.ReceiptHandle;
    public uint DeliveryCount => _message.DequeueCount;
    public IDictionary<string, string> Properties => new Dictionary<string, string>
    {
        { "Priority", _message.Priority.ToString() },
        { "NextVisibleTime", _message.NextVisibleTime.ToString() },
        { "FirstDequeueTime", _message.FirstDequeueTime.ToString() },
        { "BodyMD5", _message.BodyMD5 }
    };
}