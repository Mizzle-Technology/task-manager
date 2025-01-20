namespace subscriber.Services.Queues.Aliyun;

public class AliyunMnsConfiguration
{
    public string AccessKeyId { get; set; } = string.Empty;
    public string AccessKeySecret { get; set; } = string.Empty;
    public string Endpoint { get; set; } = string.Empty;
    public string QueueName { get; set; } = string.Empty;
    public string DeadLetterQueueName { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 10;
    public int PollingWaitSeconds { get; set; } = 30;
}