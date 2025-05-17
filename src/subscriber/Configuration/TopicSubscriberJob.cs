namespace subscriber.Configuration;

public record TopicSubscriberJobConfiguration
{
    public string TopicName { get; set; } = string.Empty;
    public string SubscriptionName { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 10;
    public int PollingWaitSeconds { get; set; } = 30;
    public bool DeadLetterFailedMessages { get; set; } = true;
}
