namespace subscriber.Configuration;

public record QueueListenerConfiguration
{
    /// <summary>
    /// Which queue provider to use: AliyunMNS or AzureServiceBus.
    /// Defaults to AliyunMNS if not specified.
    /// </summary>
    public string Provider { get; set; } = "AliyunMNS";
    public string QueueName { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 10;
    public int PollingWaitSeconds { get; set; } = 30;
    public bool DeadLetterFailedMessages { get; set; } = true;
}
