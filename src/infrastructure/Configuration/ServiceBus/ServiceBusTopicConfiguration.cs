// src/subscriber/Configuration/ServiceBus/ServiceBusTopicConfiguration.cs
namespace infrastructure.Configuration.ServiceBus;

public class ServiceBusTopicConfiguration
{
    public string TopicName { get; set; } = string.Empty;
    public string[] SubscriptionNames { get; set; } = [];
    public TimeSpan MessageTimeToLive { get; set; } = TimeSpan.FromDays(14);
    public TimeSpan SubscriptionMessageTimeToLive { get; set; } = TimeSpan.FromDays(14);
    public TimeSpan LockDuration { get; set; } = TimeSpan.FromMinutes(5);
}
