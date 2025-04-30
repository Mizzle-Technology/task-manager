namespace subscriber.Configuration.ServiceBus;

public class ServiceBusConfiguration
{
	public string ConnectionString { get; set; } = string.Empty;
	public string[] QueueNames { get; set; } = [];
	public TimeSpan MaxLockDuration { get; set; } = TimeSpan.FromMinutes(5);
	public int MaxDeliveryCount { get; set; } = 10;
	public bool RequireSession { get; set; } = false;
}