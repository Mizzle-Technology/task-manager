namespace mongodb_service.Configuration;

public class MongoDbSettings
{
    public string ConnectionString { get; set; } = string.Empty;
    public string DatabaseName { get; set; } = string.Empty;
    public string StaleTaskTimeout { get; set; } = "00:05:00";
    public string HeartbeatInterval { get; set; } = "00:00:30";

    public TimeSpan GetStaleTaskTimeout() => TimeSpan.Parse(StaleTaskTimeout);
    public TimeSpan GetHeartbeatInterval() => TimeSpan.Parse(HeartbeatInterval);
}