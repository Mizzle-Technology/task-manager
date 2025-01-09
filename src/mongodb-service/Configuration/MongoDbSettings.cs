namespace mongodb_service.Configuration;

public class MongoDbSettings
{
    public required string ConnectionString { get; set; }
    public required string DatabaseName { get; set; }
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan StaleTaskTimeout { get; set; } = TimeSpan.FromMinutes(5);
}