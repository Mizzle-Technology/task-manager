using MongoDB.Bson.Serialization.Attributes;

namespace mongodb_service.Models;

public class TaskEntity : BaseEntity
{
    [BsonElement("taskId")]
    public required string TaskId { get; set; }

    [BsonElement("body")]
    public required string Body { get; set; }

    [BsonElement("status")]
    [BsonRepresentation(MongoDB.Bson.BsonType.String)]
    public JobTaskStatus Status { get; set; } = JobTaskStatus.Pending;

    [BsonElement("retryCount")]
    public uint RetryCount { get; set; }

    [BsonElement("processedAt")]
    public DateTime? ProcessedAt { get; set; }

    [BsonElement("completedAt")]
    public DateTime? CompletedAt { get; set; }

    [BsonElement("failedAt")]
    public DateTime? FailedAt { get; set; }

    [BsonElement("errorMessage")]
    public string? ErrorMessage { get; set; }

    [BsonElement("version")]
    public long Version { get; set; } = 1;

    [BsonElement("workerPodId")]
    public string? WorkerPodId { get; set; }

    [BsonElement("workerNodeId")]
    public string? WorkerNodeId { get; set; }

    [BsonElement("lastHeartbeat")]
    public DateTime? LastHeartbeat { get; set; }

    [BsonElement("lockedAt")]
    public DateTime? LockedAt { get; set; }

    [BsonElement("metadata")]
    public Dictionary<string, string> Metadata { get; set; } = new Dictionary<string, string>();
}