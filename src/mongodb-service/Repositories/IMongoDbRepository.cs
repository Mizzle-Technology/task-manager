using MongoDB.Driver;
using mongodb_service.Models;

namespace mongodb_service.Repositories;

public interface IMongoDbRepository
{
	// Keep core operations
	Task InsertOrUpdateTaskAsync(TaskEntity task);
	Task<TaskEntity?> GetByTaskIdAsync(string taskId);
	Task<TaskEntity?> TryAcquireTaskAsync(JobTaskStatus currentStatus, JobTaskStatus newStatus, string workerId, DateTime heartbeat);
	Task<IEnumerable<TaskEntity>> GetStalledTasksAsync(TimeSpan threshold, string podId);
	Task<bool> RequeueTaskAsync(string taskId, JobTaskStatus status, string reason);
	Task PingAsync();

	// Keep versioned operations
	Task<bool> UpdateTaskStatusIfVersionMatchesAsync(string taskId, long expectedVersion, JobTaskStatus newStatus);
	Task<bool> UpdateTaskHeartbeatIfVersionMatchesAsync(string taskId, long expectedVersion, string workerId, DateTime heartbeat);
	Task<bool> TryUpdateTaskStatusAsync(string taskId, JobTaskStatus newStatus);
	Task<bool> UpdateTaskStatusAndErrorIfVersionMatchesAsync(string taskId, long expectedVersion, JobTaskStatus newStatus, string? errorMessage);
	Task InitializeAsync();
}