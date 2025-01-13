using mongodb_service.Models;

namespace workers.Services;

public interface ITaskManager
{
    Task<IEnumerable<TaskEntity>> AcquireTasksAsync(int batchSize);
    Task HandleTaskErrorAsync(TaskEntity task, Exception ex);
    Task RecoverStalledTasksAsync();
    Task UpdateHeartbeatAsync(string taskId);
}