using mongodb_service.Models;
using mongodb_service.Services;

namespace workers.Services;

public class TaskManager(ILogger<TaskManager> logger, IMongoDbRepository mongoDb) : ITaskManager
{
    private readonly string _workerId = InitializeWorkerId();

    private static string InitializeWorkerId() =>
        $"{Environment.GetEnvironmentVariable("NODE_NAME") ?? "unknown-node"}-" +
        $"{Environment.GetEnvironmentVariable("POD_NAME") ?? "unknown-pod"}-" +
        $"{Environment.GetEnvironmentVariable("INSTANCE_ID") ?? Guid.NewGuid().ToString("N")}";

    public async Task<IEnumerable<TaskEntity>> AcquireTasksAsync(int batchSize)
    {
        var tasks = new List<TaskEntity>();
        try
        {
            for (int i = 0; i < batchSize; i++)
            {
                // First try to acquire Completed tasks and queue them
                var task = await mongoDb.TryAcquireTaskAsync(
                    JobTaskStatus.Completed,
                    JobTaskStatus.Queued,
                    _workerId,
                    DateTime.UtcNow);

                // Then try to acquire Queued tasks and assign them
                task ??= await mongoDb.TryAcquireTaskAsync(
                    JobTaskStatus.Queued,
                    JobTaskStatus.Assigned,
                    _workerId,
                    DateTime.UtcNow);

                if (task == null) break;
                tasks.Add(task);
            }

            if (tasks.Count > 0)
            {
                logger.LogInformation("Worker {WorkerId} acquired {Count} tasks", _workerId, tasks.Count);
            }
            return tasks;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error getting available tasks for worker {WorkerId}", _workerId);
            return [];
        }
    }

    public async Task HandleTaskErrorAsync(TaskEntity task, Exception ex)
    {
        try
        {
            var currentTask = await mongoDb.GetByTaskIdAsync(task.TaskId);
            if (currentTask == null) return;

            // First update to Error state
            var success = await mongoDb.UpdateTaskStatusAndErrorIfVersionMatchesAsync(
                task.TaskId,
                currentTask.Version,
                JobTaskStatus.Error,
                ex.Message);

            if (success)
            {
                // Get fresh task state after error update
                currentTask = await mongoDb.GetByTaskIdAsync(task.TaskId);
                if (currentTask == null) return;

                if (currentTask.RetryCount < 3)
                {
                    // Update retry count and queue for retry
                    var retrySuccess = await mongoDb.UpdateTaskStatusAndErrorIfVersionMatchesAsync(
                        task.TaskId,
                        currentTask.Version,
                        JobTaskStatus.Queued,
                        $"Retry attempt {currentTask.RetryCount + 1}/3");

                    if (retrySuccess)
                    {
                        logger.LogInformation("Task {TaskId} queued for retry. Attempt {RetryCount}",
                            task.TaskId, currentTask.RetryCount + 1);
                    }
                }
                else
                {
                    // Mark as permanently failed
                    var failureSuccess = await mongoDb.UpdateTaskStatusAndErrorIfVersionMatchesAsync(
                        task.TaskId,
                        currentTask.Version,
                        JobTaskStatus.Failed,
                        $"Failed permanently after {currentTask.RetryCount} retries: {ex.Message}");

                    if (failureSuccess)
                    {
                        logger.LogError("Task {TaskId} failed after {RetryCount} retries",
                            task.TaskId, currentTask.RetryCount);
                    }
                }
            }
            else
            {
                logger.LogWarning("Failed to update task {TaskId} status due to version mismatch", task.TaskId);
            }
        }
        catch (Exception updateEx)
        {
            logger.LogError(updateEx, "Error updating failed task {TaskId} status", task.TaskId);
        }
    }

    public async Task RecoverStalledTasksAsync()
    {
        try
        {
            var stalledTasks = await mongoDb.GetStalledTasksAsync(
                TimeSpan.FromMinutes(5),
                _workerId);

            foreach (var task in stalledTasks)
            {
                var reason = task.WorkerPodId == _workerId
                    ? "Task stalled in current worker"
                    : $"Task stalled in worker {task.WorkerPodId}";

                var success = await mongoDb.RequeueTaskAsync(
                    task.TaskId,
                    JobTaskStatus.Queued,
                    reason);

                if (success)
                {
                    logger.LogInformation("Recovered stalled task {TaskId}. {Reason}",
                        task.TaskId, reason);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error recovering stalled tasks");
        }
    }

    public async Task UpdateHeartbeatAsync(string taskId)
    {
        try
        {
            var task = await mongoDb.GetByTaskIdAsync(taskId);
            if (task == null) return;

            var success = await mongoDb.UpdateTaskHeartbeatIfVersionMatchesAsync(
                taskId,
                task.Version,
                _workerId,
                DateTime.UtcNow);

            if (!success)
            {
                logger.LogWarning("Failed to update heartbeat due to version mismatch for task {TaskId}", taskId);
                // Could implement retry logic or task recovery here
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error updating heartbeat for task {TaskId} by worker {WorkerId}",
                taskId, _workerId);
        }
    }
}