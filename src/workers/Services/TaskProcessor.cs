using mongodb_service.Models;
using mongodb_service.Repositories;

namespace workers.Services;

public class TaskProcessor(
    ILogger<TaskProcessor> logger,
    IMongoDbRepository mongoDb) : ITaskProcessor
{
    public async Task ProcessAsync(TaskEntity task, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(task);

        try
        {
            if (!await mongoDb.TryUpdateTaskStatusAsync(task.TaskId, JobTaskStatus.Running))
            {
                return;
            }

            logger.LogInformation("Started processing task {TaskId}", task.TaskId);
            await ProcessTaskLogicAsync(task, cancellationToken);
            await mongoDb.TryUpdateTaskStatusAsync(task.TaskId, JobTaskStatus.Succeeded);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error processing task {TaskId}", task.TaskId);
            throw;
        }
    }

    private async Task ProcessTaskLogicAsync(TaskEntity task, CancellationToken cancellationToken)
    {
        // Simulate processing time
        await Task.Delay(Random.Shared.Next(1000, 5000), cancellationToken);
    }
}