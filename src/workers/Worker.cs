using workers.Services;
using mongodb_service.Configuration;
using Microsoft.Extensions.Options;

namespace workers;

public class Worker(
    ILogger<Worker> logger,
    ITaskManager taskManager,
    ITaskProcessor taskProcessor,
    IOptions<MongoDbSettings> settings) : BackgroundService
{
    private const int BatchSize = 10;
    private readonly TimeSpan _pollingInterval = TimeSpan.FromSeconds(10);
    private readonly TimeSpan _heartbeatInterval = settings.Value.HeartbeatInterval;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await taskManager.RecoverStalledTasksAsync();
                var tasks = await taskManager.AcquireTasksAsync(BatchSize);

                foreach (var task in tasks)
                {
                    try
                    {
                        using var timer = new PeriodicTimer(_heartbeatInterval);
                        var heartbeat = StartHeartbeat(task.TaskId, timer, stoppingToken);

                        try
                        {
                            using var linkedCts = CancellationTokenSource
                                .CreateLinkedTokenSource(stoppingToken);

                            // Set timeout for task processing
                            linkedCts.CancelAfter(settings.Value.StaleTaskTimeout);

                            await taskProcessor.ProcessAsync(task, linkedCts.Token);
                        }
                        finally
                        {
                            await heartbeat;
                        }
                    }
                    catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                    {
                        logger.LogWarning("Task {TaskId} cancelled due to shutdown", task.TaskId);
                        throw;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Error processing task {TaskId}", task.TaskId);
                        await taskManager.HandleTaskErrorAsync(task, ex);
                    }
                }

                await Task.Delay(_pollingInterval, stoppingToken);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Error in worker execution");
                await Task.Delay(_pollingInterval, stoppingToken);
            }
        }
    }

    private Task StartHeartbeat(string taskId, PeriodicTimer timer, CancellationToken cancellationToken)
    {
        return Task.Run(async () =>
        {
            while (await timer.WaitForNextTickAsync(cancellationToken))
            {
                await taskManager.UpdateHeartbeatAsync(taskId);
            }
        }, cancellationToken);
    }
}
