using mongodb_service.Models;

namespace workers.Services;

public interface ITaskProcessor
{
    Task ProcessAsync(TaskEntity task, CancellationToken cancellationToken);
}