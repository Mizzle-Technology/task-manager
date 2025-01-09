using MongoDB.Driver;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Logging;
using MongoDB.Bson;
using mongodb_service.Configuration;
using mongodb_service.Exceptions;
using mongodb_service.Models;

namespace mongodb_service.Services;

public class MongoDbRepository : IMongoDbRepository
{
    private IMongoCollection<TaskEntity>? _taskCollection;
    private readonly ILogger<MongoDbRepository> _logger;
    private readonly TimeSpan _staleTaskTimeout;
    private readonly IMongoClient _client;
    private readonly string _databaseName;

    public MongoDbRepository(
        IMongoClient client,
        IOptions<MongoDbSettings> settings,
        ILogger<MongoDbRepository> logger)
    {
        _logger = logger;
        _client = client;
        _databaseName = settings.Value.DatabaseName;
        _staleTaskTimeout = settings.Value.StaleTaskTimeout;
    }

    private IMongoCollection<TaskEntity> GetCollection()
    {
        if (_taskCollection == null)
        {
            throw new DatabaseOperationException("MongoDB repository not initialized. Call InitializeAsync first.");
        }
        return _taskCollection;
    }

    public async Task InitializeAsync()
    {
        try
        {
            var database = _client.GetDatabase(_databaseName);
            _taskCollection = database.GetCollection<TaskEntity>("tasks");

            // Create indexes
            var indexKeysDefinition = Builders<TaskEntity>.IndexKeys.Ascending(x => x.TaskId);
            var indexOptions = new CreateIndexOptions { Unique = true };
            var indexModel = new CreateIndexModel<TaskEntity>(indexKeysDefinition, indexOptions);
            await _taskCollection.Indexes.CreateOneAsync(indexModel);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize MongoDB connection");
            throw new DatabaseOperationException("Failed to initialize MongoDB connection", ex);
        }
    }

    public async Task InsertOrUpdateTaskAsync(TaskEntity task)
    {
        try
        {
            var collection = GetCollection();
            var filter = Builders<TaskEntity>.Filter.Eq(x => x.TaskId, task.TaskId);
            var options = new ReplaceOptions { IsUpsert = true };

            if (string.IsNullOrEmpty(task.Id))
            {
                task.Id = ObjectId.GenerateNewId().ToString();
                task.CreatedAt = DateTime.UtcNow;
            }
            task.UpdatedAt = DateTime.UtcNow;

            await collection.ReplaceOneAsync(filter, task, options);
            _logger.LogInformation("Task {TaskId} upserted successfully", task.TaskId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to upsert task: {TaskId}", task.TaskId);
            throw new DatabaseOperationException($"Failed to upsert task {task.TaskId}", ex);
        }
    }

    public async Task UpdateTaskErrorMessageAsync(string taskId, string errorMessage)
    {
        try
        {
            var collection = GetCollection();
            var filter = Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId);
            var update = Builders<TaskEntity>.Update
                .Set(x => x.ErrorMessage, errorMessage)
                .Set(x => x.UpdatedAt, DateTime.UtcNow);
            await collection.UpdateOneAsync(filter, update);
            _logger.LogInformation("Task {TaskId} error message updated", taskId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update task error message: {TaskId}", taskId);
            throw new DatabaseOperationException($"Failed to update task {taskId}", ex);
        }
    }

    public async Task<TaskEntity?> TryAcquireTaskAsync(
        JobTaskStatus currentStatus,
        JobTaskStatus newStatus,
        string workerId,
        DateTime heartbeat)
    {
        var collection = GetCollection();
        var options = new FindOneAndUpdateOptions<TaskEntity>
        {
            ReturnDocument = ReturnDocument.After,
            Sort = Builders<TaskEntity>.Sort.Ascending(x => x.CreatedAt)
        };

        var filter = Builders<TaskEntity>.Filter.And(
            Builders<TaskEntity>.Filter.Eq(x => x.Status, currentStatus),
            Builders<TaskEntity>.Filter.Or(
                Builders<TaskEntity>.Filter.Eq(x => x.WorkerPodId, null),
                Builders<TaskEntity>.Filter.Lt(x => x.LastHeartbeat,
                    DateTime.UtcNow.Subtract(_staleTaskTimeout))
            )
        );

        var update = Builders<TaskEntity>.Update
            .Set(x => x.Status, newStatus)
            .Set(x => x.WorkerPodId, workerId)
            .Set(x => x.LastHeartbeat, heartbeat)
            .Set(x => x.LockedAt, DateTime.UtcNow)
            .Set(x => x.UpdatedAt, DateTime.UtcNow)
            .Inc(x => x.Version, 1);

        return await collection.FindOneAndUpdateAsync(filter, update, options);
    }

    public async Task UpdateTaskHeartbeatAsync(string taskId, string podId, DateTime heartbeat)
    {
        var collection = GetCollection();
        var filter = Builders<TaskEntity>.Filter.And(
            Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId),
            Builders<TaskEntity>.Filter.Eq(x => x.WorkerPodId, podId)
        );

        var update = Builders<TaskEntity>.Update
            .Set(x => x.LastHeartbeat, heartbeat)
            .Set(x => x.UpdatedAt, DateTime.UtcNow);

        await collection.UpdateOneAsync(filter, update);
    }

    public async Task<IEnumerable<TaskEntity>> GetStalledTasksAsync(TimeSpan threshold, string currentWorkerId)
    {
        var collection = GetCollection();
        var staleCutoff = DateTime.UtcNow.Subtract(threshold);

        var filter = Builders<TaskEntity>.Filter.And(
            Builders<TaskEntity>.Filter.Eq(x => x.Status, JobTaskStatus.Running),
            Builders<TaskEntity>.Filter.Or(
                Builders<TaskEntity>.Filter.And(
                    Builders<TaskEntity>.Filter.Eq(x => x.WorkerPodId, currentWorkerId),
                    Builders<TaskEntity>.Filter.Lt(x => x.LastHeartbeat, staleCutoff)
                ),
                Builders<TaskEntity>.Filter.And(
                    Builders<TaskEntity>.Filter.Ne(x => x.WorkerPodId, currentWorkerId),
                    Builders<TaskEntity>.Filter.Lt(x => x.LastHeartbeat, staleCutoff.Subtract(threshold))
                )
            )
        );

        return await collection.Find(filter)
            .Sort(Builders<TaskEntity>.Sort.Ascending(x => x.LastHeartbeat))
            .ToListAsync();
    }

    public async Task<bool> RequeueTaskAsync(string taskId, JobTaskStatus status, string reason)
    {
        try
        {
            var collection = GetCollection();
            var filter = Builders<TaskEntity>.Filter.And(
                Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId),
                Builders<TaskEntity>.Filter.Eq(x => x.Status, JobTaskStatus.Running)
            );

            var update = Builders<TaskEntity>.Update
                .Set(x => x.Status, status)
                .Set(x => x.WorkerPodId, null)
                .Set(x => x.WorkerNodeId, null)
                .Set(x => x.LastHeartbeat, null)
                .Set(x => x.LockedAt, null)
                .Set(x => x.ErrorMessage, reason)
                .Set(x => x.UpdatedAt, DateTime.UtcNow)
                .Inc(x => x.Version, 1);

            var result = await collection.UpdateOneAsync(filter, update);
            var success = result.ModifiedCount == 1;

            if (success)
            {
                _logger.LogInformation("Task {TaskId} requeued with status {Status}. Reason: {Reason}",
                    taskId, status, reason);
            }
            else
            {
                _logger.LogWarning("Failed to requeue task {TaskId}. Task may have been recovered by another worker",
                    taskId);
            }

            return success;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error requeueing task {TaskId}", taskId);
            throw new DatabaseOperationException($"Failed to requeue task {taskId}", ex);
        }
    }

    public async Task PingAsync()
    {
        try
        {
            var collection = GetCollection();
            await collection.Database.RunCommandAsync((Command<BsonDocument>)"{ping:1}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to ping MongoDB");
            throw new DatabaseOperationException("Failed to ping MongoDB", ex);
        }
    }

    public async Task<bool> UpdateTaskStatusIfVersionMatchesAsync(
        string taskId,
        long expectedVersion,
        JobTaskStatus newStatus)
    {
        try
        {
            var collection = GetCollection();
            var filter = Builders<TaskEntity>.Filter.And(
                Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId),
                Builders<TaskEntity>.Filter.Eq(x => x.Version, expectedVersion)
            );

            var update = Builders<TaskEntity>.Update
                .Set(x => x.Status, newStatus)
                .Inc(x => x.Version, 1)
                .Set(x => x.UpdatedAt, DateTime.UtcNow);

            var result = await collection.UpdateOneAsync(filter, update);
            return result.ModifiedCount == 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update task status with version check: {TaskId}", taskId);
            throw new DatabaseOperationException($"Failed to update task {taskId}", ex);
        }
    }

    public async Task<bool> UpdateTaskHeartbeatIfVersionMatchesAsync(
        string taskId,
        long expectedVersion,
        string workerId,
        DateTime heartbeat)
    {
        try
        {
            var collection = GetCollection();
            var filter = Builders<TaskEntity>.Filter.And(
                Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId),
                Builders<TaskEntity>.Filter.Eq(x => x.Version, expectedVersion),
                Builders<TaskEntity>.Filter.Eq(x => x.WorkerPodId, workerId)
            );

            var update = Builders<TaskEntity>.Update
                .Set(x => x.LastHeartbeat, heartbeat)
                .Inc(x => x.Version, 1)
                .Set(x => x.UpdatedAt, DateTime.UtcNow);

            var result = await collection.UpdateOneAsync(filter, update);
            return result.ModifiedCount == 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update task heartbeat with version check: {TaskId}", taskId);
            throw new DatabaseOperationException($"Failed to update task {taskId}", ex);
        }
    }

    public async Task<bool> UpdateTaskErrorMessageIfVersionMatchesAsync(
        string taskId,
        long expectedVersion,
        string errorMessage)
    {
        try
        {
            var collection = GetCollection();
            var filter = Builders<TaskEntity>.Filter.And(
                Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId),
                Builders<TaskEntity>.Filter.Eq(x => x.Version, expectedVersion)
            );

            var update = Builders<TaskEntity>.Update
                .Set(x => x.ErrorMessage, errorMessage)
                .Inc(x => x.Version, 1)
                .Set(x => x.UpdatedAt, DateTime.UtcNow);

            var result = await collection.UpdateOneAsync(filter, update);
            return result.ModifiedCount == 1;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update task error message with version check: {TaskId}", taskId);
            throw new DatabaseOperationException($"Failed to update task {taskId}", ex);
        }
    }

    public async Task<TaskEntity?> GetByTaskIdAsync(string taskId)
    {
        try
        {
            var collection = GetCollection();
            var filter = Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId);
            return await collection.Find(filter).FirstOrDefaultAsync();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get task by ID: {TaskId}", taskId);
            throw new DatabaseOperationException($"Failed to get task {taskId}", ex);
        }
    }

    public async Task<bool> TryUpdateTaskStatusAsync(string taskId, JobTaskStatus newStatus)
    {
        try
        {
            var collection = GetCollection();
            var filter = Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId);
            var currentTask = await collection.Find(filter).FirstOrDefaultAsync();
            if (currentTask == null) return false;

            var success = await UpdateTaskStatusIfVersionMatchesAsync(
            taskId,
            currentTask.Version,
            newStatus);

            if (success)
            {
                _logger.LogInformation("Task {TaskId} status updated to {Status}", taskId, newStatus);
            }
            else
            {
                _logger.LogWarning("Failed to update task {TaskId} to {Status} state due to version mismatch",
                    taskId, newStatus);
            }

            return success;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update task {TaskId} status to {Status}", taskId, newStatus);
            throw;
        }
    }

    public async Task<bool> UpdateTaskStatusAndErrorIfVersionMatchesAsync(
        string taskId,
        long expectedVersion,
        JobTaskStatus newStatus,
        string? errorMessage)
    {
        try
        {
            var filter = Builders<TaskEntity>.Filter.And(
                Builders<TaskEntity>.Filter.Eq(x => x.TaskId, taskId),
                Builders<TaskEntity>.Filter.Eq(x => x.Version, expectedVersion)
            );

            var update = Builders<TaskEntity>.Update
                .Set(x => x.Status, newStatus)
                .Set(x => x.ErrorMessage, errorMessage)
                .Inc(x => x.Version, 1)
                .Set(x => x.UpdatedAt, DateTime.UtcNow);

            // Set the appropriate timestamp based on status
            switch (newStatus)
            {
                case JobTaskStatus.Processing:
                    update = update.Set(x => x.ProcessedAt, DateTime.UtcNow);
                    break;
                case JobTaskStatus.Completed:
                    update = update.Set(x => x.CompletedAt, DateTime.UtcNow);
                    break;
                case JobTaskStatus.Failed:
                    update = update.Set(x => x.FailedAt, DateTime.UtcNow);
                    break;
            }

            var collection = GetCollection();
            var result = await collection.UpdateOneAsync(filter, update);
            var success = result.ModifiedCount == 1;

            if (success)
            {
                _logger.LogInformation("Task {TaskId} status updated to {Status} with error message",
                    taskId, newStatus);
            }
            else
            {
                _logger.LogWarning("Concurrency conflict detected for task {TaskId}. Expected version: {Version}",
                    taskId, expectedVersion);
            }

            return success;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to update task status and error atomically: {TaskId}", taskId);
            throw new DatabaseOperationException($"Failed to update task {taskId}", ex);
        }
    }
}