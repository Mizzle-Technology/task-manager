using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using MongoDB.Driver.Core.Configuration;
using mongodb_service.Configuration;
using mongodb_service.Models;
using mongodb_service.Services;
using mongodb_service.tests.Infrastructure;
using Moq;
using Xunit;
using MongoDB.Bson;
using mongodb_service.Exceptions;

namespace mongodb_service.tests.Services;

public class MongoDbRepositoryTests : IAsyncLifetime
{
    private readonly MongoDbTestContainer _container;
    private IMongoClient? _client;
    private MongoDbRepository? _repository;
    private readonly ILogger<MongoDbRepository> _logger;

    public MongoDbRepositoryTests()
    {
        _container = new MongoDbTestContainer();
        _logger = Mock.Of<ILogger<MongoDbRepository>>();
    }

    public async Task InitializeAsync()
    {
        await _container.InitializeAsync();
        var settings = _container.GetMongoDbSettings();
        _client = new MongoClient(settings.ConnectionString);

        _repository = new MongoDbRepository(
            _client,
            Options.Create(settings),
            _logger);

        await _repository.InitializeAsync();
    }

    public async Task DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    [Fact]
    public async Task InsertOrUpdateTaskAsync_ShouldInsertNewTask()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "test-task-1",
            Status = JobTaskStatus.Pending,
            Body = "Test task body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString()
        };

        // Act
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Assert
        var savedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(savedTask);
        Assert.Equal(task.TaskId, savedTask.TaskId);
        Assert.Equal(JobTaskStatus.Pending, savedTask.Status);
    }

    [Fact]
    public async Task TryAcquireTaskAsync_ShouldAcquireAvailableTask()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "test-task-2",
            Status = JobTaskStatus.Pending,
            Body = "Test task body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString()
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var acquired = await _repository.TryAcquireTaskAsync(
            JobTaskStatus.Pending,
            JobTaskStatus.Running,
            "worker-1",
            DateTime.UtcNow);

        // Assert
        Assert.NotNull(acquired);
        Assert.Equal(task.TaskId, acquired.TaskId);
        Assert.Equal(JobTaskStatus.Running, acquired.Status);
        Assert.Equal("worker-1", acquired.WorkerPodId);
    }

    [Fact]
    public async Task InsertOrUpdateTaskAsync_WhenUpdatingExistingTask_ShouldUpdateTask()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "test-task-1",
            Status = JobTaskStatus.Pending,
            Body = "Original body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString()
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        task.Body = "Updated body";
        task.Status = JobTaskStatus.Running;
        await _repository.InsertOrUpdateTaskAsync(task);

        // Assert
        var updatedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(updatedTask);
        Assert.Equal("Updated body", updatedTask.Body);
        Assert.Equal(JobTaskStatus.Running, updatedTask.Status);
    }

    [Fact]
    public async Task GetByTaskIdAsync_WhenTaskDoesNotExist_ShouldReturnNull()
    {
        // Act
        var result = await _repository!.GetByTaskIdAsync("non-existent-task");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public async Task TryAcquireTaskAsync_WhenTaskIsStale_ShouldAcquireTask()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "stale-task",
            Status = JobTaskStatus.Running,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            WorkerPodId = "old-worker",
            LastHeartbeat = DateTime.UtcNow.AddHours(-1) // Stale heartbeat
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var acquired = await _repository.TryAcquireTaskAsync(
            JobTaskStatus.Running,
            JobTaskStatus.Running,
            "new-worker",
            DateTime.UtcNow);

        // Assert
        Assert.NotNull(acquired);
        Assert.Equal("new-worker", acquired.WorkerPodId);
    }

    [Fact]
    public async Task TryAcquireTaskAsync_WhenNoTasksAvailable_ShouldReturnNull()
    {
        // Act
        var acquired = await _repository!.TryAcquireTaskAsync(
            JobTaskStatus.Pending,
            JobTaskStatus.Running,
            "worker-1",
            DateTime.UtcNow);

        // Assert
        Assert.Null(acquired);
    }

    [Fact]
    public async Task UpdateTaskStatusIfVersionMatchesAsync_WhenVersionMatches_ShouldUpdateStatus()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "version-test",
            Status = JobTaskStatus.Pending,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            Version = 1
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var success = await _repository.UpdateTaskStatusIfVersionMatchesAsync(
            task.TaskId,
            1,
            JobTaskStatus.Running);

        // Assert
        Assert.True(success);
        var updatedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(updatedTask);
        Assert.Equal(JobTaskStatus.Running, updatedTask.Status);
        Assert.Equal(2, updatedTask.Version);
    }

    [Fact]
    public async Task UpdateTaskStatusIfVersionMatchesAsync_WhenVersionMismatch_ShouldNotUpdate()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "version-mismatch",
            Status = JobTaskStatus.Pending,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            Version = 2
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var success = await _repository.UpdateTaskStatusIfVersionMatchesAsync(
            task.TaskId,
            1, // Wrong version
            JobTaskStatus.Running);

        // Assert
        Assert.False(success);
        var unchangedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(unchangedTask);
        Assert.Equal(JobTaskStatus.Pending, unchangedTask.Status);
        Assert.Equal(2, unchangedTask.Version);
    }

    [Fact]
    public async Task RequeueTaskAsync_WhenTaskIsRunning_ShouldRequeueSuccessfully()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "requeue-test",
            Status = JobTaskStatus.Running,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            WorkerPodId = "worker-1"
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var success = await _repository.RequeueTaskAsync(
            task.TaskId,
            JobTaskStatus.Pending,
            "Task timed out");

        // Assert
        Assert.True(success);
        var requeuedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(requeuedTask);
        Assert.Equal(JobTaskStatus.Pending, requeuedTask.Status);
        Assert.Null(requeuedTask.WorkerPodId);
        Assert.Equal("Task timed out", requeuedTask.ErrorMessage);
    }

    [Fact]
    public async Task GetStalledTasksAsync_ShouldReturnOnlyStalledTasks()
    {
        // Arrange
        var stalledTask = new TaskEntity
        {
            TaskId = "stalled-task",
            Status = JobTaskStatus.Running,
            Body = "Stalled task",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            WorkerPodId = "worker-1",
            LastHeartbeat = DateTime.UtcNow.AddMinutes(-10)
        };

        var activeTask = new TaskEntity
        {
            TaskId = "active-task",
            Status = JobTaskStatus.Running,
            Body = "Active task",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            WorkerPodId = "worker-1",
            LastHeartbeat = DateTime.UtcNow
        };

        await _repository!.InsertOrUpdateTaskAsync(stalledTask);
        await _repository.InsertOrUpdateTaskAsync(activeTask);

        // Act
        var stalledTasks = await _repository.GetStalledTasksAsync(
            TimeSpan.FromMinutes(5),
            "worker-1");

        // Assert
        Assert.Single(stalledTasks);
        Assert.Equal("stalled-task", stalledTasks.First().TaskId);
    }

    [Fact]
    public async Task InitializeAsync_WhenDatabaseUnavailable_ShouldThrowException()
    {
        // Arrange
        var invalidSettings = new MongoDbSettings
        {
            ConnectionString = "mongodb://localhost:27018", // Use non-existent port
            DatabaseName = "test-db",
            StaleTaskTimeout = TimeSpan.FromMinutes(5)
        };

        var clientSettings = MongoClientSettings.FromUrl(new MongoUrl(invalidSettings.ConnectionString));
        clientSettings.ServerSelectionTimeout = TimeSpan.FromSeconds(1); // Short timeout
        clientSettings.ConnectTimeout = TimeSpan.FromSeconds(1);

        var repository = new MongoDbRepository(
            new MongoClient(clientSettings),
            Options.Create(invalidSettings),
            _logger);

        // Act & Assert
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => repository.InitializeAsync());

        Assert.True(
            ex is MongoConnectionException ||
            ex is TimeoutException ||
            ex is DatabaseOperationException);
    }

    [Fact]
    public async Task Operations_WhenDatabaseDisconnected_ShouldThrowException()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "test-task",
            Status = JobTaskStatus.Pending,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString()
        };

        // First insert with valid connection
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Stop the container to simulate database disconnection
        await _container.DisposeAsync();

        // Act & Assert
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => _repository.GetByTaskIdAsync(task.TaskId));

        Assert.True(
            ex is MongoConnectionException ||
            ex is TimeoutException ||
            ex is DatabaseOperationException);
    }

    [Fact]
    public async Task TryAcquireTaskAsync_WhenMultipleWorkersCompete_ShouldOnlyAcquireOnce()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "concurrent-task",
            Status = JobTaskStatus.Pending,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString()
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var acquisitionTasks = new List<Task<TaskEntity?>>();
        for (int i = 0; i < 5; i++)
        {
            acquisitionTasks.Add(_repository.TryAcquireTaskAsync(
                JobTaskStatus.Pending,
                JobTaskStatus.Running,
                $"worker-{i}",
                DateTime.UtcNow));
        }

        var results = await Task.WhenAll(acquisitionTasks);

        // Assert
        var acquiredTasks = results.Where(r => r != null).ToList();
        Assert.Single(acquiredTasks);

        var acquiredTask = acquiredTasks[0];
        Assert.NotNull(acquiredTask);
        Assert.Equal(JobTaskStatus.Running, acquiredTask.Status);
    }

    [Theory]
    [InlineData(JobTaskStatus.Pending, JobTaskStatus.Running)]
    [InlineData(JobTaskStatus.Running, JobTaskStatus.Completed)]
    [InlineData(JobTaskStatus.Running, JobTaskStatus.Failed)]
    [InlineData(JobTaskStatus.Failed, JobTaskStatus.Retrying)]
    public async Task UpdateTaskStatus_WithValidTransition_ShouldSucceed(
        JobTaskStatus initialStatus,
        JobTaskStatus newStatus)
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = $"transition-test-{Guid.NewGuid()}",
            Status = initialStatus,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            Version = 1
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var success = await _repository.UpdateTaskStatusIfVersionMatchesAsync(
            task.TaskId,
            1,
            newStatus);

        // Assert
        Assert.True(success);
        var updatedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(updatedTask);
        Assert.Equal(newStatus, updatedTask.Status);
    }

    [Fact]
    public async Task UpdateTaskStatus_WithTimestamps_ShouldSetCorrectTimestamp()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "timestamp-test",
            Status = JobTaskStatus.Pending,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString()
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var success = await _repository.UpdateTaskStatusAndErrorIfVersionMatchesAsync(
            task.TaskId,
            1, // Initial version
            JobTaskStatus.Failed,
            "Test error");

        // Assert
        Assert.True(success);
        var updatedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(updatedTask);
        Assert.NotNull(updatedTask.FailedAt);
        Assert.Equal(JobTaskStatus.Failed, updatedTask.Status);
        Assert.Equal("Test error", updatedTask.ErrorMessage);
    }

    [Theory]
    [InlineData(JobTaskStatus.Pending, JobTaskStatus.Processing, "ProcessedAt")]
    [InlineData(JobTaskStatus.Processing, JobTaskStatus.Completed, "CompletedAt")]
    [InlineData(JobTaskStatus.Processing, JobTaskStatus.Failed, "FailedAt")]
    public async Task UpdateTaskStatus_ShouldSetAppropriateTimestamp(
        JobTaskStatus initialStatus,
        JobTaskStatus newStatus,
        string timestampProperty)
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = $"timestamp-test-{Guid.NewGuid()}",
            Status = initialStatus,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            Version = 1
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var success = await _repository.UpdateTaskStatusAndErrorIfVersionMatchesAsync(
            task.TaskId,
            1,
            newStatus,
            null);

        // Assert
        Assert.True(success);
        var updatedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(updatedTask);
        Assert.Equal(newStatus, updatedTask.Status);

        // Check the appropriate timestamp was set
        var timestamp = typeof(TaskEntity).GetProperty(timestampProperty)?.GetValue(updatedTask) as DateTime?;
        Assert.NotNull(timestamp);
        Assert.True(timestamp > DateTime.UtcNow.AddSeconds(-5));
    }

    [Fact]
    public async Task InitializeAsync_ShouldCreateUniqueTaskIdIndex()
    {
        // Arrange
        var task1 = new TaskEntity
        {
            TaskId = "duplicate-task",
            Status = JobTaskStatus.Pending,
            Body = "Test body 1",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString()
        };

        var task2 = new TaskEntity
        {
            TaskId = "duplicate-task", // Same TaskId
            Status = JobTaskStatus.Pending,
            Body = "Test body 2",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString()
        };

        // Get direct access to collection to bypass the repository's upsert logic
        var database = _client!.GetDatabase(_container.GetMongoDbSettings().DatabaseName);
        var collection = database.GetCollection<TaskEntity>("tasks");

        // Act & Assert
        await collection.InsertOneAsync(task1);

        var ex = await Assert.ThrowsAsync<MongoWriteException>(
            () => collection.InsertOneAsync(task2));

        // Check if it's a duplicate key error (E11000)
        var isDuplicateKeyError =
            ex.WriteError.Code == 11000 || // MongoDB duplicate key error code
            ex.WriteError.Category == ServerErrorCategory.DuplicateKey;

        Assert.True(isDuplicateKeyError, "Expected a duplicate key error");
    }

    [Fact]
    public async Task RequeueTask_ShouldCleanupWorkerMetadata()
    {
        // Arrange
        var task = new TaskEntity
        {
            TaskId = "cleanup-test",
            Status = JobTaskStatus.Running,
            Body = "Test body",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            WorkerPodId = "worker-1",
            WorkerNodeId = "node-1",
            LastHeartbeat = DateTime.UtcNow,
            LockedAt = DateTime.UtcNow
        };
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        await _repository.RequeueTaskAsync(
            task.TaskId,
            JobTaskStatus.Pending,
            "Cleanup test");

        // Assert
        var requeuedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(requeuedTask);
        Assert.Null(requeuedTask.WorkerPodId);
        Assert.Null(requeuedTask.WorkerNodeId);
        Assert.Null(requeuedTask.LastHeartbeat);
        Assert.Null(requeuedTask.LockedAt);
    }

    [Fact]
    public async Task GetStalledTasks_ShouldIncludeTasksFromDeadWorkers()
    {
        // Arrange
        var now = DateTime.UtcNow;
        var stalledTasks = new[]
        {
            CreateStalledTask("stalled-1", "dead-worker-1", now.AddHours(-2)),
            CreateStalledTask("stalled-2", "dead-worker-2", now.AddHours(-3))
        };

        foreach (var task in stalledTasks)
        {
            await _repository!.InsertOrUpdateTaskAsync(task);
        }

        // Act
        var results = await _repository!.GetStalledTasksAsync(
            TimeSpan.FromMinutes(5),
            "current-worker");

        // Assert
        Assert.NotNull(results);
        var resultsList = results.ToList();
        Assert.Equal(2, resultsList.Count);
        Assert.All(resultsList, task =>
        {
            Assert.NotNull(task.LastHeartbeat);
            Assert.True(task.LastHeartbeat < now.AddHours(-1));
        });
    }

    private TaskEntity CreateStalledTask(string taskId, string workerId, DateTime lastHeartbeat)
    {
        return new TaskEntity
        {
            TaskId = taskId,
            Status = JobTaskStatus.Running,
            Body = "Stalled task",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            WorkerPodId = workerId,
            LastHeartbeat = lastHeartbeat
        };
    }
}