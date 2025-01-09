using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using mongodb_service.Configuration;
using mongodb_service.Models;
using mongodb_service.Services;
using mongodb_service.tests.Infrastructure;
using MongoDB.Driver;
using Moq;
using Xunit;
using MongoDB.Bson;

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
        Assert.Equal(JobTaskStatus.Running, updatedTask!.Status);
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
        Assert.Equal(JobTaskStatus.Pending, unchangedTask!.Status);
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
        Assert.Equal(JobTaskStatus.Pending, requeuedTask!.Status);
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
}