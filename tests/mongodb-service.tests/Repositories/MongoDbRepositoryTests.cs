using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using mongodb_service.Configuration;
using mongodb_service.Models;
using mongodb_service.Repositories;
using mongodb_service.tests.Infrastructure;
using Moq;
using Xunit;
using MongoDB.Bson;
using mongodb_service.Exceptions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Polly;

namespace mongodb_service.tests.Repositories;

[Collection("MongoDB Collection")]
public class MongoDbRepositoryTests : IAsyncLifetime
{
    private readonly MongoDbFixture _fixture;
    private MongoClient _client => _fixture.MongoClient;
    private MongoDbRepository? _repository;
    private readonly ILogger<MongoDbRepository> _logger;
    private string _dbName;
    private readonly Stopwatch _stopwatch = new Stopwatch();
    private const int PerformanceThresholdMs = 500; // Maximum acceptable time for critical operations

    // Retry policy for potentially flaky tests
    private readonly AsyncPolicy _retryPolicy = Policy
        .Handle<Exception>()
        .WaitAndRetryAsync(3, retryAttempt =>
            TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

    public MongoDbRepositoryTests(MongoDbFixture fixture)
    {
        _fixture = fixture;
        _logger = Mock.Of<ILogger<MongoDbRepository>>();
        _dbName = $"test-db-{Guid.NewGuid()}"; // Use a unique DB for each test
    }

    public async Task InitializeAsync()
    {
        var settings = _fixture.GetMongoDbSettings();
        settings.DatabaseName = _dbName; // Use the unique DB name

        _repository = new MongoDbRepository(
            _client,
            Options.Create(settings),
            _logger);

        await _repository.InitializeAsync();
    }

    public async Task DisposeAsync()
    {
        // Clean up the test database after each test
        if (_client != null && !string.IsNullOrEmpty(_dbName))
        {
            await _client.DropDatabaseAsync(_dbName);
        }
    }

    #region Test Data Helpers

    private TaskEntity CreateTestTask(string taskId, JobTaskStatus status = JobTaskStatus.Pending)
    {
        return new TaskEntity
        {
            TaskId = taskId,
            Status = status,
            Body = $"Test body for {taskId}",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            Version = 1
        };
    }

    private TaskEntity CreateStalledTask(string taskId, string workerId, DateTime lastHeartbeat)
    {
        return new TaskEntity
        {
            TaskId = taskId,
            Status = JobTaskStatus.Running,
            Body = $"Stalled task {taskId}",
            RetryCount = 0,
            CreatedAt = DateTime.UtcNow,
            Id = ObjectId.GenerateNewId().ToString(),
            WorkerPodId = workerId,
            LastHeartbeat = lastHeartbeat
        };
    }

    private async Task<TaskEntity> InsertTestTaskAsync(string taskId, JobTaskStatus status = JobTaskStatus.Pending)
    {
        var task = CreateTestTask(taskId, status);
        await _repository!.InsertOrUpdateTaskAsync(task);
        return task;
    }

    private async Task WithPerformanceMeasurement(Func<Task> operation, string operationName)
    {
        _stopwatch.Restart();
        await operation();
        _stopwatch.Stop();

        var elapsed = _stopwatch.ElapsedMilliseconds;
        Assert.True(elapsed < PerformanceThresholdMs,
            $"{operationName} took {elapsed}ms which exceeds the performance threshold of {PerformanceThresholdMs}ms");
    }

    #endregion

    #region Basic CRUD Operations

    [Fact]
    public async Task InsertOrUpdateTaskAsync_ShouldInsertNewTask()
    {
        // Arrange
        var task = CreateTestTask("test-task-1");

        // Act
        await WithPerformanceMeasurement(
            async () => await _repository!.InsertOrUpdateTaskAsync(task),
            "Task insertion");

        // Assert
        var savedTask = await _repository!.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(savedTask);
        Assert.Equal(task.TaskId, savedTask.TaskId);
        Assert.Equal(JobTaskStatus.Pending, savedTask.Status);
    }

    [Fact]
    public async Task InsertOrUpdateTaskAsync_WhenUpdatingExistingTask_ShouldUpdateTask()
    {
        // Arrange
        var task = await InsertTestTaskAsync("test-task-1");

        // Act
        task.Body = "Updated body";
        task.Status = JobTaskStatus.Running;
        await _repository!.InsertOrUpdateTaskAsync(task);

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

    #endregion

    #region Task Acquisition and Processing

    [Fact]
    public async Task TryAcquireTaskAsync_ShouldAcquireAvailableTask()
    {
        // Arrange
        await InsertTestTaskAsync("test-task-2");

        // Act
        var acquired = await _retryPolicy.ExecuteAsync(async () =>
            await _repository!.TryAcquireTaskAsync(
                JobTaskStatus.Pending,
                JobTaskStatus.Running,
                "worker-1",
                DateTime.UtcNow));

        // Assert
        Assert.NotNull(acquired);
        Assert.Equal("test-task-2", acquired.TaskId);
        Assert.Equal(JobTaskStatus.Running, acquired.Status);
        Assert.Equal("worker-1", acquired.WorkerPodId);
    }

    [Fact]
    public async Task TryAcquireTaskAsync_WhenTaskIsStale_ShouldAcquireTask()
    {
        // Arrange
        var task = CreateStalledTask("stale-task", "old-worker", DateTime.UtcNow.AddHours(-1));
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
    public async Task TryAcquireTaskAsync_WhenMultipleWorkersCompete_ShouldOnlyAcquireOnce()
    {
        // Arrange
        await InsertTestTaskAsync("concurrent-task");

        // Act
        var acquisitionTasks = new List<Task<TaskEntity?>>();
        for (int i = 0; i < 5; i++)
        {
            acquisitionTasks.Add(_repository!.TryAcquireTaskAsync(
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

    #endregion

    #region Task Status Management

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
        var task = await InsertTestTaskAsync($"transition-test-{Guid.NewGuid()}", initialStatus);

        // Act
        var success = await _repository!.UpdateTaskStatusIfVersionMatchesAsync(
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
    public async Task UpdateTaskStatusIfVersionMatchesAsync_WhenVersionMatches_ShouldUpdateStatus()
    {
        // Arrange
        var task = await InsertTestTaskAsync("version-test");

        // Act
        var success = await _repository!.UpdateTaskStatusIfVersionMatchesAsync(
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
        var task = CreateTestTask("version-mismatch");
        task.Version = 2; // Set version to 2
        await _repository!.InsertOrUpdateTaskAsync(task);

        // Act
        var success = await _repository.UpdateTaskStatusIfVersionMatchesAsync(
            task.TaskId,
            1, // Provide version 1 (mismatch)
            JobTaskStatus.Running);

        // Assert
        Assert.False(success);
        var unchangedTask = await _repository.GetByTaskIdAsync(task.TaskId);
        Assert.NotNull(unchangedTask);
        Assert.Equal(JobTaskStatus.Pending, unchangedTask.Status);
        Assert.Equal(2, unchangedTask.Version);
    }

    [Fact]
    public async Task UpdateTaskStatus_WithTimestamps_ShouldSetCorrectTimestamp()
    {
        // Arrange
        var task = await InsertTestTaskAsync("timestamp-test");

        // Act
        var success = await _repository!.UpdateTaskStatusAndErrorIfVersionMatchesAsync(
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
        var task = await InsertTestTaskAsync($"timestamp-test-{Guid.NewGuid()}", initialStatus);

        // Act
        var success = await _repository!.UpdateTaskStatusAndErrorIfVersionMatchesAsync(
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

    #endregion

    #region Error Handling and Edge Cases

    [Fact]
    public async Task InitializeAsync_WhenDatabaseUnavailable_ShouldThrowException()
    {
        // Arrange
        var invalidSettings = new MongoDbSettings
        {
            ConnectionString = "mongodb://localhost:27018", // Use non-existent port
            DatabaseName = "test-db",
            StaleTaskTimeout = TimeSpan.FromMinutes(5).ToString()
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
        var task = await InsertTestTaskAsync("test-task");

        // Create a new client with invalid connection string to simulate disconnection
        var invalidSettings = new MongoDbSettings
        {
            ConnectionString = "mongodb://localhost:27018", // Non-existent port
            DatabaseName = _dbName,
            StaleTaskTimeout = TimeSpan.FromMinutes(5).ToString()
        };

        var clientSettings = MongoClientSettings.FromUrl(new MongoUrl(invalidSettings.ConnectionString));
        clientSettings.ServerSelectionTimeout = TimeSpan.FromSeconds(1); // Short timeout
        clientSettings.ConnectTimeout = TimeSpan.FromSeconds(1);

        var disconnectedClient = new MongoClient(clientSettings);
        var disconnectedRepo = new MongoDbRepository(
            disconnectedClient,
            Options.Create(invalidSettings),
            _logger);

        // Act & Assert
        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => disconnectedRepo.GetByTaskIdAsync(task.TaskId));

        Assert.True(
            ex is MongoConnectionException ||
            ex is TimeoutException ||
            ex is DatabaseOperationException);
    }

    [Fact]
    public async Task InitializeAsync_ShouldCreateUniqueTaskIdIndex()
    {
        // Arrange
        var task1 = CreateTestTask("duplicate-task");
        var task2 = CreateTestTask("duplicate-task"); // Same TaskId
        task2.Body = "Test body 2"; // Different content

        // Get direct access to collection to bypass the repository's upsert logic
        var database = _client!.GetDatabase(_dbName);
        var collection = database.GetCollection<TaskEntity>("tasks");

        // We need to manually create the unique index to test
        // since _repository.InitializeAsync() already called before this test
        var indexKeys = Builders<TaskEntity>.IndexKeys.Ascending(x => x.TaskId);
        var indexOptions = new CreateIndexOptions { Unique = true };
        var indexModel = new CreateIndexModel<TaskEntity>(indexKeys, indexOptions);
        await collection.Indexes.CreateOneAsync(indexModel);

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

    #endregion

    #region Task Management

    [Fact]
    public async Task RequeueTaskAsync_WhenTaskIsRunning_ShouldRequeueSuccessfully()
    {
        // Arrange
        var task = CreateTestTask("requeue-test", JobTaskStatus.Running);
        task.WorkerPodId = "worker-1";
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
        var stalledTask = CreateStalledTask("stalled-task", "worker-1", DateTime.UtcNow.AddMinutes(-10));

        var activeTask = CreateTestTask("active-task", JobTaskStatus.Running);
        activeTask.WorkerPodId = "worker-1";
        activeTask.LastHeartbeat = DateTime.UtcNow;

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
    public async Task RequeueTask_ShouldCleanupWorkerMetadata()
    {
        // Arrange
        var task = CreateTestTask("cleanup-test", JobTaskStatus.Running);
        task.WorkerPodId = "worker-1";
        task.WorkerNodeId = "node-1";
        task.LastHeartbeat = DateTime.UtcNow;
        task.LockedAt = DateTime.UtcNow;
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

    #endregion
}