using Aliyun.MNS;
using Microsoft.Extensions.Options;
using Aliyun.MNS.Model;
using subscriber.Services.Queues.Exceptions;

namespace subscriber.Services.Queues.Aliyun;

public class AliyunMnsClient : IQueueClient
{
    private readonly ILogger<AliyunMnsClient> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly IMNS _mnsClient;
    private readonly AliyunMnsConfiguration _config;
    private readonly Dictionary<string, AliyunMnsQueue> _queues;
    private bool _isInitialized;

    public AliyunMnsClient(
        ILogger<AliyunMnsClient> logger,
        ILoggerFactory loggerFactory,
        IOptions<AliyunMnsConfiguration> config)
    {
        _logger = logger;
        _loggerFactory = loggerFactory;
        _config = config.Value;
        _queues = new Dictionary<string, AliyunMnsQueue>();
        _mnsClient = new MNSClient(
            _config.AccessKeyId,
            _config.AccessKeySecret,
            _config.Endpoint);
    }

    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (_isInitialized) return;

        try
        {
            await EnsureQueuesExistAsync(cancellationToken);
            _isInitialized = true;
        }
        catch (MNSException ex)
        {
            _logger.LogError(ex, "Failed to initialize MNS client. ErrorCode: {ErrorCode}", ex.ErrorCode);
            throw new QueueOperationException("Failed to initialize MNS client", ex);
        }
    }

    public IMessageQueue GetQueue(string queueName)
    {
        if (!_isInitialized)
        {
            throw new InvalidOperationException("MNS client not initialized. Call InitializeAsync first.");
        }

        if (!_queues.TryGetValue(queueName, out var queue))
        {
            throw new QueueNotFoundException($"Queue {queueName} not found or not initialized");
        }

        return queue;
    }

    public async Task<QueueHealth> GetQueueHealthAsync(CancellationToken cancellationToken)
    {
        if (!_isInitialized)
        {
            return new QueueHealth(false, "Not initialized", 0, 0);
        }

        try
        {
            var queue = _mnsClient.GetNativeQueue(_config.QueueName);
            var attributes = await Task.Run(() => queue.GetAttributes(), cancellationToken);

            return new QueueHealth(
                true,
                "Healthy",
                (int)attributes.Attributes.ActiveMessages,
                (int)attributes.Attributes.InactiveMessages
                );
        }
        catch (MNSException ex)
        {
            _logger.LogError(ex, "Failed to get MNS queue health. ErrorCode: {ErrorCode}", ex.ErrorCode);
            return new QueueHealth(false, ex.Message, 0, 0);
        }
    }

    private async Task EnsureQueuesExistAsync(CancellationToken cancellationToken)
    {
        try
        {
            var queueAttributes = new QueueAttributes
            {
                DelaySeconds = 0,
                MaximumMessageSize = 65536,
                MessageRetentionPeriod = 345600,
                VisibilityTimeout = 30,
                PollingWaitSeconds = 30,
                LoggingEnabled = true
            };

            // Create DLQ first
            var deadLetterQueue = await CreateQueueIfNotExistsAsync(
                _config.DeadLetterQueueName,
                new CreateQueueRequest(_config.DeadLetterQueueName, queueAttributes),
                cancellationToken);

            var mainQueue = await CreateQueueIfNotExistsAsync(
                _config.QueueName,
                new CreateQueueRequest(_config.QueueName, queueAttributes),
                cancellationToken);

            _queues[_config.QueueName] = new AliyunMnsQueue(
                _loggerFactory.CreateLogger<AliyunMnsQueue>(),
                mainQueue,
                deadLetterQueue);
        }
        catch (MNSException ex)
        {
            _logger.LogError(ex, "Failed to ensure queues exist. ErrorCode: {ErrorCode}", ex.ErrorCode);
            throw new QueueOperationException("Failed to initialize queues", ex);
        }
    }

    private async Task<Queue> CreateQueueIfNotExistsAsync(
        string queueName,
        CreateQueueRequest request,
        CancellationToken cancellationToken)
    {
        var queue = _mnsClient.GetNativeQueue(queueName);
        try
        {
            await Task.Run(() => queue.GetAttributes(), cancellationToken);
            _logger.LogInformation("Queue {QueueName} already exists", queueName);
        }
        catch (MNSException ex) when (ex.ErrorCode == "QueueNotExist")
        {
            _logger.LogInformation("Creating queue {QueueName}", queueName);
            await Task.Run(() => _mnsClient.CreateQueue(request), cancellationToken);
        }

        return queue;
    }

    public async ValueTask DisposeAsync()
    {
        if (_mnsClient is IDisposable disposable)
        {
            disposable.Dispose();
        }
        await Task.CompletedTask;
    }
}