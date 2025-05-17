using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using infrastructure.Configuration.ServiceBus;
using infrastructure.Queues.Exceptions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace infrastructure.Queues.Azure;

public class AzureServiceBusClient : IQueueClient
{
    private readonly ILogger<AzureServiceBusClient> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ServiceBusClient _client;
    private readonly ServiceBusConfiguration _config;
    private readonly Dictionary<string, ServiceBusQueue> _queues;
    private bool _isInitialized;

    public AzureServiceBusClient(
        ILogger<AzureServiceBusClient> logger,
        ILoggerFactory loggerFactory,
        IOptions<ServiceBusConfiguration> config
    )
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _config = config?.Value ?? throw new ArgumentNullException(nameof(config));
        _queues = [];
        _client = new ServiceBusClient(_config.ConnectionString);
    }

    public void Initialize(CancellationToken cancellationToken)
    {
        if (_isInitialized)
            return;

        try
        {
            // Create receivers and senders for configured queues
            foreach (var queueName in _config.QueueNames)
            {
                var receiver = _client.CreateReceiver(queueName);
                var sender = _client.CreateSender(queueName);

                _queues[queueName] = new ServiceBusQueue(
                    _loggerFactory.CreateLogger<ServiceBusQueue>(),
                    receiver,
                    sender
                );
            }

            _isInitialized = true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to initialize Service Bus client");
            throw new QueueOperationException("Failed to initialize Service Bus client", ex);
        }
    }

    public IMessageQueue GetQueue(string queueName)
    {
        if (!_isInitialized)
        {
            throw new InvalidOperationException(
                "Service Bus client not initialized. Call InitializeAsync first."
            );
        }

        if (!_queues.TryGetValue(queueName, out var queue))
        {
            throw new QueueNotFoundException($"Queue {queueName} not found or not initialized");
        }

        return queue;
    }

    public async Task<QueueHealth> GetQueueHealthAsync(CancellationToken cancellationToken)
    {
        try
        {
            var adminClient = new ServiceBusAdministrationClient(_config.ConnectionString);
            var queueProperties = await adminClient.GetQueueRuntimePropertiesAsync(
                _config.QueueNames.First(),
                cancellationToken
            );

            return new QueueHealth(
                IsHealthy: true,
                Status: "Healthy",
                ActiveMessageCount: queueProperties.Value.ActiveMessageCount,
                DeadLetterMessageCount: queueProperties.Value.DeadLetterMessageCount
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to get Service Bus queue health");
            return new QueueHealth(
                IsHealthy: false,
                Status: ex.Message,
                ActiveMessageCount: 0,
                DeadLetterMessageCount: 0
            );
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_client != null)
        {
            await _client.DisposeAsync().ConfigureAwait(false);
        }
    }
}
