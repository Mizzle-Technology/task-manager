using infrastructure.Queues;
using infrastructure.Queues.Exceptions;
using Microsoft.Extensions.Options;
using Polly;
using subscriber.Configuration;

namespace subscriber.Services;

public class QueueListenerService(
    IOptions<QueueListenerConfiguration> options,
    IQueueClientFactory clientFactory,
    ILogger<QueueListenerService> logger
) : BackgroundService
{
    private readonly IQueueClientFactory _clientFactory =
        clientFactory ?? throw new ArgumentNullException(nameof(clientFactory));
    private readonly QueueListenerConfiguration _config =
        options?.Value ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<QueueListenerService> _logger =
        logger ?? throw new ArgumentNullException(nameof(logger));

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Determine provider (default to AliyunMNS if blank)
        QueueProvider provider;
        if (string.IsNullOrWhiteSpace(_config.Provider))
        {
            provider = QueueProvider.AliyunMNS;
        }
        else if (!Enum.TryParse(_config.Provider, true, out provider))
        {
            throw new InvalidOperationException($"Invalid queue provider '{_config.Provider}'");
        }

        var queueClient = _clientFactory.GetClient(provider);
        queueClient.Initialize(stoppingToken);
        var queue = queueClient.GetQueue(_config.QueueName);

        _logger.LogInformation(
            "Starting QueueListenerService for provider {Provider}, queue {QueueName}",
            provider,
            _config.QueueName
        );

        // Retry/back-off policy on receive errors
        var receiveRetryPolicy = Policy
            .Handle<QueueOperationException>()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                onRetry: (ex, ts, attempt, ctx) =>
                    _logger.LogWarning(
                        ex,
                        "Receive attempt {Attempt} failed, backing off {Delay}s",
                        attempt,
                        ts.TotalSeconds
                    )
            );
        while (!stoppingToken.IsCancellationRequested)
        {
            // Pull messages with retry/back-off
            IEnumerable<IQueueMessage> messages;
            try
            {
                messages = await receiveRetryPolicy.ExecuteAsync(
                    ct =>
                        queue.ReceiveMessagesAsync(
                            _config.BatchSize,
                            TimeSpan.FromSeconds(_config.PollingWaitSeconds),
                            ct
                        ),
                    stoppingToken
                );
            }
            catch (Exception ex)
            {
                _logger.LogError(
                    ex,
                    "ReceiveMessagesAsync failed after retries for queue {QueueName}",
                    _config.QueueName
                );
                await Task.Delay(TimeSpan.FromSeconds(_config.PollingWaitSeconds), stoppingToken);
                continue;
            }

            foreach (var message in messages)
            {
                try
                {
                    // TODO: implement your message handling logic here
                    _logger.LogInformation(
                        "Processing message {MessageId}: {Body}",
                        message.MessageId,
                        message.Body
                    );

                    await queue.CompleteMessageAsync(message, stoppingToken);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message {MessageId}", message.MessageId);
                    if (_config.DeadLetterFailedMessages)
                        await queue.DeadLetterMessageAsync(message, ex.Message, stoppingToken);
                    else
                        await queue.AbandonMessageAsync(message, stoppingToken);
                }
            }

            // Wait before polling again
            await Task.Delay(TimeSpan.FromSeconds(_config.PollingWaitSeconds), stoppingToken);
        }
    }
}
