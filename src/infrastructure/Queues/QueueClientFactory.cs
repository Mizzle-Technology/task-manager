using infrastructure.Queues.Aliyun;
using infrastructure.Queues.Azure;

namespace infrastructure.Queues;

public enum QueueProvider
{
    AliyunMNS,
    AzureServiceBus,
}

// Queue interfaces and factory
public interface IQueueClientFactory
{
    IQueueClient GetClient(QueueProvider provider);
}

public class QueueClientFactory(IEnumerable<IQueueClient> clients) : IQueueClientFactory
{
    private readonly Dictionary<QueueProvider, IQueueClient> _clients = clients.ToDictionary(
        GetProviderType
    );

    public IQueueClient GetClient(QueueProvider provider)
    {
        if (!_clients.TryGetValue(provider, out var client))
        {
            throw new ArgumentException($"No client registered for provider {provider}");
        }
        return client;
    }

    private static QueueProvider GetProviderType(IQueueClient client) =>
        client switch
        {
            AliyunMnsClient => QueueProvider.AliyunMNS,
            AzureServiceBusClient => QueueProvider.AzureServiceBus,
            _ => throw new ArgumentException($"Unknown client type: {client.GetType().Name}"),
        };
}
