using subscriber.Services.Queues.Azure;
using subscriber.Services.Queues.Aliyun;

namespace subscriber.Services.Queues;
public enum QueueProvider
{
    AliyunMNS,
    AzureServiceBus
}

public interface IQueueClientFactory
{
    IQueueClient GetClient(QueueProvider provider);
}

public class QueueClientFactory : IQueueClientFactory
{
    private readonly IDictionary<QueueProvider, IQueueClient> _clients;

    public QueueClientFactory(IEnumerable<IQueueClient> clients)
    {
        _clients = clients.ToDictionary(
            client => GetProviderType(client),
            client => client);
    }

    public IQueueClient GetClient(QueueProvider provider)
    {
        if (!_clients.TryGetValue(provider, out var client))
        {
            throw new ArgumentException($"No client registered for provider {provider}");
        }
        return client;
    }

    private static QueueProvider GetProviderType(IQueueClient client) => client switch
    {
        AliyunMnsClient => QueueProvider.AliyunMNS,
        AzureServiceBusClient => QueueProvider.AzureServiceBus,
        _ => throw new ArgumentException($"Unknown client type: {client.GetType().Name}")
    };
}