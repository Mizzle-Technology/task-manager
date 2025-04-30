using subscriber.Services.Queues.Azure;
using subscriber.Services.Queues.Aliyun;

namespace subscriber.Services.Queues;
public enum QueueProvider
{
    AliyunMNS,
    AzureServiceBus
}

// Queue interfaces and factory
public interface IQueueClientFactory
{
    IQueueClient GetClient(QueueProvider provider);
}

public class QueueClientFactory(IEnumerable<IQueueClient> clients) : IQueueClientFactory
{
    private readonly Dictionary<QueueProvider, IQueueClient> _clients = clients.ToDictionary(GetProviderType);

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

// Topic interfaces and factory
public interface ITopicClientFactory
{
    ITopicClient GetTopicClient();
}

public class TopicClientFactory : ITopicClientFactory
{
    private readonly ITopicClient _topicClient;

    public TopicClientFactory(ITopicClient topicClient)
    {
        _topicClient = topicClient ?? throw new ArgumentNullException(nameof(topicClient));
    }

    public ITopicClient GetTopicClient() => _topicClient;
}