namespace subscriber.Services.Topics;

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