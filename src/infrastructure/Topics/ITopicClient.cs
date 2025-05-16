namespace infrastructure.Topics;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Manages Azure Service Bus topics and subscriptions
/// </summary>
public interface ITopicClient
{
    /// <summary>
    /// Initialize the topic client (create topic/subscriptions if needed)
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Publish a message to the topic
    /// </summary>
    Task PublishMessageAsync(
        string messageBody,
        IDictionary<string, string>? properties = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Receive messages from a specific subscription.
    /// Messages received via this method must be explicitly completed, abandoned, or dead-lettered
    /// using the corresponding methods below before their lock expires.
    /// </summary>
    /// <param name="subscriptionName">The name of the subscription to receive from.</param>
    /// <param name="maxMessages">The maximum number of messages to receive.</param>
    /// <param name="maxWaitTime">The maximum time to wait for messages to arrive. Null means wait indefinitely (or until cancellation).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A collection of messages received.</returns>
    Task<IEnumerable<TopicSubscriptionMessage>> ReceiveMessagesFromSubscriptionAsync(
        string subscriptionName,
        int maxMessages,
        TimeSpan? maxWaitTime,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Complete a message (mark as processed).
    /// </summary>
    /// <param name="message">The message received from ReceiveMessagesFromSubscriptionAsync.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task CompleteMessageAsync(
        TopicSubscriptionMessage message,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Abandon a message (return to subscription for later processing).
    /// </summary>
    /// <param name="message">The message received from ReceiveMessagesFromSubscriptionAsync.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task AbandonMessageAsync(TopicSubscriptionMessage message, CancellationToken cancellationToken);

    /// <summary>
    /// Dead-letter a message (move to Dead Letter Queue).
    /// </summary>
    /// <param name="message">The message received from ReceiveMessagesFromSubscriptionAsync.</param>
    /// <param name="reason">The reason for dead-lettering.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    Task DeadLetterMessageAsync(
        TopicSubscriptionMessage message,
        string reason,
        CancellationToken cancellationToken
    );
}
