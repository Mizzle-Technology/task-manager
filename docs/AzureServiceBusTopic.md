# Azure Service Bus Topic Client

This document provides detailed information about the Azure Service Bus Topic implementation in the Otto Task Manager application.

## Overview

The `AzureServiceBusTopic` class provides a robust implementation for interacting with Azure Service Bus Topics and Subscriptions. It implements the publish-subscribe messaging pattern, allowing messages to be sent to a topic and then received by one or more subscriptions to that topic.

This implementation includes:

- Topic and subscription creation if they don't exist
- Message publishing with custom properties
- Message receiving from specific subscriptions
- Complete, abandon, and dead-letter operations for message handling
- Automatic receiver recreation for better resilience
- Proper resource disposal

## Configuration

### ServiceBusTopicConfiguration

The `ServiceBusTopicConfiguration` class provides configuration options for the topic client:

| Property                      | Type     | Default   | Description                                 |
| ----------------------------- | -------- | --------- | ------------------------------------------- |
| ConnectionString              | string   | Empty     | Azure Service Bus connection string         |
| TopicName                     | string   | Empty     | Name of the topic to use                    |
| SubscriptionNames             | string[] | []        | Names of subscriptions to initialize        |
| MessageTimeToLive             | TimeSpan | 14 days   | Default TTL for messages in the topic       |
| SubscriptionMessageTimeToLive | TimeSpan | 14 days   | Default TTL for messages in subscriptions   |
| LockDuration                  | TimeSpan | 5 minutes | Duration a message is locked for processing |

## Message Structure

### TopicSubscriptionMessage

The `TopicSubscriptionMessage` class adapts Azure Service Bus messages to the application's `IQueueMessage` interface:

| Property         | Type                        | Description                                    |
| ---------------- | --------------------------- | ---------------------------------------------- |
| MessageId        | string                      | Unique identifier for the message              |
| Body             | string                      | The message content (UTF-8 encoded)            |
| BodyBytes        | ReadOnlyMemory&lt;byte&gt;  | The raw message bytes                          |
| EnqueuedTime     | DateTime                    | When the message was added to the topic        |
| ReceiptHandle    | string                      | Token used to complete/abandon the message     |
| DeliveryCount    | uint                        | Number of delivery attempts                    |
| Properties       | IDictionary<string, string> | Custom message properties                      |
| SubscriptionName | string                      | Name of the subscription the message came from |

## Interfaces

### ITopicClient

The `ITopicClient` interface defines operations for working with topics and subscriptions:

| Method                               | Description                                                              |
| ------------------------------------ | ------------------------------------------------------------------------ |
| InitializeAsync                      | Initialize the topic client, creating topics and subscriptions if needed |
| PublishMessageAsync                  | Publish a message to the topic with optional properties                  |
| ReceiveMessagesFromSubscriptionAsync | Receive messages from a specific subscription                            |
| CompleteMessageAsync                 | Mark a message as successfully processed                                 |
| AbandonMessageAsync                  | Return a message to the subscription for reprocessing                    |
| DeadLetterMessageAsync               | Move a message to the dead-letter queue with a reason                    |

## Implementation

### AzureServiceBusTopic

The main implementation class that handles all topic and subscription operations:

#### Constructor

```csharp
public AzureServiceBusTopic(ILogger<AzureServiceBusTopic> logger, ServiceBusTopicConfiguration config)
```

- **logger**: For logging operations and errors
- **config**: Configuration for topics and subscriptions

#### Key Methods

##### InitializeAsync

```csharp
public async Task InitializeAsync(CancellationToken cancellationToken)
```

Initializes the topic client:

- Creates the topic if it doesn't exist
- Creates subscriptions if they don't exist
- Sets up receivers for each subscription
- Creates the topic sender

##### PublishMessageAsync

```csharp
public async Task PublishMessageAsync(
    string messageBody,
    IDictionary<string, string>? properties = null,
    CancellationToken cancellationToken = default)
```

Publishes a message to the topic:

- Creates a message with the provided body
- Adds custom properties if provided
- Sends the message to the topic
- Logs success or failure

##### ReceiveMessagesFromSubscriptionAsync

```csharp
public async Task<IEnumerable<TopicSubscriptionMessage>> ReceiveMessagesFromSubscriptionAsync(
    string subscriptionName,
    int maxMessages,
    TimeSpan? maxWaitTime,
    CancellationToken cancellationToken)
```

Receives messages from a specified subscription:

- Gets messages from the subscription
- Wraps each message in a TopicSubscriptionMessage object
- Returns the messages as a collection

##### CompleteMessageAsync

```csharp
public async Task CompleteMessageAsync(
    TopicSubscriptionMessage message,
    CancellationToken cancellationToken)
```

Marks a message as successfully processed:

- Uses the original Service Bus message stored in the wrapper
- Completes the message in the subscription

##### AbandonMessageAsync

```csharp
public async Task AbandonMessageAsync(
    TopicSubscriptionMessage message,
    CancellationToken cancellationToken)
```

Returns a message to the subscription for reprocessing:

- Abandons the message in the subscription

##### DeadLetterMessageAsync

```csharp
public async Task DeadLetterMessageAsync(
    TopicSubscriptionMessage message,
    string reason,
    CancellationToken cancellationToken)
```

Sends a message to the dead-letter queue:

- Moves the message to the subscription's dead-letter queue
- Includes a reason for dead-lettering

#### Helper Methods

##### GetReceiver

```csharp
private ServiceBusReceiver GetReceiver(string subscriptionName)
```

Gets a receiver for a subscription:

- Returns an existing receiver if available
- Automatically recreates the receiver if it's closed
- Throws exceptions for invalid subscription names

##### IsLockLostException

```csharp
private static bool IsLockLostException(ServiceBusException sbException)
```

Checks if an exception is due to a lost message lock:

- Determines if messages can't be completed/abandoned/dead-lettered due to lock expiration

## Error Handling

The implementation includes robust error handling:

- Specific handling for Service Bus exceptions based on their reason
- Special handling for lock lost scenarios using MessageLockLostException
- Automatic recreation of closed receivers
- Detailed logging with appropriate severity levels

## Usage Examples

### Service Registration

```csharp
// In Program.cs or Startup.cs
services.Configure<ServiceBusTopicConfiguration>(configuration.GetSection("ServiceBus:Topic"));
services.AddSingleton<ITopicClient, AzureServiceBusTopic>();
services.AddSingleton<ITopicClientFactory, TopicClientFactory>();
```

### Publishing Messages

```csharp
// Publishing a message with properties
var topicClient = serviceProvider.GetRequiredService<ITopicClient>();
await topicClient.InitializeAsync(CancellationToken.None);

var properties = new Dictionary<string, string>
{
    ["MessageType"] = "UserCreated",
    ["UserId"] = "12345"
};

await topicClient.PublishMessageAsync(
    JsonSerializer.Serialize(new { Name = "John Doe", Email = "john@example.com" }),
    properties,
    CancellationToken.None);
```

### Consuming Messages

```csharp
// In TopicSubscriberJob.cs

// Get messages from a subscription
var messages = await _topicClient.ReceiveMessagesFromSubscriptionAsync(
    "user-events-subscription",
    10,
    TimeSpan.FromSeconds(30),
    cancellationToken);

foreach (var message in messages)
{
    try {
        // Process the message
        var content = message.Body;
        var messageType = message.Properties["MessageType"];

        // Do processing...

        // Mark as complete
        await _topicClient.CompleteMessageAsync(
            message,
            cancellationToken);
    }
    catch (Exception ex) {
        // Handle failure
        await _topicClient.DeadLetterMessageAsync(
            message,
            $"Processing error: {ex.Message}",
            cancellationToken);
    }
}
```

## Best Practices

1. **Initialize Before Use**: Always call `InitializeAsync` before using the topic client
2. **Complete Messages Promptly**: Complete messages as soon as processing is finished
3. **Handle Errors with Dead-Lettering**: Use dead-lettering for messages that can't be processed
4. **Check Message Properties**: Always check if a property exists before accessing it
5. **Type Safety**: Always use TopicSubscriptionMessage when interacting with the topic client
6. **Proper Casting**: When using with generic IQueueMessage interfaces, use pattern matching for typesafe casting
7. **Dispose Properly**: The client implements `IAsyncDisposable`, use it with `await using`

## Related Components

- `TopicSubscriberJob.cs`: Quartz.NET job for processing messages from subscriptions
- `QueueClientFactory.cs`: Factory methods for getting queue and topic clients
- `ServiceBusTopicConfiguration.cs`: Configuration for the topic client
- `MessageLockLostException.cs`: Custom exception for handling lock expiration scenarios
