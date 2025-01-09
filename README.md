# Task Management Service

A distributed task management service that processes messages from multiple queue providers (Alibaba Cloud MNS, Azure Service Bus) with MongoDB for state persistence and distributed workers for task processing.

## Architecture

### Projects

- `subscriber`: Message queue subscriber service
  - Processes messages from different queue providers
  - Uses Quartz.NET for scheduled message pulling
  - Implements queue provider abstractions
  - Stores tasks in MongoDB

- `workers`: Distributed task processing service
  - Processes tasks from MongoDB
  - Supports multiple worker instances
  - Handles task lifecycle management
  - Provides heartbeat mechanism
  - Automatic task recovery
  - Scales horizontally
  
- `mongodb-service`: MongoDB data access library
  - Handles task persistence and state management
  - Provides repository pattern implementation
  - Manages task status and timestamps
  - Supports distributed locking

### Key Components

#### Message Queue Services

- Alibaba Cloud MNS implementation
  - Automatic queue creation
  - Built-in Dead Letter Queue (DLQ)
  - Configurable retry mechanism
- Azure Service Bus implementation
  - Message session support
  - FIFO message processing
  - Auto-forwarding to DLQ
  - Message deferral capability
  - Scheduled message delivery
  - Topic/Subscription pattern support
  - Built-in message batching
  - Role-based access control (RBAC)
- Extensible design through `IMessageQueueService` interface

### Message Processing

- Message queue integration (supports Aliyun MNS and Azure Service Bus)
- Batch message processing with parallel execution
- Retry mechanism with exponential backoff using Polly
- Individual message processing timeout (5 minutes)
- Configurable batch size for message pulls

#### Task Processing

- Distributed worker architecture
- Atomic task acquisition
- Pod-level task ownership
- Heartbeat monitoring
- Stalled task recovery
- Automatic retries
- Cancellation support
- Error handling

#### Data Storage

- MongoDB for task persistence
- Task status tracking
- Optimistic concurrency control
- Worker state tracking
- Timestamp tracking
- Error message logging

## Configuration

### Alibaba Cloud MNS

```json
{
  "Aliyun": {
    "AccessKeyId": "your-access-key-id",
    "AccessKeySecret": "your-access-key-secret",
    "MNSEndpoint": "https://your-queue-endpoint.mns.region.aliyuncs.com",
    "QueueName": "your-queue-name",
    "DeadLetterQueueName": "your-dead-letter-queue-name"
  }
}
```

### MongoDB

```json
{
  "MongoDB": {
    "ConnectionString": "mongodb://localhost:27017",
    "DatabaseName": "your-database-name"
  }
}
```

### Worker Settings

```json
{
  "Worker": {
    "BatchSize": 10,
    "PollingInterval": "00:00:10",
    "HeartbeatInterval": "00:00:30",
    "MaxRetries": 3
  }
}
```

### Azure Service Bus

```json
{
  "AzureServiceBus": {
    "ConnectionString": "Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key",
    "QueueName": "your-queue-name",
    "TopicName": "your-topic-name",
    "SubscriptionName": "your-subscription-name",
    "MaxConcurrentCalls": 100,
    "PrefetchCount": 50,
    "MaxAutoLockRenewalDuration": "00:05:00",
    "EnablePartitioning": true
  }
}
```

## Dependencies

- .NET 9.0
- MongoDB.Driver (3.1.0)
- Quartz (3.13.1)
- Aliyun.MNS.NETCore (1.3.8.1)
- Azure.Messaging.ServiceBus (7.18.2)
- Newtonsoft.Json (13.0.3)

## Getting Started

1. Clone the repository

   ```bash
   git clone https://github.com/yourusername/task-management.git
   ```

2. Configure settings in `appsettings.json`
   - Queue provider credentials
   - MongoDB connection string
   - Queue names and endpoints
   - Worker settings

3. Run the subscriber service

   ```bash
   cd src/subscriber
   dotnet run
   ```

4. Run worker instances

   ```bash
   cd src/workers
   dotnet run
   ```

## Deployment

### Kubernetes

The service is designed to run in Kubernetes with:

- Multiple subscriber pods for high availability
- Multiple worker pods for scalability
- Pod identity tracking
- Node affinity support
- Graceful shutdown handling

Example worker deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: task-workers
spec:
  replicas: 3
  template:
    spec:
      containers:
      - name: worker
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        - name: INSTANCE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.uid
                lifecycle:
                  preStop:
                    exec:
              command: ["/bin/sh", "-c", "sleep 30"]
```

## Error Handling

- Automatic retry with configurable attempts
- Dead Letter Queue for failed messages
- Error logging in MongoDB
- Exception tracking with stack traces
- Task state persistence
- Worker failure recovery

## Monitoring

- Task status tracking
- Worker heartbeat monitoring
- Message processing logs
- Queue health monitoring
- Error tracking and reporting
- Task processing metrics

## Development

### Adding a New Queue Provider

1. Implement `IMessageQueueService` interface
2. Add configuration in `appsettings.json`
3. Register the service in `Program.cs`

### Task States

```csharp
public enum JobTaskStatus
{
    // Initial states
    Pending = 0,
    Created = 1,

    // Subscriber states
    Processing = 10,
    Completed = 11,
    Failed = 12,

    // Worker states
    Queued = 20,        // Task is queued for worker processing
    Assigned = 21,      // Task is assigned to a worker
    Running = 22,       // Worker is actively processing
    Succeeded = 23,     // Worker completed successfully
    Error = 24,         // Worker encountered an error
    Retrying = 25,      // Worker is retrying
    Cancelled = 26,     // Task was cancelled
    Timeout = 27,       // Worker processing timed out
    
    // Final states
    Archived = 90,      // Task is archived
    Deleted = 91        // Task is marked for deletion
}
```

## Race Condition Prevention

The system implements multiple safeguards to prevent race conditions in a distributed environment:

### 1. Atomic Task Acquisition

- Uses MongoDB's `FindAndModify` operation for atomic task updates
- Guarantees only one worker can acquire a task
- Updates task status and worker assignment in a single operation
- Prevents multiple workers from claiming the same task

### 2. Unique Worker Identity

- Each worker pod has a guaranteed unique ID
- IDs persist across restarts
- Format: `{node-name}-{pod-name}-{guid}`
- Multiple workers on same node remain distinct

### 3. Task Locking

- Tasks are locked to specific worker pods
- Lock includes:
  - Worker Pod ID
  - Acquisition timestamp
  - Current status
- Other workers cannot process locked tasks

### 4. Heartbeat Mechanism

- Workers regularly update task heartbeat
- Prevents other workers from "stealing" active tasks
- Tasks only reassigned if worker stops heartbeat
- Configurable heartbeat timeout (default: 5 minutes)

### 5. Version Control

- Optimistic concurrency using document versions
- Each task update increments version
- Concurrent updates detected and rejected
- Prevents data corruption from simultaneous updates

Task lifecycle:

1. Subscriber flow: `Created -> Processing -> Completed/Failed`
2. Worker flow: `Completed -> Queued -> Assigned -> Running -> Succeeded/Error`
3. Error handling: `Error -> Retrying -> Queued`
4. Cleanup: `Succeeded/Failed -> Archived -> Deleted`.

## License

[MIT License](LICENSE)
