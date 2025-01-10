namespace subscriber.Services.Queues.Exceptions;

public class QueueOperationException : Exception
{
    public QueueOperationException(string message) : base(message) { }
    public QueueOperationException(string message, Exception innerException)
        : base(message, innerException) { }
}

public class QueueNotFoundException : Exception
{
    public QueueNotFoundException(string message) : base(message) { }
}