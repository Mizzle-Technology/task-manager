namespace subscriber.Services.Queues.Exceptions;

public class MessageLockLostException : QueueOperationException
{
	public MessageLockLostException(string message) : base(message) { }
	public MessageLockLostException(string message, Exception innerException) : base(message, innerException) { }
}