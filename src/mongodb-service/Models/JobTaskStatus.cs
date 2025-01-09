namespace mongodb_service.Models;

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