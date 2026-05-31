namespace SagaThing.Messages;

public record BatchCompleted
{
    public Guid BatchId { get; init; }
    public int TotalItems { get; init; }
}
