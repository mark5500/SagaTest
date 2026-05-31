namespace SagaThing.Messages;

public record BatchStale
{
    public Guid CorrelationId { get; init; }
    public int FailedItemIndex { get; init; }
}
