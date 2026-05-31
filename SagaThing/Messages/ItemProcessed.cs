namespace SagaThing.Messages;

public record ItemProcessed
{
    // Must match saga CorrelationId so MassTransit can route it back
    public Guid CorrelationId { get; init; }
    public int ItemIndex { get; init; }
}
