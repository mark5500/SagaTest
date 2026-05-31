namespace SagaThing.Messages;

public record BatchTimeout
{
    public Guid CorrelationId { get; init; }
}
