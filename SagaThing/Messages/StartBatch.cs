using MassTransit;

namespace SagaThing.Messages;

public record StartBatch
{
    public Guid CorrelationId { get; init; } = NewId.NextGuid();
    public string TenantId { get; init; } = "standard";
    public int ItemCount { get; init; }
}
