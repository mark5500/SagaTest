using MassTransit;

namespace SagaThing.Sagas;

// ISagaVersion is required by MassTransit.Redis for optimistic concurrency
public class BatchState : SagaStateMachineInstance, ISagaVersion
{
    public Guid CorrelationId { get; set; }
    public string CurrentState { get; set; } = null!;
    public string TenantId { get; set; } = "standard";
    public int TotalItems { get; set; }
    public int ProcessedCount { get; set; }
    public int Version { get; set; }
    public Guid? TimeoutTokenId { get; set; }
}
