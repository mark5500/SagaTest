namespace SagaThing.Messages;

public record ProcessItem
{
    public string TenantId { get; init; } = "standard";
    public Guid BatchId { get; init; }
    public int ItemIndex { get; init; }
}
