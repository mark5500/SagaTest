using MassTransit;
using SagaThing.Messages;

namespace SagaThing.Consumers;

public class ProcessItemConsumer(ILogger<ProcessItemConsumer> logger) : IConsumer<ProcessItem>
{
    private static readonly Random Rng = Random.Shared;

    public async Task Consume(ConsumeContext<ProcessItem> context)
    {
        logger.LogInformation(
            "Processing item {Index} for batch {BatchId} tenant {TenantId}",
            context.Message.ItemIndex, context.Message.BatchId, context.Message.TenantId);

        if (Rng.NextDouble() < 0.05)
        {
            logger.LogWarning("💥 Simulated failure for item {Index} (batch {BatchId})",
                context.Message.ItemIndex, context.Message.BatchId);
            throw new InvalidOperationException("Simulated transient failure");
        }

        await Task.Delay(1000);


        await context.Publish(new ItemProcessed
        {
            CorrelationId = context.Message.BatchId,
            ItemIndex = context.Message.ItemIndex
        });
    }
}
