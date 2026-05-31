using MassTransit;
using SagaThing.Messages;

namespace SagaThing.Consumers;

public class ProcessItemConsumer(ILogger<ProcessItemConsumer> logger) : IConsumer<ProcessItem>
{
    public async Task Consume(ConsumeContext<ProcessItem> context)
    {
        logger.LogInformation(
            "Processing item {Index} for batch {BatchId} tenant {TenantId}",
            context.Message.ItemIndex, context.Message.BatchId, context.Message.TenantId);


        await context.Publish(new ItemProcessed
        {
            CorrelationId = context.Message.BatchId,
            ItemIndex = context.Message.ItemIndex
        });
    }
}
