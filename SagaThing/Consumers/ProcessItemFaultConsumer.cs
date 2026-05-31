using MassTransit;
using SagaThing.Messages;

namespace SagaThing.Consumers;

public class ProcessItemFaultConsumer(ILogger<ProcessItemFaultConsumer> logger) : IConsumer<Fault<ProcessItem>>
{
    public async Task Consume(ConsumeContext<Fault<ProcessItem>> context)
    {
        var original = context.Message.Message;

        logger.LogError(
            "❌ Item {Index} for batch {BatchId} failed after all retries — marking batch as stale",
            original.ItemIndex, original.BatchId);

        await context.Publish(new BatchStale
        {
            CorrelationId = original.BatchId,
            FailedItemIndex = original.ItemIndex
        });
    }
}
