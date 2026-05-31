using MassTransit;
using SagaThing.Messages;

namespace SagaThing.Consumers;

public class BatchStaleConsumer(ILogger<BatchStaleConsumer> logger) : IConsumer<BatchStale>
{
    public Task Consume(ConsumeContext<BatchStale> context)
    {
        logger.LogWarning(
            "⚠️ Batch {BatchId} marked stale — item {FailedItemIndex} failed permanently. Visualisation data is incomplete.",
            context.Message.CorrelationId, context.Message.FailedItemIndex);

        return Task.CompletedTask;
    }
}
