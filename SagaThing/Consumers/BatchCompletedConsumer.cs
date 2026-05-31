using MassTransit;
using SagaThing.Messages;

namespace SagaThing.Consumers;

public class BatchCompletedConsumer(ILogger<BatchCompletedConsumer> logger) : IConsumer<BatchCompleted>
{
    public Task Consume(ConsumeContext<BatchCompleted> context)
    {
        logger.LogInformation(
            "✅ Batch {BatchId} fully completed — {Total} items processed",
            context.Message.BatchId, context.Message.TotalItems);

        return Task.CompletedTask;
    }
}
