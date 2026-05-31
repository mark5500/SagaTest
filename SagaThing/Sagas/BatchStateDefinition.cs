using MassTransit;
using SagaThing.Messages;

namespace SagaThing.Sagas;

public class BatchStateDefinition : SagaDefinition<BatchState>
{
    private const ushort SagaConcurrency = 32;

    protected override void ConfigureSaga(
        IReceiveEndpointConfigurator endpointConfigurator,
        ISagaConfigurator<BatchState> sagaConfigurator,
        IRegistrationContext context)
    {
        endpointConfigurator.UseInMemoryOutbox(context);

        endpointConfigurator.PrefetchCount = SagaConcurrency;
        endpointConfigurator.ConcurrentMessageLimit = SagaConcurrency;
        
        // Partition by CorrelationId so each saga instance is updated serially.
        var partition = endpointConfigurator.CreatePartitioner(SagaConcurrency);

        sagaConfigurator.Message<ItemProcessed>(x =>
            x.UsePartitioner(partition, m => m.Message.CorrelationId));
    }
}
