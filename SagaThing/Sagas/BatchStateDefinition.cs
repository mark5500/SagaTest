using MassTransit;
using MassTransit.RabbitMqTransport;
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
        endpointConfigurator.UseMessageScheduler(new Uri("queue:quartz"));
        endpointConfigurator.UseInMemoryOutbox(context);

        endpointConfigurator.PrefetchCount = SagaConcurrency;
        endpointConfigurator.ConcurrentMessageLimit = SagaConcurrency;

        // Only one replica actively consumes the saga queue at a time, eliminating cross-replica
        // version conflicts. RabbitMQ automatically fails over to another replica if the active one drops.
        if (endpointConfigurator is IRabbitMqReceiveEndpointConfigurator rmq)
            rmq.SetQueueArgument("x-single-active-consumer", true);

        // Partition by CorrelationId so each saga instance is updated serially within the active replica.
        var partition = endpointConfigurator.CreatePartitioner(SagaConcurrency);

        sagaConfigurator.Message<ItemProcessed>(x =>
            x.UsePartitioner(partition, m => m.Message.CorrelationId));
        sagaConfigurator.Message<BatchStale>(x =>
            x.UsePartitioner(partition, m => m.Message.CorrelationId));
    }
}
