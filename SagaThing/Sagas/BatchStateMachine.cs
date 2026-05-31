using MassTransit;
using SagaThing.Messages;

namespace SagaThing.Sagas;

public class BatchStateMachine : MassTransitStateMachine<BatchState>
{
    public State Processing { get; private set; } = null!;
    public State Completed { get; private set; } = null!;
    public State Stale { get; private set; } = null!;

    public Event<StartBatch> BatchStarted { get; private set; } = null!;
    public Event<ItemProcessed> ItemProcessed { get; private set; } = null!;
    public Event<BatchStale> BatchStale { get; private set; } = null!;

    public Schedule<BatchState, BatchTimeout> BatchTimeoutSchedule { get; private set; } = null!;

    public BatchStateMachine()
    {
        InstanceState(x => x.CurrentState);

        Event(() => BatchStarted, x => x.CorrelateById(m => m.Message.CorrelationId));
        Event(() => ItemProcessed, x => x.CorrelateById(m => m.Message.CorrelationId));
        Event(() => BatchStale, x => x.CorrelateById(m => m.Message.CorrelationId));

        Schedule(() => BatchTimeoutSchedule, x => x.TimeoutTokenId, s =>
        {
            s.Delay = TimeSpan.FromHours(24);
            s.Received = r => r.CorrelateById(m => m.Message.CorrelationId);
        });

        Initially(
            When(BatchStarted)
                .Then(ctx =>
                {
                    ctx.Saga.TenantId = NormalizeTenant(ctx.Message.TenantId);
                    ctx.Saga.TotalItems = ctx.Message.ItemCount;
                    ctx.Saga.ProcessedCount = 0;
                })
                .TransitionTo(Processing)
                .Schedule(BatchTimeoutSchedule, ctx => new BatchTimeout { CorrelationId = ctx.Saga.CorrelationId })
                .ThenAsync(async ctx =>
                {
                    var endpoint = await ctx.GetSendEndpoint(new Uri($"queue:process-item-{ctx.Saga.TenantId}"));

                    for (var i = 0; i < ctx.Saga.TotalItems; i++)
                    {
                        await endpoint.Send(new ProcessItem
                        {
                            TenantId = ctx.Saga.TenantId,
                            BatchId = ctx.Saga.CorrelationId,
                            ItemIndex = i
                        });
                    }
                })
        );

        During(Processing,
            When(ItemProcessed)
                .Then(ctx =>
                {
                    ctx.Saga.ProcessedCount++;
                })
                .ThenAsync(async ctx =>
                {
                    if (ctx.Saga.ProcessedCount < ctx.Saga.TotalItems)
                        return;

                    await ctx.Publish(new BatchCompleted
                    {
                        BatchId = ctx.Saga.CorrelationId,
                        TotalItems = ctx.Saga.TotalItems
                    });
                })
                .If(
                    ctx => ctx.Saga.ProcessedCount >= ctx.Saga.TotalItems,
                    binder => binder.Unschedule(BatchTimeoutSchedule).TransitionTo(Completed).Finalize()
                )
        );

        During(Processing,
            When(BatchStale)
                .Unschedule(BatchTimeoutSchedule)
                .TransitionTo(Stale)
                .Finalize()
        );

        During(Processing,
            When(BatchTimeoutSchedule.Received)
                .Then(ctx => ctx.Saga.ProcessedCount = -1) // sentinel: timed out
                .ThenAsync(async ctx => await ctx.Publish(new BatchStale
                {
                    CorrelationId = ctx.Saga.CorrelationId,
                    FailedItemIndex = -1
                }))
                .TransitionTo(Stale)
                .Finalize()
        );

        SetCompletedWhenFinalized();
    }

    private static string NormalizeTenant(string? tenantId)
    {
        return string.IsNullOrWhiteSpace(tenantId)
            ? "standard"
            : tenantId.Trim().ToLowerInvariant();
    }
}

