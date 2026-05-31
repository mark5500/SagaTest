using MassTransit;
using SagaThing.Consumers;
using SagaThing.Messages;
using SagaThing.Sagas;

var builder = WebApplication.CreateBuilder(args);

builder.AddServiceDefaults();
builder.AddRedisClient("redis");

var redisConnectionString = builder.Configuration.GetConnectionString("redis")
                            ?? "localhost:6379";

var tenantProfiles = new Dictionary<string, (ushort Prefetch, int ConcurrentLimit)>(StringComparer.OrdinalIgnoreCase)
{
    ["standard"] = (8, 8),
    ["gold"] = (32, 32)
};

builder.Services.AddMassTransit(busConfigurator =>
{
    busConfigurator.AddSagaStateMachine<BatchStateMachine, BatchState, BatchStateDefinition>()
        .RedisRepository(redisConnectionString);

    busConfigurator.AddConsumer<ProcessItemConsumer>();
    busConfigurator.AddConsumer<BatchCompletedConsumer>();

    busConfigurator.UsingRabbitMq((context, cfg) =>
    {
        var rabbitConnectionString = builder.Configuration.GetConnectionString("rabbitmq")
                                     ?? throw new InvalidOperationException("Connection string 'rabbitmq' is not configured.");

        cfg.Host(new Uri(rabbitConnectionString));

        foreach (var tenant in tenantProfiles)
        {
            var queueName = $"process-item-{tenant.Key.ToLowerInvariant()}";

            cfg.ReceiveEndpoint(queueName, endpoint =>
            {
                endpoint.PrefetchCount = tenant.Value.Prefetch;
                endpoint.ConcurrentMessageLimit = tenant.Value.ConcurrentLimit;
                endpoint.ConfigureConsumer<ProcessItemConsumer>(context);
            });
        }

        cfg.ConfigureEndpoints(context);
    });
});

var app = builder.Build();

app.MapDefaultEndpoints();

app.MapGet("/", () => "Saga POC running. GET /start-batch?count=5&tenant=standard|gold");

app.MapGet("/start-batch", async (IBus bus, string tenant = "standard", int count = 5) =>
{
    var normalizedTenant = tenant.Trim().ToLowerInvariant();
    if (!tenantProfiles.ContainsKey(normalizedTenant))
    {
        return Results.BadRequest(new
        {
            error = "Unknown tenant",
            allowed = tenantProfiles.Keys.OrderBy(x => x).ToArray()
        });
    }

    var message = new StartBatch
    {
        ItemCount = count,
        TenantId = normalizedTenant
    };

    await bus.Publish(message);
    return Results.Accepted($"/start-batch/{message.CorrelationId}",
        new { message.CorrelationId, message.ItemCount, message.TenantId });
});

app.Run();