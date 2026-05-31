var builder = DistributedApplication.CreateBuilder(args);

var rabbitMq = builder.AddRabbitMQ("rabbitmq");
var redis = builder.AddRedis("redis");

builder.AddProject<Projects.SagaThing>("sagathing")
	.WaitFor(rabbitMq)
	.WaitFor(redis)
	.WithReference(rabbitMq)
	.WithReference(redis);

builder.Build().Run();
