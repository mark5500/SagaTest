var builder = DistributedApplication.CreateBuilder(args);

var rabbitMq = builder.AddRabbitMQ("rabbitmq");
var redis = builder.AddRedis("redis");
var postgres = builder.AddPostgres("postgres").AddDatabase("quartzdb");

builder.AddProject<Projects.SagaThing>("sagathing")
	.WaitFor(rabbitMq)
	.WaitFor(redis)
	.WaitFor(postgres)
	.WithReference(rabbitMq)
	.WithReference(redis)
	.WithReference(postgres)
	.WithReplicas(6);

builder.Build().Run();
