using infrastructure.Configuration.ServiceBus;
using infrastructure.Queues;
using infrastructure.Queues.Aliyun;
using infrastructure.Queues.Azure;
using infrastructure.Topics.Azure;
using mongodb_service.Extensions;
using subscriber.Configuration;
using subscriber.Services;
using ITopicClient = infrastructure.Topics.ITopicClient;
using ITopicClientFactory = infrastructure.Topics.ITopicClientFactory;
using TopicClientFactory = infrastructure.Topics.TopicClientFactory;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddControllers();

// Add queue services
builder.Services.Configure<AliyunMnsConfiguration>(builder.Configuration.GetSection("AliyunMns"));
builder.Services.AddSingleton<IQueueClient, AliyunMnsClient>();

// OR for Azure Service Bus
builder.Services.Configure<ServiceBusConfiguration>(
    builder.Configuration.GetSection("AzureServiceBus")
);
builder.Services.AddSingleton<IQueueClient, AzureServiceBusClient>();

// Factory to select the right IQueueClient at runtime
builder.Services.AddSingleton<IQueueClientFactory, QueueClientFactory>();

// Configure and start queue listener service
builder.Services.Configure<QueueListenerConfiguration>(
    builder.Configuration.GetSection("QueueListener")
);
builder.Services.AddHostedService<QueueListenerService>();

// Add topic services
builder.Services.AddSingleton<ITopicClient, AzureServiceBusTopic>();
builder.Services.AddSingleton<ITopicClientFactory, TopicClientFactory>();

// Update factory registration with both clients

// Add MongoDB service
builder.Services.AddMongoDb(builder.Configuration);

var app = builder.Build();

// Just use this
app.MapControllers();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.Run();
