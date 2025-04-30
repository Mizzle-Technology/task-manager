using Quartz;
using subscriber.Jobs;
using subscriber.Services.Queues.Aliyun;
using subscriber.Services.Queues.Azure;
using mongodb_service.Extensions;
using subscriber.Services.Queues;
using subscriber.Services.Topics.Azure;
using Microsoft.Extensions.Options;
using subscriber.Configuration.ServiceBus;
using subscriber.Configuration.Jobs;
using ITopicClient = subscriber.Services.Topics.ITopicClient;
using ITopicClientFactory = subscriber.Services.Topics.ITopicClientFactory;
using TopicClientFactory = subscriber.Services.Topics.TopicClientFactory;
var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddControllers();

// Add Quartz services
builder.Services.AddQuartz(q =>
{
    var jobKey = new JobKey("MessagePullJob");
    q.AddJob<MessagePullJob>(opts => opts.WithIdentity(jobKey));
    q.AddTrigger(opts => opts
        .ForJob(jobKey)
        .WithIdentity("MessagePull-trigger")
        // Run every 10 seconds
        .WithCronSchedule("0/10 * * * * ?"));

    // Topic Subscriber Job
    var topicJobKey = new JobKey("TopicSubscriberJob");
    q.AddJob<TopicSubscriberJob>(opts => opts.WithIdentity(topicJobKey));
    q.AddTrigger(opts => opts
        .ForJob(topicJobKey)
        .WithIdentity("TopicSubscriber-trigger")
        .WithCronSchedule("0/15 * * * * ?")); // Run every 15 seconds
});

// Add the Quartz.NET hosted service
builder.Services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);

// Add queue services
builder.Services.Configure<AliyunMnsConfiguration>(
    builder.Configuration.GetSection("AliyunMns"));
builder.Services.AddSingleton<IQueueClient, AliyunMnsClient>();

// OR for Azure Service Bus
builder.Services.Configure<ServiceBusConfiguration>(
    builder.Configuration.GetSection("AzureServiceBus"));
builder.Services.AddSingleton<AzureServiceBusClient>();

// Add topic services
builder.Services.AddSingleton<ITopicClient, AzureServiceBusTopic>();
builder.Services.AddSingleton<ITopicClientFactory, TopicClientFactory>();

// Add topic subscriber job configuration
builder.Services.Configure<TopicSubscriberJobConfiguration>(
    builder.Configuration.GetSection("TopicSubscriberJob"));

// Update factory registration with both clients
builder.Services.AddSingleton<IQueueClientFactory>(sp =>
{
    var aliyunClient = sp.GetRequiredService<AliyunMnsClient>();
    var azureClient = sp.GetRequiredService<AzureServiceBusClient>();

    var clients = new List<IQueueClient> { aliyunClient, azureClient };
    var factory = new QueueClientFactory(clients);

    // Parse provider from config
    var messagePullConfig = sp.GetRequiredService<IOptions<MessagePullJobConfiguration>>().Value;
    if (Enum.TryParse<QueueProvider>(builder.Configuration["MessagePullJob:Provider"], out var provider))
    {
        // Initialize only the configured client
        factory.GetClient(provider).Initialize(CancellationToken.None);
    }

    return factory;
});

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
