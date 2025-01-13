using Quartz;
using subscriber.Jobs;
using subscriber.Services.Queues.Aliyun;
using mongodb_service.Extensions;
using subscriber.Services.Queues;

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
});

// Add the Quartz.NET hosted service
builder.Services.AddQuartzHostedService(q => q.WaitForJobsToComplete = true);

// Add queue services
builder.Services.Configure<AliyunMnsConfiguration>(
    builder.Configuration.GetSection("AliyunMns"));
builder.Services.AddSingleton<IQueueClient, AliyunMnsClient>();
builder.Services.AddSingleton<IQueueClientFactory>(sp =>
{
    var clients = new List<IQueueClient> { sp.GetRequiredService<IQueueClient>() };
    var factory = new QueueClientFactory(clients);
    // Initialize the Aliyun client
    factory.GetClient(QueueProvider.AliyunMNS).Initialize(CancellationToken.None);
    return factory;
});

// OR for Azure Service Bus
// builder.Services.AddSingleton<IMessageQueueService, ServiceBusQueueService>();

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
