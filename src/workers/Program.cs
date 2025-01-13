using workers;
using mongodb_service.Extensions;
using workers.Services;

var builder = WebApplication.CreateBuilder(args);

// Add MongoDB services
builder.Services.AddMongoDb(builder.Configuration);

// Add worker
builder.Services.AddSingleton<ITaskManager, TaskManager>();
builder.Services.AddSingleton<ITaskProcessor, TaskProcessor>();
builder.Services.AddHostedService<Worker>();

builder.Services.AddControllers();

var app = builder.Build();
app.MapControllers();
app.Run();
