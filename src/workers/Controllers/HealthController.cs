using Microsoft.AspNetCore.Mvc;
using mongodb_service.Services;

namespace workers.Controllers;

[ApiController]
[Route("health")]
public class HealthController : ControllerBase
{
    private readonly ILogger<HealthController> _logger;
    private readonly IMongoDbRepository _mongoDb;

    public HealthController(ILogger<HealthController> logger, IMongoDbRepository mongoDb)
    {
        _logger = logger;
        _mongoDb = mongoDb;
    }

    [HttpGet]
    public async Task<IActionResult> Get()
    {
        try
        {
            // Check MongoDB connection
            await _mongoDb.PingAsync();

            return Ok(new
            {
                status = "healthy",
                timestamp = DateTime.UtcNow,
                podId = Environment.GetEnvironmentVariable("POD_NAME") ?? "unknown"
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Health check failed");
            return StatusCode(500, new
            {
                status = "unhealthy",
                error = ex.Message,
                timestamp = DateTime.UtcNow
            });
        }
    }
}