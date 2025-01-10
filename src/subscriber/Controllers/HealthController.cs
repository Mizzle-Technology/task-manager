using Microsoft.AspNetCore.Mvc;
using subscriber.Attributes;

namespace subscriber.Controllers;

[ApiController]
[PublicApi]
public class HealthController : ControllerBase
{
    [HttpGet]
    public IActionResult Get()
    {
        return Ok(new { status = "healthy", timestamp = DateTime.UtcNow });
    }
}