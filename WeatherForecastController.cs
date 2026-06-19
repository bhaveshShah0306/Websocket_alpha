using Microsoft.AspNetCore.Mvc;
using WebSocket_alpha.Services;

namespace WebSocket_alpha.Controllers


{
    public record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
    {
        public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
    }
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly WebConnectionManager _connections;
        public WeatherForecastController(ILogger<WeatherForecastController> logger, WebConnectionManager connection)
        {
            _logger = logger;
            _connections = connection;
        }

        [HttpGet(Name = "GetWeatherForecast")]
        public IEnumerable<WeatherForecast> Get() =>
     Enumerable.Range(1, 5).Select(index =>
         new WeatherForecast(
             DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
             Random.Shared.Next(-20, 55),
             Summaries[Random.Shared.Next(Summaries.Length)]))
     .ToArray();

        [HttpGet("ws-status")]
        public IActionResult WebSocketStatus()
        {
            var clients = _connections.All().Select(c => new
            {
                c.Id,
                c.Room,
                ConnectedAt = c.ConnectedAt.ToString("o"),
                SocketState = c.Socket.State.ToString()
            });

            return Ok(new
            {
                TotalConnections = _connections.Count,
                Clients = clients
            });
        }
    }
}
