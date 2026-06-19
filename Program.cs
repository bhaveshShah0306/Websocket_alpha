using WebSocket_alpha.Services;
using WebSocket_alpha.Middleware;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();

builder.Services.AddSingleton<WebConnectionManager>();

// Register TelemetryService as a hosted background service.
// Mirrors: architect's Main() loop that runs indefinitely until Console.ReadKey().
builder.Services.AddSingleton<TelemetryService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<TelemetryService>());

builder.Services.AddLogging(cfg => cfg.AddConsole());
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();
