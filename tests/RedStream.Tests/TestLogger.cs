using Microsoft.Extensions.Logging;

namespace RedStream.Tests;

/// <summary>
/// Captures every <see cref="ILogger.Log{TState}"/> call so tests can assert against
/// what was logged without mocking the generic signature.
/// </summary>
internal sealed class TestLogger<T> : ILogger<T>
{
    public List<LogEntry> Entries { get; } = [];

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        Entries.Add(new LogEntry(logLevel, formatter(state, exception), exception));
    }
}

internal sealed record LogEntry(LogLevel Level, string Message, Exception? Exception);
