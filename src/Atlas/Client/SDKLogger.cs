using System;

namespace Atlas.Client;

// LogLevel 是 SDK 调试日志级别（数值越大越细；默认 Error）。
public enum LogLevel
{
    Silence = -1,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
}

// SDKLogger 是 SDK 调试日志接口：分级打点（Debug=收发 JSON+seq+幂等键、
// Info=连接事件、Warn=超时/重发、Error=断连失败/协议错误）。接入方可用
// SDKLogger.With(LogLevel) 系列得到内置实现，或自行实现本接口接公司日志组件。
public interface SDKLogger
{
    void Debugf(string format, params object?[] args);
    void Infof(string format, params object?[] args);
    void Warnf(string format, params object?[] args);
    void Errorf(string format, params object?[] args);
}

// SDKLoggers 提供内置实现与按级别构造（对齐 Go 的 WithLog* Option）。
public static class SDKLoggerFactory
{
    // Silent 返回完全静默的实现（WithLogSilence 等价）。
    public static SDKLogger Silent()
    {
        return new LevelLogger(LogLevel.Silence, null);
    }

    // Of 按最小级别构造 Console.Error 输出的内置实现（零外部依赖）。
    public static SDKLogger Of(LogLevel min)
    {
        return min == LogLevel.Silence ? Silent() : new LevelLogger(min, null);
    }

    // Of 按最小级别与自定义输出构造（写盘/接日志组件；sink 收到完整行；可空走 Console.Error）。
    public static SDKLogger Of(LogLevel min, Action<string>? sink)
    {
        return min == LogLevel.Silence ? Silent() : new LevelLogger(min, sink);
    }
}

// LevelLogger 按最小级别过滤（sink 为空时输出 Console.Error）。
internal sealed class LevelLogger : SDKLogger
{
    private readonly LogLevel _min;
    private readonly Action<string>? _sink;

    public LevelLogger(LogLevel min, Action<string>? sink)
    {
        _min = min;
        _sink = sink;
    }

    public void Debugf(string format, params object?[] args)
    {
        Emit(LogLevel.Debug, format, args);
    }

    public void Infof(string format, params object?[] args)
    {
        Emit(LogLevel.Info, format, args);
    }

    public void Warnf(string format, params object?[] args)
    {
        Emit(LogLevel.Warn, format, args);
    }

    public void Errorf(string format, params object?[] args)
    {
        Emit(LogLevel.Error, format, args);
    }

    private void Emit(LogLevel lv, string format, object?[] args)
    {
        if ((int)lv > (int)_min)
        {
            return;
        }
        var line = $"[atlas-sdk {lv.ToString().ToLowerInvariant()} {DateTime.Now:HH:mm:ss.fff}] {string.Format(format, args)}";
        if (_sink != null)
        {
            _sink(line);
            return;
        }
        Console.Error.WriteLine(line);
    }
}
