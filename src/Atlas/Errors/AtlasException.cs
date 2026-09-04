using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;

namespace Atlas.Errors;

// AtlasException 是 Atlas SDK 的非业务错误与业务错误的统一基类。
public abstract class AtlasException : Exception
{
    protected AtlasException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }

    // IsBusinessError 按业务 Reason 判断错误类型；Code 与 Metadata 不参与判定。
    public static bool IsBusinessError(Exception exception, string reason)
    {
        return exception is BusinessException business && business.Reason == reason;
    }
}

// BusinessException 表示服务端 Reply 包络携带的业务拒绝。
public sealed class BusinessException : AtlasException
{
    private static readonly IReadOnlyDictionary<string, string> EmptyMetadata =
        new ReadOnlyDictionary<string, string>(new Dictionary<string, string>());

    public BusinessException(
        int code,
        string reason,
        string message,
        IReadOnlyDictionary<string, string>? metadata)
        : base(message)
    {
        Code = code;
        Reason = reason ?? string.Empty;
        Metadata = CopyMetadata(metadata);
    }

    public int Code { get; }

    public string Reason { get; }

    public IReadOnlyDictionary<string, string> Metadata { get; }

    private static IReadOnlyDictionary<string, string> CopyMetadata(
        IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata == null || metadata.Count == 0)
        {
            return EmptyMetadata;
        }

        var copy = new Dictionary<string, string>();
        foreach (var pair in metadata)
        {
            copy[pair.Key] = pair.Value;
        }

        return new ReadOnlyDictionary<string, string>(copy);
    }
}

// NetworkException 表示连接断开、发送失败或重连队列溢出等网络故障。
public sealed class NetworkException : AtlasException
{
    public NetworkException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}

// TimeoutException 表示请求等待超时，调用方需谨慎重试。
public sealed class TimeoutException : AtlasException
{
    public TimeoutException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}
