using System;

namespace Atlas.Errors;

// 帧协议错误（规范 §7：ProtocolError 不可重试，连接已断）。
public sealed class ProtocolException : Exception
{
    public ProtocolException(string message) : base(message)
    {
    }

    public ProtocolException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
