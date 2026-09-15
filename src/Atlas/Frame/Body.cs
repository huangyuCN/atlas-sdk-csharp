using System;
using System.Text;
using Atlas.Errors;

namespace Atlas.Frame;

public static class Body
{
    // [opLen:u16 大端][operation][payload]；与服务端 BuildRawBody 一致。
    public static byte[] BuildRequestBody(string operation, byte[]? payload)
    {
        return BuildRequestBodyCore(operation, null, payload);
    }

    // BuildRequestBodyWithSession 封装携带会话槽的请求 body：
    // [opLen][operation][sessionLen][session][payload]，配合帧头 FlagSession 使用
    //（对齐 Go frame.BuildRequestBodyWithSession）。无连接传输（UDP/KCP）的请求帧
    // 调用；session 为空时与无会话布局等价（匿名请求）。
    public static byte[] BuildRequestBodyWithSession(string operation, string? session, byte[]? payload)
    {
        return BuildRequestBodyCore(operation, session, payload);
    }

    // BuildRequestBodyCore 是 body 封装的唯一实现源；session 非空时追加会话槽。
    private static byte[] BuildRequestBodyCore(string operation, string? session, byte[]? payload)
    {
        if (string.IsNullOrEmpty(operation))
        {
            throw new ArgumentException("operation 不能为空", nameof(operation));
        }

        var operationBytes = Encoding.UTF8.GetBytes(operation);
        if (operationBytes.Length > FrameConst.MaxOperationLen)
        {
            throw new ProtocolException($"operation 长度 {operationBytes.Length} 超上限 {FrameConst.MaxOperationLen}");
        }

        var sessionBytes = string.IsNullOrEmpty(session) ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(session);
        if (sessionBytes.Length > FrameConst.MaxSessionLen)
        {
            throw new ProtocolException($"session 长度 {sessionBytes.Length} 超上限 {FrameConst.MaxSessionLen}");
        }

        var payloadLength = payload?.Length ?? 0;
        var body = new byte[2 + operationBytes.Length + payloadLength + (sessionBytes.Length > 0 ? 2 + sessionBytes.Length : 0)];
        body[0] = (byte)(operationBytes.Length >> 8);
        body[1] = (byte)operationBytes.Length;
        Buffer.BlockCopy(operationBytes, 0, body, 2, operationBytes.Length);
        var offset = 2 + operationBytes.Length;
        if (sessionBytes.Length > 0)
        {
            body[offset] = (byte)(sessionBytes.Length >> 8);
            body[offset + 1] = (byte)sessionBytes.Length;
            Buffer.BlockCopy(sessionBytes, 0, body, offset + 2, sessionBytes.Length);
            offset += 2 + sessionBytes.Length;
        }
        if (payloadLength > 0)
        {
            Buffer.BlockCopy(payload!, 0, body, offset, payloadLength);
        }

        return body;
    }

    // ParseRequestBody 解析帧 body，返回 operation 与 payload（不解析会话槽，
    // 对齐 Go frame.ParseRequestBody）。服务端对超长 operation 回 ProtocolError
    // 不断连；客户端侧同样不视作断连条件。
    public static (string Operation, byte[] Payload) ParseRequestBody(byte[] body)
    {
        var (operation, _, payload) = ParseRequestBodyWithSession(body, 0);
        return (operation, payload);
    }

    // ParseRequestBodyWithSession 解析帧 body，返回 operation、会话槽与 payload
    //（对齐 Go frame.ParseRequestBodyWithSession）。flags 未置位（或不携带会话槽）
    // 时 session 为空串；置位则校验槽完整性（缺长度/截断即协议错误）。
    public static (string Operation, string Session, byte[] Payload) ParseRequestBodyWithSession(
        byte[] body, byte flags)
    {
        if (body == null || body.Length < 2)
        {
            throw new ProtocolException("body 过短，缺少 opLen");
        }

        var operationLength = (body[0] << 8) | body[1];
        if (operationLength > FrameConst.MaxOperationLen)
        {
            throw new ProtocolException($"operation 长度 {operationLength} 超上限 {FrameConst.MaxOperationLen}");
        }

        if (body.Length < 2 + operationLength)
        {
            throw new ProtocolException("operation 截断");
        }

        var operation = Encoding.UTF8.GetString(body, 2, operationLength);
        var restOffset = 2 + operationLength;
        if ((flags & FrameConst.FlagSession) == 0)
        {
            return (operation, "", ParseRest(body, restOffset));
        }
        if (body.Length < restOffset + 2)
        {
            throw new ProtocolException("会话槽缺少长度");
        }
        var sessionLength = (body[restOffset] << 8) | body[restOffset + 1];
        if (body.Length < restOffset + 2 + sessionLength)
        {
            throw new ProtocolException("会话槽截断");
        }
        var session = Encoding.UTF8.GetString(body, restOffset + 2, sessionLength);
        return (operation, session, ParseRest(body, restOffset + 2 + sessionLength));
    }

    // ParseRest 从指定偏移复制余下字节为 payload。
    private static byte[] ParseRest(byte[] body, int offset)
    {
        var payload = new byte[body.Length - offset];
        Buffer.BlockCopy(body, offset, payload, 0, payload.Length);
        return payload;
    }
}
