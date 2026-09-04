using System;
using System.Text;
using Atlas.Errors;

namespace Atlas.Frame;

public static class Body
{
    // [opLen:u16 大端][operation][payload]；与服务端 BuildRawBody 一致。
    public static byte[] BuildRequestBody(string operation, byte[] payload)
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

        var body = new byte[2 + operationBytes.Length + payload.Length];
        body[0] = (byte)(operationBytes.Length >> 8);
        body[1] = (byte)operationBytes.Length;
        Buffer.BlockCopy(operationBytes, 0, body, 2, operationBytes.Length);
        Buffer.BlockCopy(payload, 0, body, 2 + operationBytes.Length, payload.Length);
        return body;
    }

    public static (string Operation, byte[] Payload) ParseRequestBody(byte[] body)
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
        var payload = new byte[body.Length - 2 - operationLength];
        Buffer.BlockCopy(body, 2 + operationLength, payload, 0, payload.Length);
        return (operation, payload);
    }
}
