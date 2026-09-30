using System;
using Atlas.Errors;

namespace Atlas.Transport;

// EdgeFlow 是接入层数据报面的 flow-id 线格式（与框架 contrib/edge 逐字节一致）：
// 首包 hello 换到 8 字节大端流号后，**双向**数据报都是「flow-id(8) || 载荷」。
// 纯函数实现（无 socket）：UDP/KCP 两个面共用同一份编码/剥离逻辑。
public static class EdgeFlow
{
    // FlowIdLen 是 flow-id 的线格式长度（8 字节大端）。
    public const int FlowIdLen = 8;

    // MismatchMessage 是 flow-id 前缀失配/长度不足的稳定报错前缀（Atlas.Battle.DirectErrors 引用）。
    public const string MismatchMessage = "数据报 flow-id 前缀失配";

    // EncodeFlowId 把流号编码为 8 字节大端。
    public static byte[] EncodeFlowId(ulong flowId)
    {
        var encoded = new byte[FlowIdLen];
        for (var i = 0; i < FlowIdLen; i++)
        {
            encoded[i] = (byte)(flowId >> (8 * (FlowIdLen - 1 - i)));
        }
        return encoded;
    }

    // DecodeFlowId 读数据报前 8 字节为流号；长度不足即协议错误。
    public static ulong DecodeFlowId(byte[]? datagram)
    {
        if (datagram == null || datagram.Length < FlowIdLen)
        {
            throw new ProtocolException($"{MismatchMessage}: 数据报长度不足 {FlowIdLen} 字节");
        }
        ulong flowId = 0;
        for (var i = 0; i < FlowIdLen; i++)
        {
            flowId = (flowId << 8) | datagram[i];
        }
        return flowId;
    }

    // WrapFlowId 给载荷加 flow-id 前缀（发出的方向）。
    public static byte[] WrapFlowId(ulong flowId, byte[]? payload)
    {
        var body = payload ?? Array.Empty<byte>();
        var wrapped = new byte[FlowIdLen + body.Length];
        var prefix = EncodeFlowId(flowId);
        Buffer.BlockCopy(prefix, 0, wrapped, 0, FlowIdLen);
        if (body.Length > 0)
        {
            Buffer.BlockCopy(body, 0, wrapped, FlowIdLen, body.Length);
        }
        return wrapped;
    }

    // StripFlowId 剥掉 flow-id 前缀并**拷贝**出载荷（读缓冲逐包复用，返回值不得指向入参）；
    // 前缀不符或长度不足即协议错误（调用方按「失配丢弃并要求重新 hello」处置）。
    public static byte[] StripFlowId(byte[]? datagram, ulong flowId)
    {
        if (datagram == null || datagram.Length < FlowIdLen)
        {
            throw new ProtocolException($"{MismatchMessage}: 数据报长度不足 {FlowIdLen} 字节");
        }
        if (DecodeFlowId(datagram) != flowId)
        {
            throw new ProtocolException($"{MismatchMessage}: 前缀与本流不符");
        }
        var payload = new byte[datagram.Length - FlowIdLen];
        if (payload.Length > 0)
        {
            Buffer.BlockCopy(datagram, FlowIdLen, payload, 0, payload.Length);
        }
        return payload;
    }
}
