// ver=3 下行解码状态机（每战斗会话一份），与 atlas-game-layout/lib/downlink/session.go 同构：
//
//	收到 ver=1/2 全量: 基线重置到该帧，交付上层，清 pending
//	收到 ver=3 增量:   序号不连续或 base_seq 与自己不符 ⇒ 丢弃 + 请求关键帧（去重）
//	                   序号 <= 基线                   ⇒ 迟到/重复，静默丢弃
//	                   其余                           ⇒ 合并后按 ver=2 全量交付
//
// **不缓冲后续增量等关键帧**：那会把「一个可恢复的失步」变成「一段静默花屏」。
using System;
using Atlas.Frame;

namespace Atlas.Downlink;

/// <summary>KeyframeRequester 发送关键帧请求的端口（由会话层注入：发 RequestFrameKeyframe(lastSeq)）。</summary>
public delegate void KeyframeRequester(ulong lastSeq);

/// <summary>DecodedFrame 是一次交付：Payload 恒为**全量** FrameBroadcast 字节，Version 恒为 2。</summary>
public readonly struct DecodedFrame
{
    /// <summary>以载荷与版本构造。</summary>
    public DecodedFrame(byte[] payload, byte version)
    {
        Payload = payload;
        Version = version;
    }

    /// <summary>Payload 全量 FrameBroadcast 字节。</summary>
    public byte[] Payload { get; }

    /// <summary>Version 交付版本（合并后恒为 2）。</summary>
    public byte Version { get; }
}

/// <summary>DownlinkDecoder 是每战斗会话一份的解码状态机（设计 §4.3）。</summary>
public sealed class DownlinkDecoder
{
    private readonly KeyframeRequester? _request;
    private byte[]? _base;
    private ulong _last;
    private bool _waiting;
    private int _requests;
    private int _drops;

    /// <summary>构造解码器；request 为 null 时只丢弃失步帧（纯解码形态）。</summary>
    public DownlinkDecoder(KeyframeRequester? request = null)
    {
        _request = request;
    }

    /// <summary>LastSeq 当前基线帧号（= 已交付的最后一帧）。</summary>
    public ulong LastSeq => _last;

    /// <summary>Pending 是否已请求关键帧但还没拿到。</summary>
    public bool Pending => _waiting;

    /// <summary>Requests 累计发出的关键帧请求次数（去重后）。</summary>
    public int Requests => _requests;

    /// <summary>Drops 累计丢弃的增量条数。</summary>
    public int Drops => _drops;

    /// <summary>Apply 处理一帧载荷：ver=1/2 全量、ver=3 增量。
    /// 返回 null = 本帧不交付（失步/迟到/无基线）；载荷本身解不开抛 DownlinkException。</summary>
    public DecodedFrame? Apply(byte[] payload, byte version)
    {
        if (version == FrameGen.Version3)
        {
            return ApplyDelta(payload);
        }
        if (version != FrameGen.Version && version != FrameGen.Version2)
        {
            throw new DownlinkException($"downlink: 未知的载荷版本 {version}");
        }
        if (version == FrameGen.Version)
        {
            // ver=1 是 protojson，SDK 不解析游戏域 JSON：不能建立二进制基线，宁可请求关键帧，
            // 也不猜「基线帧号」。（协商到 ver=1 时服务端本就永不下发 ver=3。）
            _base = null;
            _last = 0;
            _waiting = false;
            return new DecodedFrame(payload, version);
        }
        _base = payload;
        var frameId = DeltaMerger.ReadFrameId(payload);
        if (frameId.HasValue)
        {
            _last = frameId.Value;
        }
        _waiting = false;
        return new DecodedFrame(payload, version);
    }

    /// <summary>ApplyDelta 严格校验基线，不符即丢弃并请求关键帧（不猜、不缓冲）。</summary>
    private DecodedFrame? ApplyDelta(byte[] payload)
    {
        var delta = DeltaMerger.ParseDelta(payload);
        if (_base == null)
        {
            RequestKeyframe();
            return null; // 没有基线：无从合并，请求关键帧
        }
        if (delta.Seq <= _last)
        {
            return null; // 迟到/重复：静默丢弃（不发请求）
        }
        if (delta.Seq != _last + 1 || delta.BaseSeq != _last)
        {
            _drops += 1;
            RequestKeyframe();
            return null;
        }
        var merged = DeltaMerger.Merge(_base, delta);
        _base = merged;
        _last = delta.Seq;
        _waiting = false;
        return new DecodedFrame(merged, FrameGen.Version2);
    }

    /// <summary>RequestKeyframe 发一次关键帧请求（同一 pending 窗口内只发一次）。</summary>
    private void RequestKeyframe()
    {
        if (_waiting)
        {
            return;
        }
        _waiting = true;
        _requests += 1;
        _request?.Invoke(_last);
    }
}
