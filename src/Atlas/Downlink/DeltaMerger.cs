// ver=3 增量合并（与 atlas-game-layout/lib/downlink/merge.go 语义逐条对齐）。
//
// 合并规则（设计 §3.2）：
//   - frame_id = delta.seq（隐含，不单独传）；
//   - battle_id 增量永不携带 → 沿用基线；
//   - server_time：changed 含 SERVER_TIME 时用绝对纳秒替换，否则沿用基线；
//   - inputs：先按 removed_player_ids 删除，再按 player_id 用 set_inputs 整条覆盖/追加，
//     未提及的保持基线；最后把全部 frame_id 改写为 seq；
//   - snapshot：changed 含 SNAPSHOT 时用本条替换（缺失 = 本帧无快照），否则沿用基线；
//   - state_hash：**总是**用当帧增量自带的值覆盖（空 = 本帧未算哈希，绝不回退基线）。
using System;
using System.Collections.Generic;
using System.Text;

namespace Atlas.Downlink;

/// <summary>FrameDeltaField 与 battle.v1.FrameDeltaField 的数值一一对应（协议常量，勿另立）。</summary>
public static class FrameDeltaField
{
    /// <summary>SERVER_TIME server_time_unix_nano 有效。</summary>
    public const int ServerTime = 1;

    /// <summary>INPUTS set_inputs / removed_player_ids 有效。</summary>
    public const int Inputs = 2;

    /// <summary>SNAPSHOT snapshot 有效（缺失 = 本帧无快照）。</summary>
    public const int Snapshot = 3;
}

/// <summary>DeltaFields 是 FrameBroadcastDelta 的解码结果（只保留合并需要的字段）。</summary>
public sealed class DeltaFields
{
    public ulong BaseSeq { get; set; }

    public ulong Seq { get; set; }

    public HashSet<int> Changed { get; } = new HashSet<int>();

    public long ServerTimeUnixNano { get; set; }

    public List<byte[]> SetInputs { get; } = new List<byte[]>();

    public List<string> RemovedPlayerIds { get; } = new List<string>();

    /// <summary>Snapshot snapshot 字段是否出现（出现但为空 = 本帧无快照，即「清空」）。</summary>
    public byte[]? Snapshot { get; set; }

    /// <summary>StateHash 当帧 state_hash（缺失 = 本帧未算哈希）。</summary>
    public byte[]? StateHash { get; set; }
}

/// <summary>DeltaMerger 是 ver=3 增量的纯函数合并器（无 IO、可属性测试）。</summary>
public static class DeltaMerger
{
    /// <summary>ParseDelta 解出 FrameBroadcastDelta（字段号见 api/battle/v1/battle_service.proto）。</summary>
    public static DeltaFields ParseDelta(byte[] payload)
    {
        var fields = Wire.Parse(payload);
        var delta = new DeltaFields
        {
            BaseSeq = Wire.FirstVarint(fields, 1),
            Seq = Wire.FirstVarint(fields, 2),
            ServerTimeUnixNano = unchecked((long)Wire.FirstVarint(fields, 4)),
            Snapshot = Wire.FirstBytes(fields, 7),
            StateHash = Wire.FirstBytes(fields, 8),
        };
        foreach (var f in fields)
        {
            if (f.No != 3)
            {
                continue;
            }
            if (f.Wire == 0)
            {
                delta.Changed.Add((int)f.Varint);
            }
            else if (f.Wire == 2)
            {
                var off = 0;
                while (off < f.Bytes.Length)
                {
                    delta.Changed.Add((int)Wire.ReadVarint(f.Bytes, ref off));
                }
            }
        }
        var utf8 = Encoding.UTF8;
        foreach (var b in Wire.AllBytes(fields, 5))
        {
            delta.SetInputs.Add(b);
        }
        foreach (var b in Wire.AllBytes(fields, 6))
        {
            delta.RemovedPlayerIds.Add(utf8.GetString(b));
        }
        return delta;
    }

    /// <summary>ReadFrameId 从全量 FrameBroadcast 字节读 frame.frame_id（失败返回 null）。</summary>
    public static ulong? ReadFrameId(byte[] payload)
    {
        try
        {
            var frame = Wire.FirstBytes(Wire.Parse(payload), 2);
            return frame == null ? (ulong?)null : Wire.FirstVarint(Wire.Parse(frame), 1);
        }
        catch (DownlinkException)
        {
            return null;
        }
    }

    /// <summary>Merge 把增量并进基线，返回新的全量 FrameBroadcast 字节（不改入参）。</summary>
    public static byte[] Merge(byte[] basePayload, DeltaFields delta)
    {
        var baseFields = Wire.Parse(basePayload);
        var battleId = Wire.FirstBytes(baseFields, 1);
        var frameRaw = Wire.FirstBytes(baseFields, 2);
        var frame = frameRaw == null ? new List<WireField>() : Wire.Parse(frameRaw);

        var baseInputs = Wire.AllBytes(frame, 2);
        var baseServerTime = Wire.FirstBytes(frame, 4);
        var baseSnapshot = Wire.FirstBytes(frame, 5);
        var stateHash = delta.StateHash ?? Array.Empty<byte>();

        var inputs = MergeInputs(baseInputs, delta);
        var serverTime = delta.Changed.Contains(FrameDeltaField.ServerTime)
            ? EncodeTimestamp(delta.ServerTimeUnixNano)
            : baseServerTime;
        var snapshot = delta.Changed.Contains(FrameDeltaField.Snapshot) ? delta.Snapshot : baseSnapshot;

        var parts = new List<byte[]>
        {
            Wire.VarintField(1, delta.Seq),
        };
        foreach (var input in inputs)
        {
            parts.Add(Wire.MessageField(2, input));
        }
        parts.Add(Wire.LengthDelimited(3, stateHash));
        if (serverTime != null)
        {
            parts.Add(Wire.MessageField(4, serverTime));
        }
        if (snapshot != null)
        {
            parts.Add(Wire.MessageField(5, snapshot));
        }
        var mergedFrame = Wire.Concat(parts.ToArray());
        return Wire.Concat(
            battleId == null ? Array.Empty<byte>() : Wire.LengthDelimited(1, battleId),
            Wire.MessageField(2, mergedFrame));
    }

    /// <summary>MergeInputs 按 player_id 稀疏替换，并把全部输入改写成本帧帧号。</summary>
    private static List<byte[]> MergeInputs(List<byte[]> baseInputs, DeltaFields delta)
    {
        var removed = new HashSet<string>(delta.RemovedPlayerIds);
        var pending = new Dictionary<string, byte[]>();
        foreach (var raw in delta.SetInputs)
        {
            pending[PlayerIdOf(raw)] = raw;
        }
        var outInputs = new List<byte[]>();
        foreach (var raw in baseInputs)
        {
            var id = PlayerIdOf(raw);
            if (removed.Contains(id))
            {
                continue;
            }
            if (pending.TryGetValue(id, out var repl))
            {
                pending.Remove(id);
                outInputs.Add(RewriteFrameId(repl, delta.Seq));
                continue;
            }
            outInputs.Add(RewriteFrameId(raw, delta.Seq));
        }
        // 与 Go 同口径：第二遍只按「是否仍在 pending」追加，且不删除（重复 id 原样保留）。
        foreach (var raw in delta.SetInputs)
        {
            if (pending.ContainsKey(PlayerIdOf(raw)))
            {
                outInputs.Add(RewriteFrameId(raw, delta.Seq));
            }
        }
        return outInputs;
    }

    /// <summary>PlayerIdOf 读输入的 player_id（字段 2）。</summary>
    private static string PlayerIdOf(byte[] raw)
    {
        var b = Wire.FirstBytes(Wire.Parse(raw), 2);
        return b == null ? string.Empty : Encoding.UTF8.GetString(b);
    }

    /// <summary>RewriteFrameId 复制一条输入并把 frame_id（字段 1）改写为 seq，其余字段原样保留。</summary>
    private static byte[] RewriteFrameId(byte[] raw, ulong seq)
    {
        var fields = Wire.Parse(raw);
        fields.RemoveAll(f => f.No == 1);
        fields.Sort((a, b) => a.No.CompareTo(b.No));
        var parts = new List<byte[]> { Wire.VarintField(1, seq) };
        foreach (var f in fields)
        {
            parts.Add(Wire.EncodeField(f));
        }
        return Wire.Concat(parts.ToArray());
    }

    /// <summary>EncodeTimestamp 把绝对纳秒编码成 google.protobuf.Timestamp（与 timestamppb.New 同口径）。</summary>
    private static byte[] EncodeTimestamp(long nanos)
    {
        const long Billion = 1_000_000_000L;
        var seconds = nanos / Billion;
        var rest = nanos % Billion;
        if (rest < 0)
        {
            seconds -= 1;
            rest += Billion;
        }
        return Wire.Concat(
            Wire.VarintField(1, unchecked((ulong)seconds)),
            Wire.VarintField(2, (ulong)rest));
    }
}
