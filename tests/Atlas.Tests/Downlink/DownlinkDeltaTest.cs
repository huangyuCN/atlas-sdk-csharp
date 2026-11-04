using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using Atlas.Downlink;
using Atlas.Frame;
using Xunit;

namespace Atlas.Tests.Downlink;

// ver=3 下行增量解码的验收用例：跨语言 golden（与 Go 生成器同一批字节）+ 失步语义。
//
// 夹具 Downlink/delta-golden.json 由模板仓 lib/downlink 的 TestGenerateGoldenVectors 生成
//（同一份文件原样复制到 TS SDK 的 test/fixtures）：base + 7 步增量，每步给出 delta 字节与
// 该 tick 的**独立全量编码**。三端解出来必须逐字节相等。
public sealed class DownlinkDeltaTest
{
    // Golden 是夹具形态。
    private sealed class Golden
    {
        [JsonPropertyName("source")]
        public string Source { get; set; } = string.Empty;

        [JsonPropertyName("base")]
        public string Base { get; set; } = string.Empty;

        [JsonPropertyName("steps")]
        public List<GoldenStep> Steps { get; set; } = new List<GoldenStep>();
    }

    // GoldenStep 是夹具里的一条增量。
    private sealed class GoldenStep
    {
        [JsonPropertyName("seq")]
        public ulong Seq { get; set; }

        [JsonPropertyName("baseSeq")]
        public ulong BaseSeq { get; set; }

        [JsonPropertyName("delta")]
        public string Delta { get; set; } = string.Empty;

        [JsonPropertyName("merged")]
        public string Merged { get; set; } = string.Empty;

        [JsonPropertyName("note")]
        public string Note { get; set; } = string.Empty;
    }

    private static readonly Golden Fixture = Load();

    // Load 读测试输出目录里的 golden 夹具（csproj 以 PreserveNewest 复制）。
    private static Golden Load()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Downlink", "delta-golden.json");
        return JsonSerializer.Deserialize<Golden>(File.ReadAllText(path))
            ?? throw new InvalidOperationException("golden 夹具为空");
    }

    // Bytes 十六进制转字节。
    private static byte[] Bytes(string hex) => Convert.FromHexString(hex);

    // Hex 字节转小写十六进制（对比口径）。
    private static string Hex(byte[] b) => Convert.ToHexString(b).ToLowerInvariant();

    // NewDecoder 构造带请求计数的解码器。
    private static (DownlinkDecoder Dec, List<ulong> Requests) NewDecoder()
    {
        var requests = new List<ulong>();
        return (new DownlinkDecoder(lastSeq => requests.Add(lastSeq)), requests);
    }

    [Fact]
    public void Fixture_IsComplete()
    {
        Assert.Equal(7, Fixture.Steps.Count);
        var want = new ulong[] { 2, 3, 4, 5, 6, 7, 8 };
        for (var i = 0; i < want.Length; i++)
        {
            Assert.Equal(want[i], Fixture.Steps[i].Seq);
            Assert.Equal(want[i] - 1, Fixture.Steps[i].BaseSeq);
        }
    }

    [Fact]
    public void Merge_IsByteIdenticalToFullEncoding()
    {
        var basePayload = Bytes(Fixture.Base);
        foreach (var step in Fixture.Steps)
        {
            var merged = DeltaMerger.Merge(basePayload, DeltaMerger.ParseDelta(Bytes(step.Delta)));
            Assert.Equal(step.Merged, Hex(merged));
            basePayload = merged;
        }
    }

    [Fact]
    public void Decoder_DeliversAllSteps_ByteIdentical()
    {
        var (dec, requests) = NewDecoder();
        var first = dec.Apply(Bytes(Fixture.Base), FrameGen.Version2);
        Assert.NotNull(first);
        Assert.Equal(FrameGen.Version2, first!.Value.Version);
        Assert.Equal(1UL, dec.LastSeq);
        foreach (var step in Fixture.Steps)
        {
            var got = dec.Apply(Bytes(step.Delta), FrameGen.Version3);
            Assert.NotNull(got);
            Assert.Equal(FrameGen.Version2, got!.Value.Version);
            Assert.Equal(step.Merged, Hex(got.Value.Payload));
            Assert.Equal(step.Seq, dec.LastSeq);
        }
        Assert.Empty(requests);
        Assert.Equal(0, dec.Drops);
    }

    [Fact]
    public void SeqJump_RequestsKeyframeOnce()
    {
        var (dec, requests) = NewDecoder();
        dec.Apply(Bytes(Fixture.Base), FrameGen.Version2);
        // 跳过第 2 帧，直接来第 3 帧（base_seq=2）
        Assert.Null(dec.Apply(Bytes(Fixture.Steps[1].Delta), FrameGen.Version3));
        Assert.Equal(new List<ulong> { 1UL }, requests);
        Assert.Equal(1, dec.Drops);
        // 关键帧在途：再来一条失步增量，不得重复请求
        Assert.Null(dec.Apply(Bytes(Fixture.Steps[2].Delta), FrameGen.Version3));
        Assert.Equal(new List<ulong> { 1UL }, requests);
        // 关键帧到达：pending 清空、基线 = 2
        var key = dec.Apply(Bytes(Fixture.Steps[0].Merged), FrameGen.Version2);
        Assert.NotNull(key);
        Assert.False(dec.Pending);
        Assert.Equal(2UL, dec.LastSeq);
        // 之后的连续增量正常交付
        var got = dec.Apply(Bytes(Fixture.Steps[1].Delta), FrameGen.Version3);
        Assert.NotNull(got);
        Assert.Equal(Fixture.Steps[1].Merged, Hex(got!.Value.Payload));
    }

    [Fact]
    public void BaseSeqMismatch_DropsWithoutBuffering()
    {
        var (dec, requests) = NewDecoder();
        dec.Apply(Bytes(Fixture.Base), FrameGen.Version2);
        // 第 4 帧（base_seq=3）落到基线 1 上：假设不符，必须丢弃
        Assert.Null(dec.Apply(Bytes(Fixture.Steps[2].Delta), FrameGen.Version3));
        Assert.Equal(new List<ulong> { 1UL }, requests);
        Assert.Equal(1, dec.Drops);
        Assert.Equal(1UL, dec.LastSeq);
        // 紧接着的合法增量只并进真实基线：坏增量的内容一条都不许混进来
        var got = dec.Apply(Bytes(Fixture.Steps[0].Delta), FrameGen.Version3);
        Assert.NotNull(got);
        Assert.Equal(Fixture.Steps[0].Merged, Hex(got!.Value.Payload));
        Assert.Equal(2UL, dec.LastSeq);
    }

    [Fact]
    public void LateOrDuplicate_IsSilentlyDropped()
    {
        var (dec, requests) = NewDecoder();
        dec.Apply(Bytes(Fixture.Base), FrameGen.Version2);
        dec.Apply(Bytes(Fixture.Steps[0].Delta), FrameGen.Version3);
        Assert.Null(dec.Apply(Bytes(Fixture.Steps[0].Delta), FrameGen.Version3));
        Assert.NotNull(dec.Apply(Bytes(Fixture.Base), FrameGen.Version2));
        Assert.Empty(requests);
        Assert.Equal(0, dec.Drops); // 迟到不算失步（Go 同口径）
    }

    [Fact]
    public void NoBase_RequestsOnceWithoutMerging()
    {
        var (dec, requests) = NewDecoder();
        Assert.Null(dec.Apply(Bytes(Fixture.Steps[0].Delta), FrameGen.Version3));
        Assert.Equal(new List<ulong> { 0UL }, requests);
        Assert.Equal(0UL, dec.LastSeq);
    }

    [Fact]
    public void GarbageBytes_ThrowDownlinkException()
    {
        var (dec, _) = NewDecoder();
        dec.Apply(Bytes(Fixture.Base), FrameGen.Version2);
        Assert.Throws<DownlinkException>(
            () => dec.Apply(new byte[] { 0xff, 0xff, 0xff }, FrameGen.Version3));
        Assert.Throws<DownlinkException>(
            () => dec.Apply(new byte[] { 0x3a, 0x05, 0x01 }, FrameGen.Version3));
    }

    [Fact]
    public void ReadFrameId_ReadsFullFrameOnly()
    {
        Assert.Equal(1UL, DeltaMerger.ReadFrameId(Bytes(Fixture.Base)));
        Assert.Null(DeltaMerger.ReadFrameId(Bytes(Fixture.Steps[0].Delta)));
    }
}
