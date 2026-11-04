using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Atlas.Battle;
using Atlas.Downlink;
using Atlas.Frame;
using Atlas.Transport;
using Xunit;

namespace Atlas.Tests.Battle;

// 会话接入：ws 面收到 ver=3 时先按 base_seq 合并，再以**全量 + ver=2** 走统一出口；
// 序号跳变时丢弃并自动发一次关键帧请求（线级证据：假传输多写了一帧）。
// golden 夹具与 TS/C#/Go 三端同一份（模板仓 lib/downlink 生成）。
public sealed class BattleSessionDownlinkTest
{
    private static readonly byte[] Ticket = { 0xfb, 0xff, 0x00, 0x01, 0xfe, 0x10, 0x20 };

    private static DirectPlan Plan(int port)
    {
        var encoded = Convert.ToBase64String(Ticket);
        return DirectPlan.FromNotify("{\"match_id\":\"m-1\",\"battle_id\":\"b-1\",\"battle_ticket\":\""
            + encoded + "\",\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_UDP\",\"address\":\"127.0.0.1:"
            + port + "\"}]}");
    }

    private static BattleSessionOptions Options()
    {
        return new BattleSessionOptions
        {
            PreferredTransport = EdgeTransport.Udp,
            HelloTimeoutMs = 3000,
            InvokeTimeoutMs = 3000,
            BackoffBaseMs = 50,
            BackoffMaxMs = 200,
            HeartbeatInterval = TimeSpan.Zero,
            FrameNumberOf = memory => DeltaMerger.ReadFrameId(memory.ToArray()),
        };
    }

    // WaitUntilAsync 轮询等待条件成立（推送分发是异步的，不接受固定 sleep 赌时序）。
    private static async Task<bool> WaitUntilAsync(Func<bool> condition, int timeoutMs = 3000)
    {
        var watch = Stopwatch.StartNew();
        while (watch.ElapsedMilliseconds < timeoutMs)
        {
            if (condition())
            {
                return true;
            }
            await Task.Delay(10);
        }
        return condition();
    }

    // Fixture 是测试输出目录里的 golden（与 DownlinkDeltaTest 同一份文件）。
    private static (byte[] Base, List<(byte[] Delta, byte[] Merged)> Steps) LoadFixture()
    {
        using var doc = JsonDocument.Parse(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Downlink", "delta-golden.json")));
        var steps = new List<(byte[], byte[])>();
        foreach (var s in doc.RootElement.GetProperty("steps").EnumerateArray())
        {
            steps.Add((
                Convert.FromHexString(s.GetProperty("delta").GetString()!),
                Convert.FromHexString(s.GetProperty("merged").GetString()!)));
        }
        return (Convert.FromHexString(doc.RootElement.GetProperty("base").GetString()!), steps);
    }

    [Fact]
    public async Task Ver3Broadcast_IsMergedAndDeliveredAsVer2_SeqJumpRequestsKeyframe()
    {
        var (basePayload, steps) = LoadFixture();
        var transport = new BattleFakeTransport();
        var session = BattleSession.Create(Plan(7102), Options());
        session.DialOverride = _ => Task.FromResult<ITransport>(transport);
        var pushes = new List<BattlePush>();
        session.Push += push =>
        {
            lock (pushes)
            {
                pushes.Add(push);
            }
        };
        await session.ConnectAsync();

        transport.QueueNotify(TestOps.FrameBroadcast, basePayload, FrameGen.Version2);
        Assert.True(await WaitUntilAsync(() => Count(pushes) == 1), "关键帧应交付");
        Assert.Equal(FrameGen.Version2, pushes[0].Version);
        Assert.Equal(basePayload, pushes[0].Payload);
        Assert.Equal(1UL, session.LastSeenFrame);

        transport.QueueNotify(TestOps.FrameBroadcast, steps[0].Delta, FrameGen.Version3);
        Assert.True(await WaitUntilAsync(() => Count(pushes) == 2), "连续增量应交付合并后的全量");
        Assert.Equal(FrameGen.Version2, pushes[1].Version);
        Assert.Equal(steps[0].Merged, pushes[1].Payload);
        Assert.Equal(2UL, session.LastSeenFrame);

        // 丢掉第 3 帧、直接来第 4 帧：丢弃 + 自动发一次关键帧请求（线级证据 = 多写一帧）。
        var writtenBefore = transport.WrittenCount;
        transport.QueueNotify(TestOps.FrameBroadcast, steps[2].Delta, FrameGen.Version3);
        Assert.True(
            await WaitUntilAsync(() => transport.WrittenCount > writtenBefore),
            "失步必须触发一次关键帧请求");
        await Task.Delay(30);
        Assert.Equal(2, Count(pushes));
        Assert.Equal(2UL, session.LastSeenFrame);

        // 关键帧（第 3 帧全量）到达：恢复交付，补帧基准前移。
        transport.QueueNotify(TestOps.FrameBroadcast, steps[1].Merged, FrameGen.Version2);
        Assert.True(await WaitUntilAsync(() => Count(pushes) == 3), "关键帧应恢复交付");
        Assert.Equal(FrameGen.Version2, pushes[2].Version);
        Assert.Equal(steps[1].Merged, pushes[2].Payload);
        Assert.Equal(3UL, session.LastSeenFrame);
        await session.CloseAsync();
    }

    private static int Count(List<BattlePush> pushes)
    {
        lock (pushes)
        {
            return pushes.Count;
        }
    }
}
