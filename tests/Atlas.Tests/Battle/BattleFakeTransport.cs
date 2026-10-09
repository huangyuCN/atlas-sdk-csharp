using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Atlas.Frame;
using Atlas.Transport;

namespace Atlas.Tests.Battle;

// BattleFakeTransport 是直连战斗会话的**假传输**：不碰网络，测试可精确断言「写了几帧」
// 并按序号注入成功/业务拒绝（BATTLE_ENDED）回执，也能注入服务端推送——
// 「终态后零写入」「心跳停表」「结束通知幂等」的线级证据都取自它。
internal sealed class BattleFakeTransport : ITransport
{
    private readonly object _gate = new();
    private readonly Channel<(Header Header, byte[] Body)> _incoming =
        Channel.CreateUnbounded<(Header, byte[])>();
    private readonly List<Header> _written = new();
    private TaskCompletionSource<bool> _writeSignal = NewSignal();

    // WrittenCount 是累计写入帧数（终态断言：结束后不再增长）。
    public int WrittenCount
    {
        get
        {
            lock (_gate)
            {
                return _written.Count;
            }
        }
    }

    // WrittenHeaders 是已写帧头快照（取序号注入对应回执）。
    public Header[] WrittenHeaders
    {
        get
        {
            lock (_gate)
            {
                return _written.ToArray();
            }
        }
    }

    // BeforeWrite 是写帧前的测试钩子（默认 null）：挂起即把「本次写已进写锁、尚未上线」
    // 的窗口固定住，供「组帧与写线之间置终态」的竞态用例使用。
    internal Func<Task>? BeforeWrite { get; set; }

    // WaitForWritesAsync 等到写入数达到 count（超时即失败，不接受固定 sleep 赌时序）。
    public async Task WaitForWritesAsync(int count)
    {
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                if (_written.Count >= count)
                {
                    return;
                }
                changed = _writeSignal.Task;
            }
            await changed.WaitAsync(TimeSpan.FromSeconds(3));
        }
    }

    // QueueSuccess 注入一条成功回执（envelope 口径与 EdgeReply.Success 一致）。
    public void QueueSuccess(uint sequence, byte[] payload)
    {
        QueueReply(sequence, FrameGen.Version, EdgeReply.Success(payload));
    }

    // QueueBusinessError 注入一条业务拒绝回执（BATTLE_ENDED 等稳定 reason）。
    public void QueueBusinessError(uint sequence, int code, string reason, string message)
    {
        QueueReply(sequence, FrameGen.Version, EdgeReply.BusinessError(code, reason, message));
    }

    // QueueNotify 注入一条服务端推送（帧广播 / 战斗结束通知）。
    public void QueueNotify(string op, byte[] payload, byte version = FrameGen.Version)
    {
        _incoming.Writer.TryWrite((new Header
        {
            Type = MsgType.Notify,
            Version = version,
            Seq = 1,
        }, Body.BuildRequestBody(op, payload)));
    }

    public ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(
        int maxBodySize, CancellationToken cancellationToken)
    {
        return _incoming.Reader.ReadAsync(cancellationToken);
    }

    public async Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken cancellationToken)
    {
        if (BeforeWrite != null)
        {
            await BeforeWrite().ConfigureAwait(false);
        }
        lock (_gate)
        {
            _written.Add(header);
            _writeSignal.TrySetResult(true);
            _writeSignal = NewSignal();
        }
    }

    public Task CloseAsync()
    {
        _incoming.Writer.TryComplete();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _incoming.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }

    private void QueueReply(uint sequence, byte version, byte[] envelope)
    {
        _incoming.Writer.TryWrite((new Header
        {
            Type = MsgType.Response,
            Version = version,
            Seq = sequence,
        }, envelope));
    }

    private static TaskCompletionSource<bool> NewSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }
}
