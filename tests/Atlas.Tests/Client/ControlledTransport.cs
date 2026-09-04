using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Atlas.Frame;
using Atlas.Transport;

namespace Atlas.Tests.Client;

// ControlledTransport 是可控帧传输：测试可精确安排响应次序与读写时机。
internal sealed class ControlledTransport : ITransport
{
    private readonly object _gate = new();
    private readonly Channel<(Header Header, byte[] Body)> _responses = Channel.CreateUnbounded<(Header, byte[])>();
    private readonly List<Header> _writtenHeaders = new();
    private TaskCompletionSource<bool> _writeChanged = NewSignal();
    private readonly TaskCompletionSource<bool> _readStarted = NewSignal();

    public int LastReadMaxBodySize { get; private set; }

    public int LastWriteMaxBodySize { get; private set; }

    public IReadOnlyList<Header> WrittenHeaders
    {
        get
        {
            lock (_gate)
            {
                return _writtenHeaders.ToArray();
            }
        }
    }

    public Task WaitForReadAsync() => _readStarted.Task;

    public async Task WaitForWritesAsync(int count)
    {
        while (true)
        {
            Task changed;
            lock (_gate)
            {
                if (_writtenHeaders.Count >= count)
                {
                    return;
                }
                changed = _writeChanged.Task;
            }
            await changed;
        }
    }

    public ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(int maxBodySize, CancellationToken cancellationToken)
    {
        LastReadMaxBodySize = maxBodySize;
        _readStarted.TrySetResult(true);
        return _responses.Reader.ReadAsync(cancellationToken);
    }

    public Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            LastWriteMaxBodySize = maxBodySize;
            _writtenHeaders.Add(header);
            _writeChanged.TrySetResult(true);
            _writeChanged = NewSignal();
        }
        return Task.CompletedTask;
    }

    public void QueueReply(uint sequence, byte version, byte[] payload)
    {
        var reply = new byte[5 + payload.Length];
        reply[0] = 0;
        WriteUInt32(reply, 1, (uint)payload.Length);
        Array.Copy(payload, 0, reply, 5, payload.Length);
        _responses.Writer.TryWrite((new Header
        {
            Type = MsgType.Response,
            Version = version,
            Seq = sequence,
        }, reply));
    }

    public Task CloseAsync()
    {
        _responses.Writer.TryComplete();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _responses.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }

    private static TaskCompletionSource<bool> NewSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static void WriteUInt32(byte[] bytes, int offset, uint value)
    {
        bytes[offset] = (byte)(value >> 24);
        bytes[offset + 1] = (byte)(value >> 16);
        bytes[offset + 2] = (byte)(value >> 8);
        bytes[offset + 3] = (byte)value;
    }
}
