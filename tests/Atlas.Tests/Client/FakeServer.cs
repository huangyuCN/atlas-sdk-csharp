using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Frame;
using Atlas.Transport;

namespace Atlas.Tests.Client;

internal sealed class FakeServer : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly TaskCompletionSource<bool> _holdSeen = NewSignal();
    private readonly TaskCompletionSource<bool> _releaseHold = NewSignal();
    private readonly ConcurrentQueue<uint> _sequences = new();
    private readonly ConcurrentQueue<string> _operations = new();
    private readonly object _pushGate = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1); // 连接写串行化（评审 M2-4 P2：
    // PushNotifyAsync 与 ServeAsync 可并发写同 stream——.NET NetworkStream 并发写无
    // 原子性保证，共用写锁消除字节交错）。
    private Stream? _stream;
    private Task? _serveTask;
    private int _accepted;

    // PingBusinessError 为 true 时对传输心跳（Ping）回业务拒绝包络：
    // 模拟网关 UDP 通道未注册内置 Ping 的服务端形态——往返完成但业务拒绝
    //（验证「心跳业务拒绝不计死链」用，对齐 Go fakeServer.hbBusinessErr）。
    public bool PingBusinessError { get; set; }

    // BusinessErrorOps 是收到即回业务拒绝包络的 operation 集合（如会话心跳的
    // 业务 Heartbeat 模拟会话过期拒绝）：命中时回业务错误 Status——验证会话心跳
    // 业务错误触发重登钩子用（对齐 Go echoService 的 boom op）。
    public System.Collections.Generic.HashSet<string> BusinessErrorOps { get; } = new();

    // DropPings 为 true 时静默丢弃传输心跳（不响应也不断开）：客户端 invoke
    // 等响应超时（无往返）——模拟网络类失败计死链（验证死链触发重连用）。
    public bool DropPings { get; set; }

    public FakeServer()
    {
        _listener.Start();
        _serveTask = AcceptLoopAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public uint[] Sequences => _sequences.ToArray();

    // OperationNames 返回服务端收到的全部 operation（按到达顺序）。
    public string[] OperationNames => _operations.ToArray();

    // AcceptedConnections 返回服务端接受的连接总数（重连观测用）。
    public int AcceptedConnections
    {
        get
        {
            lock (_pushGate)
            {
                return _accepted;
            }
        }
    }

    public int PingCount
    {
        get
        {
            var count = 0;
            foreach (var op in _operations)
            {
                if (op == Channel.HeartbeatOperation)
                {
                    count++;
                }
            }
            return count;
        }
    }

    public async Task PushNotifyAsync(string operation, byte[] payload)
    {
        var stream = await WaitForStreamAsync();
        var body = Body.BuildRequestBody(operation, payload);
        var header = new Header { Type = MsgType.Notify, Version = FrameConst.Version, Seq = 1 };
        await _writeLock.WaitAsync(_stop.Token);
        try
        {
            await FrameIO.WriteFrameAsync(stream, header, body, FrameConst.MaxBodySize, _stop.Token);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    // WaitForStreamAsync 等待服务端 accept 并保存连接流（PushNotify 前置）。
    private async Task<Stream> WaitForStreamAsync()
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            Stream? stream;
            lock (_pushGate)
            {
                stream = _stream;
            }
            if (stream != null)
            {
                return stream;
            }
            await Task.Delay(5);
        }
        throw new InvalidOperationException("服务端尚无活动连接");
    }

    public Task WaitForHoldAsync() => _holdSeen.Task;

    public void ReleaseHold() => _releaseHold.TrySetResult(true);

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        ReleaseHold();
        if (_serveTask != null)
        {
            await IgnoreCancellationAsync(_serveTask);
        }
        _stop.Dispose();
    }

    // AcceptLoopAsync 循环 accept：每次新连接独立 Serve（重连客户端重拨后仍能接入）。
    private async Task AcceptLoopAsync()
    {
        while (!_stop.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_stop.Token);
            }
            catch (OperationCanceledException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            catch (ObjectDisposedException) when (_stop.IsCancellationRequested)
            {
                return;
            }
            lock (_pushGate)
            {
                _accepted++;
            }
            _ = ServeOneAsync(client);
        }
    }

    private async Task ServeOneAsync(TcpClient client)
    {
        try
        {
            using (client)
            {
                await ServeAsync(client.GetStream());
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
        }
        catch (ObjectDisposedException) when (_stop.IsCancellationRequested)
        {
        }
        catch (IOException)
        {
        }
    }


    private async Task ServeAsync(Stream stream)
    {
        lock (_pushGate)
        {
            _stream = stream;
        }
        while (!_stop.IsCancellationRequested)
        {
            var (header, body) = await FrameIO.ReadFrameAsync(stream, FrameConst.MaxBodySize, _stop.Token);
            var (operation, payload) = Body.ParseRequestBody(body);
            _sequences.Enqueue(header.Seq);
            _operations.Enqueue(operation);
            if (operation == "hold")
            {
                _holdSeen.TrySetResult(true);
                await _releaseHold.Task;
            }
            if (operation == "kick")
            {
                return;
            }
            if (operation == Channel.HeartbeatOperation && PingBusinessError)
            {
                await WriteBusinessErrorReplyAsync(stream, header.Seq, header.Version, _stop.Token);
                continue;
            }
            if (BusinessErrorOps.Contains(operation))
            {
                await WriteBusinessErrorReplyAsync(stream, header.Seq, header.Version, _stop.Token);
                continue;
            }
            if (operation == Channel.HeartbeatOperation && DropPings)
            {
                continue; // 静默丢弃 Ping：客户端 invoke 超时（网络类失败）。
            }

            var version = operation == "bad-version" ? FrameConst.Version2 : header.Version;
            await WriteReplyAsync(stream, header.Seq, version, payload, _stop.Token);
        }
    }

    private async Task WriteReplyAsync(Stream stream, uint sequence, byte version, byte[] payload, CancellationToken token)
    {
        var reply = new byte[5 + payload.Length];
        reply[0] = 0;
        WriteUInt32(reply, 1, (uint)payload.Length);
        Array.Copy(payload, 0, reply, 5, payload.Length);
        var header = new Header { Type = MsgType.Response, Version = version, Seq = sequence };
        await _writeLock.WaitAsync(token);
        try
        {
            await FrameIO.WriteFrameAsync(stream, header, reply, FrameConst.MaxBodySize, token);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    // WriteBusinessErrorReplyAsync 回业务拒绝包络：
    // [1][statusLen:u32][Status wire][dataLen:u32][data]
    // Status 含 code=1（int32 varint）与 message=3（string），reason 空——
    // 对齐 Go testStatus(500, "", "engine: not registered") 形态。
    private async Task WriteBusinessErrorReplyAsync(
        Stream stream, uint sequence, byte version, CancellationToken token)
    {
        // Status wire：field1=code(500) varint + field3=message(string)。
        // 0x08 <varint 500=0xF4 0x03> 0x1A <len> "engine: not registered"
        var message = System.Text.Encoding.UTF8.GetBytes("engine: not registered");
        var status = new List<byte>
        {
            0x08, 0xF4, 0x03,
            0x1A, (byte)message.Length,
        };
        status.AddRange(message);
        var reply = new byte[1 + 4 + status.Count + 4];
        reply[0] = 1;
        WriteUInt32(reply, 1, (uint)status.Count);
        status.CopyTo(reply, 5);
        WriteUInt32(reply, 5 + status.Count, 0); // dataLen=0
        var header = new Header { Type = MsgType.Response, Version = version, Seq = sequence };
        await _writeLock.WaitAsync(token);
        try
        {
            await FrameIO.WriteFrameAsync(stream, header, reply, FrameConst.MaxBodySize, token);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private static TaskCompletionSource<bool> NewSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private static async Task IgnoreCancellationAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
        }
        catch (EndOfStreamException)
        {
        }
        catch (IOException)
        {
        }
    }

    private static void WriteUInt32(byte[] bytes, int offset, uint value)
    {
        bytes[offset] = (byte)(value >> 24);
        bytes[offset + 1] = (byte)(value >> 16);
        bytes[offset + 2] = (byte)(value >> 8);
        bytes[offset + 3] = (byte)value;
    }
}

internal sealed class TcpTestTransport : ITransport
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;

    private TcpTestTransport(TcpClient client)
    {
        _client = client;
        _stream = client.GetStream();
    }

    public static async Task<ITransport> ConnectAsync(int port, CancellationToken token)
    {
        var client = new TcpClient();
        try
        {
            await client.ConnectAsync(IPAddress.Loopback, port, token);
            return new TcpTestTransport(client);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    public ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(int maxBodySize, CancellationToken token)
    {
        return FrameIO.ReadFrameAsync(_stream, maxBodySize, token);
    }

    public Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken token)
    {
        return FrameIO.WriteFrameAsync(_stream, header, body, maxBodySize, token).AsTask();
    }

    public Task CloseAsync()
    {
        _client.Close();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return ValueTask.CompletedTask;
    }
}
