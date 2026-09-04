using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Serialization;
using Gateway.V1;
using Xunit;

namespace Atlas.Tests.Client;

// SmokeMockTest 覆盖 mock 网关闭环：注册→登录→业务心跳→Notify 收推→传输 Ping 往返。
// mock 网关按 auth.proto 业务语义回响应（对标 Go/TS 的 mock-gateway；真机冒烟在 M3/M4）。
public sealed class SmokeMockTest
{
    [Fact]
    public async Task Smoke_RegisterLoginHeartbeat_ClosedLoopSucceeds()
    {
        await using var gateway = new MockGatewayServer();
        await using var channel = await ConnectAsync(gateway, 500);
        var serializer = new JsonSerializer();

        // 注册：player_id + password → 回执 player_id。
        var registerReq = new RegisterRequest { PlayerId = "p1", Password = "pw-1" };
        var registerBytes = serializer.Serialize(registerReq);
        var registerResp = await channel.InvokeRawAsync(opRegister, registerBytes, CancellationToken.None);
        var registerReply = (RegisterReply)serializer.Deserialize(registerResp, typeof(RegisterReply));
        Assert.Equal("p1", registerReply.PlayerId);

        // 登录：player_id + password → token + player_id + server_time。
        var loginReq = new LoginRequest { PlayerId = "p1", Password = "pw-1" };
        var loginBytes = serializer.Serialize(loginReq);
        var loginResp = await channel.InvokeRawAsync(opLogin, loginBytes, CancellationToken.None);
        var loginReply = (LoginReply)serializer.Deserialize(loginResp, typeof(LoginReply));
        Assert.Equal("p1", loginReply.PlayerId);
        Assert.False(string.IsNullOrEmpty(loginReply.Token));
        Assert.True(loginReply.ServerTimeUnixMs > 0);

        // 业务心跳：token + player_id + ts → 回显 ts + server_time。
        var heartbeatReq = new HeartbeatRequest
        {
            Token = loginReply.Token,
            PlayerId = "p1",
            Ts = 1234567890L,
        };
        var heartbeatBytes = serializer.Serialize(heartbeatReq);
        var heartbeatResp = await channel.InvokeRawAsync(opHeartbeat, heartbeatBytes, CancellationToken.None);
        var heartbeatReply = (HeartbeatReply)serializer.Deserialize(heartbeatResp, typeof(HeartbeatReply));
        Assert.Equal(1234567890L, heartbeatReply.Ts);
        Assert.True(heartbeatReply.ServerTimeUnixMs > 0);

        // 网关应收到四种 op（顺序注册→登录→业务心跳；Ping 默认关闭）。
        Assert.Equal(new[] { opRegister, opLogin, opHeartbeat }, gateway.Operations);
    }

    [Fact]
    public async Task Smoke_NotifyPush_DeliveredToSubscriber()
    {
        await using var gateway = new MockGatewayServer();
        await using var channel = await ConnectAsync(gateway, 500);
        var received = NewSignal();

        // 订阅业务通知 op（如匹配成功推送）。
        channel.On("match.found", (op, payload) =>
        {
            Assert.Equal("match.found", op);
            var serializer = new JsonSerializer();
            var notice = (RegisterReply)serializer.Deserialize(payload, typeof(RegisterReply));
            Assert.Equal("p1", notice.PlayerId);
            received.TrySetResult(true);
        });

        await gateway.PushNotifyAsync("match.found", new JsonSerializer().Serialize(new RegisterReply { PlayerId = "p1" }));

        await received.Task.WaitAsync(TimeSpan.FromSeconds(2));
    }

    [Fact]
    public async Task Smoke_TransportPing_RoundtripReturnsEmpty()
    {
        await using var gateway = new MockGatewayServer();
        await using var channel = await ConnectAsync(gateway, 500);

        // 传输心跳 Ping：空 payload → 空响应（往返完成即链路存活）。
        var pingResp = await channel.InvokeRawAsync(Channel.HeartbeatOperation, null, CancellationToken.None);

        Assert.Empty(pingResp);
        Assert.Contains(Channel.HeartbeatOperation, gateway.Operations);
    }

    private static async Task<Channel> ConnectAsync(MockGatewayServer gateway, int timeoutMs)
    {
        var channel = new Channel(
            token => TcpTestTransport.ConnectAsync(gateway.Port, token),
            new ChannelOptions
            {
                InvokeTimeoutMs = timeoutMs,
                HeartbeatIntervalMs = 0, // 测试按需手动发 Ping，避免周期心跳干扰 op 断言。
            });
        await channel.ConnectAsync(CancellationToken.None);
        return channel;
    }

    private static TaskCompletionSource<bool> NewSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private const string opRegister = "/gateway.v1.GatewayAuth/Register";
    private const string opLogin = "/gateway.v1.GatewayAuth/Login";
    private const string opHeartbeat = "/gateway.v1.GatewayAuth/Heartbeat";
}

// MockGatewayServer 是模拟网关业务语义的 TCP 回环服务端：按 operation 分派
// 业务响应（auth.proto DTO，protojson 编解码），Ping 回空包；支持主动推 Notify。
internal sealed class MockGatewayServer : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentQueue<string> _operations = new();
    private readonly JsonSerializer _serializer = new();
    private readonly object _streamGate = new();
    private Stream? _stream;
    private Task? _serveTask;

    public MockGatewayServer()
    {
        _listener.Start();
        _serveTask = AcceptAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    // Operations 返回网关收到的全部 operation（按到达顺序）。
    public string[] Operations => _operations.ToArray();

    // PushNotifyAsync 主动推送一条 Notify（模拟服务端业务推送）。
    public async Task PushNotifyAsync(string operation, byte[] payload)
    {
        var stream = await WaitForStreamAsync();
        var body = Body.BuildRequestBody(operation, payload);
        var header = new Header { Type = MsgType.Notify, Version = FrameConst.Version, Seq = 1 };
        await FrameIO.WriteFrameAsync(stream, header, body, FrameConst.MaxBodySize, _stop.Token);
    }

    public async ValueTask DisposeAsync()
    {
        _stop.Cancel();
        _listener.Stop();
        if (_serveTask != null)
        {
            try
            {
                await _serveTask;
            }
            catch (OperationCanceledException)
            {
            }
            catch (IOException)
            {
            }
            catch (SocketException)
            {
            }
        }
        _stop.Dispose();
    }

    private async Task AcceptAsync()
    {
        try
        {
            using var client = await _listener.AcceptTcpClientAsync(_stop.Token);
            await ServeAsync(client.GetStream());
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested)
        {
        }
        catch (ObjectDisposedException) when (_stop.IsCancellationRequested)
        {
        }
    }

    private async Task ServeAsync(Stream stream)
    {
        lock (_streamGate)
        {
            _stream = stream;
        }
        while (!_stop.IsCancellationRequested)
        {
            var (header, body) = await FrameIO.ReadFrameAsync(stream, FrameConst.MaxBodySize, _stop.Token);
            var (operation, payload) = Body.ParseRequestBody(body);
            _operations.Enqueue(operation);
            await HandleOperationAsync(stream, header, operation, payload);
        }
    }

    private async Task HandleOperationAsync(Stream stream, Header header, string operation, byte[] payload)
    {
        switch (operation)
        {
            case Channel.HeartbeatOperation:
                await WriteReplyAsync(stream, header.Seq, Array.Empty<byte>(), _stop.Token);
                return;
            case opRegister:
            {
                var req = (RegisterRequest)_serializer.Deserialize(payload, typeof(RegisterRequest));
                Assert.False(string.IsNullOrEmpty(req.PlayerId), "注册请求缺 player_id");
                await WriteReplyAsync(stream, header.Seq, _serializer.Serialize(new RegisterReply { PlayerId = req.PlayerId }), _stop.Token);
                return;
            }
            case opLogin:
            {
                var req = (LoginRequest)_serializer.Deserialize(payload, typeof(LoginRequest));
                Assert.False(string.IsNullOrEmpty(req.PlayerId), "登录请求缺 player_id");
                await WriteReplyAsync(
                    stream,
                    header.Seq,
                    _serializer.Serialize(new LoginReply
                    {
                        Token = "tok-" + req.PlayerId,
                        PlayerId = req.PlayerId,
                        ServerTimeUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    }),
                    _stop.Token);
                return;
            }
            case opHeartbeat:
            {
                var req = (HeartbeatRequest)_serializer.Deserialize(payload, typeof(HeartbeatRequest));
                Assert.False(string.IsNullOrEmpty(req.Token), "业务心跳缺 token");
                await WriteReplyAsync(
                    stream,
                    header.Seq,
                    _serializer.Serialize(new HeartbeatReply
                    {
                        Ts = req.Ts,
                        ServerTimeUnixMs = (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    }),
                    _stop.Token);
                return;
            }
            default:
                throw new ProtocolException($"mock 网关不支持 operation: {operation}");
        }
    }

    private static async Task WriteReplyAsync(Stream stream, uint sequence, byte[] data, CancellationToken token)
    {
        // 成功包络 [hasError=0][dataLen:u32][data...]。
        var reply = new byte[5 + data.Length];
        reply[0] = 0;
        WriteUInt32(reply, 1, (uint)data.Length);
        Array.Copy(data, 0, reply, 5, data.Length);
        var header = new Header { Type = MsgType.Response, Version = FrameConst.Version, Seq = sequence };
        await FrameIO.WriteFrameAsync(stream, header, reply, FrameConst.MaxBodySize, token);
    }

    private async Task<Stream> WaitForStreamAsync()
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(2);
        while (DateTime.UtcNow < deadline)
        {
            Stream? stream;
            lock (_streamGate)
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

    private static void WriteUInt32(byte[] bytes, int offset, uint value)
    {
        bytes[offset] = (byte)(value >> 24);
        bytes[offset + 1] = (byte)(value >> 16);
        bytes[offset + 2] = (byte)(value >> 8);
        bytes[offset + 3] = (byte)value;
    }

    private const string opRegister = "/gateway.v1.GatewayAuth/Register";
    private const string opLogin = "/gateway.v1.GatewayAuth/Login";
    private const string opHeartbeat = "/gateway.v1.GatewayAuth/Heartbeat";
}
