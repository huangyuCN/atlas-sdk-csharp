using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Frame;
using Atlas.Serialization;
using Atlas.Transport;
using Google.Protobuf;
using Xunit;

namespace Atlas.Tests.Client;

public sealed class ChannelTest
{
    [Fact]
    public async Task InvokeRawAsync_EchoesPayload_AndSupportsNullPayload()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);

        var response = await channel.InvokeRawAsync("echo", new byte[] { 1, 2, 3 }, CancellationToken.None);
        var empty = await channel.InvokeRawAsync("empty", null, CancellationToken.None);

        Assert.Equal(new byte[] { 1, 2, 3 }, response);
        Assert.Empty(empty);
        Assert.Equal(ClientState.Connected, channel.State);
    }

    [Fact]
    public async Task InvokeRawAsync_TimesOut_AndLateResponseIsDiscarded()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 50);

        var pending = channel.InvokeRawAsync("hold", new byte[] { 1 }, CancellationToken.None);
        await server.WaitForHoldAsync();
        await Assert.ThrowsAsync<Atlas.Errors.TimeoutException>(() => pending);

        server.ReleaseHold();
        await Task.Delay(30);
        var response = await channel.InvokeRawAsync("echo", new byte[] { 2 }, CancellationToken.None);

        Assert.Equal(new byte[] { 2 }, response);
    }

    [Fact]
    public async Task InvokeRawAsync_SequencesIncreaseMonotonically()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);

        await channel.InvokeRawAsync("echo", new byte[] { 1 }, CancellationToken.None);
        await channel.InvokeRawAsync("echo", new byte[] { 2 }, CancellationToken.None);

        Assert.Equal(new uint[] { 1, 2 }, server.Sequences.Take(2).ToArray());
    }

    [Fact]
    public async Task InvokeRawAsync_ConnectionClosed_FailsWithNetworkException()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);

        await Assert.ThrowsAsync<NetworkException>(
            () => channel.InvokeRawAsync("kick", Array.Empty<byte>(), CancellationToken.None));
    }

    [Fact]
    public async Task InvokeRawAsync_ResponseVersionMismatch_FailsWithProtocolException()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500);

        await Assert.ThrowsAsync<ProtocolException>(
            () => channel.InvokeRawAsync("bad-version", Array.Empty<byte>(), CancellationToken.None));

        await WaitForStateAsync(channel, ClientState.Disconnected);
        await Assert.ThrowsAsync<NetworkException>(
            () => channel.InvokeRawAsync("echo", Array.Empty<byte>(), CancellationToken.None));
    }

    [Fact]
    public async Task InvokeRawAsync_ResponseClaimedBeforeTimeout_ReturnsResponse()
    {
        var transport = new ControlledTransport();
        await using var channel = await ConnectControlledAsync(transport, 40, 37);
        var releaseCompletion = NewSignal();
        var claimed = NewSignal();
        channel.BeforeInflightCompletion = async () =>
        {
            claimed.TrySetResult(true);
            await releaseCompletion.Task;
        };

        var invoke = channel.InvokeRawAsync("echo", new byte[] { 7 }, CancellationToken.None);
        await transport.WaitForWritesAsync(1).WaitAsync(TimeSpan.FromSeconds(1));
        transport.QueueReply(transport.WrittenHeaders[0].Seq, FrameConst.Version, new byte[] { 9 });

        try
        {
            await claimed.Task.WaitAsync(TimeSpan.FromSeconds(1));
            await Task.Delay(80);
            Assert.False(invoke.IsCompleted);
        }
        finally
        {
            releaseCompletion.TrySetResult(true);
        }

        Assert.Equal(new byte[] { 9 }, await invoke);
        Assert.Equal(37, transport.LastReadMaxBodySize);
        Assert.Equal(37, transport.LastWriteMaxBodySize);
    }

    [Fact]
    public async Task InvokeRawAsync_OutOfOrderResponses_MatchTheirSequences()
    {
        var transport = new ControlledTransport();
        await using var channel = await ConnectControlledAsync(transport, 500, FrameConst.MaxBodySize);

        var first = channel.InvokeRawAsync("first", new byte[] { 1 }, CancellationToken.None);
        await transport.WaitForWritesAsync(1).WaitAsync(TimeSpan.FromSeconds(1));
        var second = channel.InvokeRawAsync("second", new byte[] { 2 }, CancellationToken.None);
        await transport.WaitForWritesAsync(2).WaitAsync(TimeSpan.FromSeconds(1));
        var headers = transport.WrittenHeaders;
        transport.QueueReply(headers[1].Seq, FrameConst.Version, new byte[] { 20 });

        Assert.Equal(new byte[] { 20 }, await second);
        Assert.False(first.IsCompleted);
        transport.QueueReply(headers[0].Seq, FrameConst.Version, new byte[] { 10 });
        Assert.Equal(new byte[] { 10 }, await first);
    }

    [Fact]
    public async Task InvokeRawAsync_CustomMaxBodySize_IsEnforcedOnWrite()
    {
        await using var server = new FakeServer();
        await using var channel = await ConnectAsync(server, 500, 2);

        await Assert.ThrowsAsync<ProtocolException>(
            () => channel.InvokeRawAsync("a", Array.Empty<byte>(), CancellationToken.None));
    }

    [Fact]
    public async Task CloseAsync_WaitsForInstallation_AndLeavesChannelDisconnected()
    {
        var transport = new ControlledTransport();
        await using var channel = new Channel(
            _ => Task.FromResult<ITransport>(transport),
            new ChannelOptions { InvokeTimeoutMs = 500 });
        var releaseInstallation = NewSignal();
        var installed = NewSignal();
        channel.AfterTransportInstalled = async () =>
        {
            installed.TrySetResult(true);
            await releaseInstallation.Task;
        };

        var connecting = channel.ConnectAsync(CancellationToken.None);
        await installed.Task.WaitAsync(TimeSpan.FromSeconds(1));
        var closing = channel.CloseAsync();
        await Task.Delay(50);
        Assert.False(closing.IsCompleted);

        releaseInstallation.TrySetResult(true);
        await connecting;
        await closing;
        Assert.Equal(ClientState.Disconnected, channel.State);
        await Assert.ThrowsAsync<NetworkException>(
            () => channel.InvokeRawAsync("echo", Array.Empty<byte>(), CancellationToken.None));
    }

    [Fact]
    public void Channel_InvalidSerializerVersion_IsRejectedBeforeConnection()
    {
        Assert.Throws<ArgumentException>(() => new Channel(
            _ => throw new InvalidOperationException("不应拨号"),
            new ChannelOptions { Serializer = new InvalidSerializer() }));
    }

    private static async Task<Channel> ConnectAsync(FakeServer server, int timeoutMs, int? maxBodySize = null)
    {
        var channel = new Channel(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            new ChannelOptions
            {
                InvokeTimeoutMs = timeoutMs,
                MaxBodySize = maxBodySize ?? FrameConst.MaxBodySize,
            });
        await channel.ConnectAsync(CancellationToken.None);
        return channel;
    }

    private static async Task<Channel> ConnectControlledAsync(
        ControlledTransport transport,
        int timeoutMs,
        int maxBodySize)
    {
        var channel = new Channel(
            _ => Task.FromResult<ITransport>(transport),
            new ChannelOptions { InvokeTimeoutMs = timeoutMs, MaxBodySize = maxBodySize });
        await channel.ConnectAsync(CancellationToken.None);
        await transport.WaitForReadAsync().WaitAsync(TimeSpan.FromSeconds(1));
        return channel;
    }

    private static async Task WaitForStateAsync(Channel channel, ClientState state)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            if (channel.State == state)
            {
                return;
            }
            await Task.Delay(10);
        }
        Assert.Equal(state, channel.State);
    }

    private static TaskCompletionSource<bool> NewSignal()
    {
        return new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class InvalidSerializer : ISerializer
    {
        public int Version => 3;

        public byte[] Serialize(IMessage message) => Array.Empty<byte>();

        public object Deserialize(byte[] data, Type type) => throw new NotSupportedException();
    }
}
