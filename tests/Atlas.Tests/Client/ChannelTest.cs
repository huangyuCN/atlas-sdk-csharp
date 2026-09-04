using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Serialization;
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
    }

    [Fact]
    public void Channel_InvalidSerializerVersion_IsRejectedBeforeConnection()
    {
        Assert.Throws<ArgumentException>(() => new Channel(
            _ => throw new InvalidOperationException("不应拨号"),
            new ChannelOptions { Serializer = new InvalidSerializer() }));
    }

    private static async Task<Channel> ConnectAsync(FakeServer server, int timeoutMs)
    {
        var channel = new Channel(
            token => TcpTestTransport.ConnectAsync(server.Port, token),
            new ChannelOptions { InvokeTimeoutMs = timeoutMs });
        await channel.ConnectAsync(CancellationToken.None);
        return channel;
    }

    private sealed class InvalidSerializer : ISerializer
    {
        public int Version => 3;

        public byte[] Serialize(IMessage message) => Array.Empty<byte>();

        public object Deserialize(byte[] data, Type type) => throw new NotSupportedException();
    }
}
