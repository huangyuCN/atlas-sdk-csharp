using System;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Frame;

namespace Atlas.Transport;

// TcpTransport 基于 System.Net.Sockets 的流式帧传输（TCP 通道）：
// 读侧先读满 16B 帧头、按 bodyLen 读满 body（半包补齐、多包按 Length 切分），
// 与 Go 的 tcpTransport 语义一致。帧写入以整帧原子进行（网络流顺序保证，
// 并发写由 Channel 的写锁串行化）。
public sealed class TcpTransport : ITransport
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;

    private TcpTransport(TcpClient client)
    {
        _client = client;
        _stream = client.GetStream();
    }

    // ConnectAsync 建立 TCP 连接并返回流式帧传输。netstandard2.1 的 TcpClient
    // ConnectAsync 无 token 重载——取消经注册回调关闭 socket 实现（连接中途取消
    // 会抛 ObjectDisposedException，统一按取消处理）。
    public static async Task<ITransport> ConnectAsync(string host, int port, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(host))
        {
            throw new ArgumentException("host 不能为空", nameof(host));
        }

        var client = new TcpClient();
        try
        {
            using (cancellationToken.Register(client.Dispose))
            {
                await client.ConnectAsync(host, port);
            }
            cancellationToken.ThrowIfCancellationRequested();
            return new TcpTransport(client);
        }
        catch (ObjectDisposedException) when (cancellationToken.IsCancellationRequested)
        {
            throw new OperationCanceledException(cancellationToken);
        }
        catch
        {
            client.Dispose();
            throw;
        }
    }

    public ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(int maxBodySize, CancellationToken cancellationToken)
    {
        return FrameIO.ReadFrameAsync(_stream, maxBodySize, cancellationToken);
    }

    public Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken cancellationToken)
    {
        return FrameIO.WriteFrameAsync(_stream, header, body, maxBodySize, cancellationToken).AsTask();
    }

    public Task CloseAsync()
    {
        _client.Close();
        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync()
    {
        _client.Dispose();
        return default;
    }
}
