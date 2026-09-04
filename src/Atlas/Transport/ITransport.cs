using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Frame;

namespace Atlas.Transport;

// ITransport 抽象一条已连接的帧传输；M3 提供 TCP、WS、UDP、KCP 具体实现。
public interface ITransport : IAsyncDisposable
{
    ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(int maxBodySize, CancellationToken cancellationToken);

    Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken cancellationToken);

    Task CloseAsync();
}
