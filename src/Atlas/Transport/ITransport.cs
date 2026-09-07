using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Frame;

namespace Atlas.Transport;

// ITransport 抽象一条已连接的帧传输；M3 提供 TCP、WS、UDP、KCP 具体实现。
// 契约：ReadFrameAsync 由单读循环顺序调用，不得并发（UDP 传输读缓冲复用
// 依赖此约束）；WriteFrameAsync 可由上层串行化（Channel 写锁）或并发调用。
public interface ITransport : IAsyncDisposable
{
    ValueTask<(Header Header, byte[] Body)> ReadFrameAsync(int maxBodySize, CancellationToken cancellationToken);

    Task WriteFrameAsync(Header header, byte[] body, int maxBodySize, CancellationToken cancellationToken);

    Task CloseAsync();
}
