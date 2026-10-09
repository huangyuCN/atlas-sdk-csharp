using System.Threading;
using System.Threading.Tasks;
using Atlas.Errors;

namespace Atlas.Battle;

// BattleSession 的连接面：首次连接与显式重连。两者共用 _connectGate 串行化（单飞）：
// 并发调用不得各拨一代——后到者会摘掉/关掉先到者刚装的通道，两代都不完整（会话不可用）。
// 关闭路径只关「本次调用持有的那一代」（DetachAndCloseAsync），绝不误关别人新装的通道。
public sealed partial class BattleSession
{
    // ConnectAsync 建立直连（hello 握手 + 接入层验票 + 转发到 battle 帧面）并订阅推送。
    // 可选地随后自行调用 JoinBattleAsync / SyncFramesAsync 入局。已连接即明确报错
    //（不静默复用：重复连接说明调用方状态管理有问题）。
    public async Task ConnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();
        ThrowIfTerminal();
        await _connectGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            ThrowIfTerminal();
            if (Volatile.Read(ref _channel) != null)
            {
                throw new NetworkException(DirectErrors.SessionAlreadyConnected);
            }
            var channel = NewChannel();
            // 先登记再拨号：拨号失败/立刻断线时，重连钩子（重新入局）也能找到本代通道。
            _channel = channel;
            try
            {
                await channel.ConnectAsync(cancellationToken).ConfigureAwait(false);
                StartHeartbeat(); // 连接就绪才起表：周期到点即发保活探针（未连接不探测）。
            }
            catch
            {
                await DetachAndCloseAsync(channel).ConfigureAwait(false);
                throw;
            }
        }
        finally
        {
            _connectGate.Release();
        }
    }

    // ReconnectAsync 显式重连：关闭上一代通道 → 重新 hello（同一张票）→ JoinBattle →
    // SyncFrames(LastSeenFrame) 补帧。并发调用**单飞**：后到者复用先到者刚完成的那一轮
    //（不重复拨号/入局，也不互相关掉对方装上的通道）；任一环节失败都不留半开会话
    //（关闭本次持有的通道后抛出原异常）。
    public async Task ReconnectAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();
        ThrowIfTerminal();
        var observedRound = Volatile.Read(ref _reconnectRound);
        await _connectGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            ThrowIfClosed();
            ThrowIfTerminal();
            if (Volatile.Read(ref _reconnectRound) != observedRound)
            {
                return; // 单飞：本轮已由先到的调用完成（当前通道即新一轮的连接）。
            }
            await ReconnectRoundAsync(cancellationToken).ConfigureAwait(false);
            Interlocked.Increment(ref _reconnectRound);
        }
        finally
        {
            _connectGate.Release();
        }
    }

    // ReconnectRoundAsync 执行一轮重连（调用方持 _connectGate 且已确认需要重连）：
    // 摘并关上一代（只关自己摘下的那一个）→ 拨号 → 重新入局 → 补帧 → 起保活。
    private async Task ReconnectRoundAsync(CancellationToken cancellationToken)
    {
        var previous = Interlocked.Exchange(ref _channel, null);
        if (previous != null)
        {
            await previous.CloseAsync().ConfigureAwait(false);
        }
        var channel = NewChannel();
        _channel = channel;
        try
        {
            await channel.ConnectAsync(cancellationToken).ConfigureAwait(false);
            await JoinBattleAsync(cancellationToken).ConfigureAwait(false);
            await SyncFramesAsync(LastSeenFrame, cancellationToken).ConfigureAwait(false);
            ThrowIfClosed(); // 本轮期间被 CloseAsync：不留下无主通道（关闭后不得再连接）。
            StartHeartbeat(); // 幂等：显式重连后同样要保活（首次 Connect 已起表则原样复用）。
            CountReconnect(); // 显式重连一轮成功（拨号 + 入局 + 补帧完成）。
        }
        catch
        {
            await DetachAndCloseAsync(channel).ConfigureAwait(false);
            throw;
        }
    }
}
