using System.Threading.Tasks;
using Atlas.Frame;

namespace Atlas.Client;

// InflightKey 用连接代次隔离跨重连的相同 seq 响应。
internal readonly struct InflightKey
{
    public InflightKey(uint epoch, uint sequence)
    {
        Epoch = epoch;
        Sequence = sequence;
    }

    public uint Epoch { get; }

    public uint Sequence { get; }
}

// Inflight 保存等待响应的请求完成源；RunContinuationsAsynchronously 防止读循环执行调用方续体。
internal sealed class Inflight
{
    public Inflight()
    {
        Completion = new TaskCompletionSource<ReplyData>(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    public TaskCompletionSource<ReplyData> Completion { get; }
}
