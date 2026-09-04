namespace Atlas.Client;

// ClientState 表示单通道连接状态；M4 将据此聚合 dual 形态状态。
public enum ClientState
{
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
}
