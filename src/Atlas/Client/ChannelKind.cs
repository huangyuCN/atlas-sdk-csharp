namespace Atlas.Client;

// ChannelKind 是通道角色（dual 形态区分业务/战斗通道，对标 Go client.Kind）。
// Client 级 Invoke/On 未指定通道时默认走业务通道；战斗通道用于高频帧输入
//（dual 形态第二通道，M4-5）。生命周期归 Client——视图不单独连接/关闭。
public enum ChannelKind
{
    // Business 是业务通道：登录/会话/常规业务请求（会话心跳仅业务通道生效）。
    Business,

    // Battle 是战斗通道：帧输入等高频请求（战斗通道不做业务认证，连通性由
    // 传输心跳验证；KCP/UDP 战斗通道无会话概念）。
    Battle,
}
