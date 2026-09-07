using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Serialization;
using Gateway.V1;

namespace Atlas.Smoke;

// 协议常量（与模板 api/gateway/v1 op 全名一致；冒烟 DTO 字段与
// atlas-game-layout api/gateway/v1/auth.proto 对齐，去掉跨包引用）。
public static class Ops
{
    public const string Register = "/gateway.v1.GatewayAuth/Register";
    public const string Login = "/gateway.v1.GatewayAuth/Login";
    public const string Heartbeat = "/gateway.v1.GatewayAuth/Heartbeat";
    public const string Ping = "/atlas.internal.Heartbeat/Ping";
    public const string JoinBattle = "/gateway.v1.GatewayBattle/JoinBattle";
}

// 载荷编码模式：json=protojson（ver=1）、protobuf=二进制（ver=2）。
// C# 全 Google.Protobuf 官方栈——json 即 protojson 语义（设计决策 D4/D9）。
public enum SmokeMode
{
    Json,       // JsonSerializer（protojson 兼容，ver=1）
    ProtoJson,  // 同上（C# 无独立 protojson 形态，json 即 protojson）
    Protobuf,   // ProtobufSerializer（ver=2 二进制）
}

// SmokeOps 封装冒烟业务操作的 DTO 构造与解析（三编码经同一 ISerializer 分派）。
public sealed class SmokeOps
{
    private readonly ISerializer _serializer;

    public SmokeOps(SmokeMode mode)
    {
        _serializer = mode == SmokeMode.Protobuf
            ? new ProtobufSerializer()
            : new JsonSerializer();
    }

    public string ModeName(SmokeMode mode)
    {
        return mode == SmokeMode.Protobuf ? "protobuf(ver=2)" : "json/protojson(ver=1)";
    }

    public async Task<string> RegisterAsync(Channel channel, string account, CancellationToken ct)
    {
        var request = new RegisterRequest
        {
            Account = account,
            Password = "pw-123456",
            Nickname = "冒烟玩家",
        };
        var payload = _serializer.Serialize(request);
        var response = await channel.InvokeRawAsync(Ops.Register, payload, ct);
        var reply = (RegisterReply)_serializer.Deserialize(response, typeof(RegisterReply));
        return reply.PlayerId;
    }

    public async Task<(string playerId, string token)> LoginAsync(Channel channel, string player, CancellationToken ct)
    {
        var request = new LoginRequest { PlayerId = player, Password = "pw-123456" };
        var payload = _serializer.Serialize(request);
        var response = await channel.InvokeRawAsync(Ops.Login, payload, ct);
        var reply = (LoginReply)_serializer.Deserialize(response, typeof(LoginReply));
        return (reply.PlayerId, reply.Token);
    }

    public async Task HeartbeatAsync(Channel channel, string player, string token, CancellationToken ct)
    {
        var request = new HeartbeatRequest
        {
            Token = token,
            PlayerId = player,
            Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        };
        var payload = _serializer.Serialize(request);
        var response = await channel.InvokeRawAsync(Ops.Heartbeat, payload, ct);
        _serializer.Deserialize(response, typeof(HeartbeatReply));
    }

    // PingAsync 传输保活探针：空 payload 往返（对齐 Go HeartbeatOperation）。
    // 返回 true = 往返完成：内置 Ping 成功或业务拒绝（BusinessException）均视为
    // 链路存活（对齐 Go probeAlive——业务拒绝即请求-响应往返完成，网络类错误
    // 才算失败）。
    public async Task<bool> PingAsync(Channel channel, CancellationToken ct)
    {
        try
        {
            await channel.InvokeRawAsync(Ops.Ping, null, ct);
            return true;
        }
        catch (BusinessException)
        {
            return true; // 业务拒绝 = 往返完成 = 链路存活
        }
        catch (Exception)
        {
            return false; // 网络/超时/协议类 = 链路未恢复
        }
    }

    // JoinBattleAsync 战斗绑定探针：伪造 token 验证战斗通道 payload 编解码——
    // 服务端因会话无效回业务拒绝（BusinessException）即证明解码成功
    //（协议错误/解码失败才说明编解码问题，对标 Go runBattleChannelSmoke）。
    public async Task<bool> TryJoinBattleAsync(Channel channel, CancellationToken ct)
    {
        var request = new JoinBattleRequest
        {
            Token = "no-token",
            PlayerId = "none",
            BattleId = "b1",
        };
        var payload = _serializer.Serialize(request);
        try
        {
            await channel.InvokeRawAsync(Ops.JoinBattle, payload, ct);
            return false; // 未拒绝 = 异常
        }
        catch (BusinessException)
        {
            return true; // 业务拒绝 = 解码成功
        }
    }
}
