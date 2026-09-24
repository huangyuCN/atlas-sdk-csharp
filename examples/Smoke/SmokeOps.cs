using System;
using System.Threading;
using System.Threading.Tasks;
using Atlas.Client;
using Atlas.Errors;
using Atlas.Serialization;
using Battle.V1;
using Gateway.V1;

namespace Atlas.Smoke;

// 协议常量：会话 op 名取自生成物（Atlas.Gateway.V1.SessionProtocolOps），本类只保留
// 生成物覆盖不到的传输面 op。
// 战斗域 op 为 examples 局部常量：战斗 POCO 生成物（Atlas.Battle.V1）会与其 IMessage
// 族的 lockstep 命名空间（protoc 生成的 Atlas.Game.Lockstep）同名冲突，故本仓不生成
// 战斗 POCO（冒烟走 IMessage 路径，见 scripts/gen-dto.sh）。
public static class Ops
{
    // Ping 是服务端内置传输心跳 op（无 payload 往返）。
    public const string Ping = "/atlas.internal.Heartbeat/Ping";

    // JoinBattle 是战斗域加入 op（与模板 api/battle/v1/battle_service.proto 同源）。
    public const string JoinBattle = "/battle.v1.BattleService/JoinBattle";
}

// 载荷编码模式：json=protojson（ver=1）、protobuf=二进制（ver=2）。
// C# 全 Google.Protobuf 官方栈——json 即 protojson 语义（设计决策 D4/D9）。
public enum SmokeMode
{
    Json,       // JsonSerializer（protojson 兼容，ver=1）
    ProtoJson,  // 同上（C# 无独立 protojson 形态，json 即 protojson）
    Protobuf,   // ProtobufSerializer（ver=2 二进制）
}

// SmokeOps 封装冒烟业务操作的 DTO 构造与解析（三编码经同一 ISerializer 分派；
// op 名取自生成物 Atlas.Gateway.V1.SessionProtocolOps / Atlas.Battle.V1.BattleService）。
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

    // RegisterAsync 注册（回执含 playerId；不建立会话）。
    public async Task<string> RegisterAsync(AtlasClient client, string account, CancellationToken ct)
    {
        var request = new RegisterRequest
        {
            Account = account,
            Password = "pw-123456",
            Nickname = "冒烟玩家",
        };
        var payload = _serializer.Serialize(request);
        var response = await client.InvokeRawAsync(Atlas.Gateway.V1.SessionProtocolOps.Register, payload, ct);
        var reply = (RegisterReply)_serializer.Deserialize(response, typeof(RegisterReply));
        return reply.PlayerId;
    }

    // LoginAsync 直调登录 op（ver=2 protobuf 路径：会话回执为二进制，走 IMessage 解码）。
    public async Task<(string playerId, string token)> LoginAsync(AtlasClient client, string player, CancellationToken ct)
    {
        var request = new LoginRequest
        {
            PlayerId = player,
            Password = "pw-123456",
            // 客户端版本单一来源（M1：服务端按 min_client_version 门槛裁决）。
            ClientVersion = AtlasVersion.Value,
        };
        var payload = _serializer.Serialize(request);
        var response = await client.InvokeRawAsync(Atlas.Gateway.V1.SessionProtocolOps.Login, payload, ct);
        var reply = (LoginReply)_serializer.Deserialize(response, typeof(LoginReply));
        return (reply.PlayerId, reply.Token);
    }

    // LoginViaSessionAsync 经会话状态机登录（ver=1 protojson 路径）：请求体由本方法
    // 构造（DTO 来自生成物），凭据由状态机经接缝提取并保管（回执 JSON 由项目侧
    // GatewaySessionProtocol 用生成物 DTO 解码）。
    public async Task<(string playerId, string token)> LoginViaSessionAsync(
        Session session, string player, CancellationToken ct)
    {
        var request = new LoginRequest
        {
            PlayerId = player,
            Password = "pw-123456",
            ClientVersion = AtlasVersion.Value,
        };
        var credentials = await session.LoginAsync(ct, _serializer.Serialize(request));
        return (credentials.PlayerId, credentials.Token);
    }

    // HeartbeatAsync 业务心跳（消息体不含身份字段：身份由连接/帧会话槽承载）。
    public async Task HeartbeatAsync(AtlasClient client, CancellationToken ct)
    {
        var request = new HeartbeatRequest { Ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() };
        var payload = _serializer.Serialize(request);
        var response = await client.InvokeRawAsync(Atlas.Gateway.V1.SessionProtocolOps.Heartbeat, payload, ct);
        _serializer.Deserialize(response, typeof(HeartbeatReply));
    }

    // PingAsync 传输保活探针：空 payload 往返（对齐 Go HeartbeatOperation）。
    // 返回 true = 往返完成：内置 Ping 成功或业务拒绝（BusinessException）均视为
    // 链路存活（对齐 Go probeAlive——业务拒绝即请求-响应往返完成，网络类错误
    // 才算失败）。
    public async Task<bool> PingAsync(AtlasClient client, CancellationToken ct)
    {
        try
        {
            await client.InvokeRawAsync(Ops.Ping, null, ct);
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

    // TryJoinBattleAsync 战斗绑定探针：向不存在的战斗发起 Join（battle_id 客体寻址，
    // 消息体零身份字段）验证战斗通道 payload 编解码——服务端回业务拒绝
    //（BusinessException）即证明解码成功（协议错误/解码失败才说明编解码问题，
    // 对标 Go runBattleChannelSmoke）。
    public async Task<bool> TryJoinBattleAsync(AtlasClient client, CancellationToken ct)
    {
        var request = new JoinBattleReq { BattleId = "b1" };
        var payload = _serializer.Serialize(request);
        try
        {
            await client.InvokeRawAsync(Ops.JoinBattle, payload, ct);
            return false; // 未拒绝 = 异常
        }
        catch (BusinessException)
        {
            return true; // 业务拒绝 = 解码成功
        }
    }
}
