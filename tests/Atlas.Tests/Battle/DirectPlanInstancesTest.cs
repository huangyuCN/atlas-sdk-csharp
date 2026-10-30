using System;
using System.Collections.Generic;
using Atlas.Battle;
using Xunit;

namespace Atlas.Tests.Battle;

// DirectPlanInstancesTest 覆盖「逐人 endpoints」取值口径：多实例接入层按 player_id 哈希选实例，
// 服务端把该玩家被选中实例的面列表写进**票条目**（BattleTicketEntry.endpoints）；SDK 必须优先用
// 本人票条目里的地址，票条目缺失/为空（旧服务端）才回落到回执（事件）级 endpoints（= 首实例）。
public sealed class DirectPlanInstancesTest
{
    // 两玩家的票密文（标准 base64；身份由票本身确定，SDK 不猜、不取第一条）。
    private const string TicketP1 = "AQIDBA=="; // 0x01 0x02 0x03 0x04
    private const string TicketP2 = "BQYHCA=="; // 0x05 0x06 0x07 0x08

    // FirstInstance 是回执（事件）级 endpoints：服务端取**首个实例**的面列表，仅作旧客户端兜底。
    private const string FirstInstance = "10.0.0.1:7100";

    // EntriesAb 是两玩家的票条目：p-1 → 实例 A，p-2 → 实例 B。
    private const string EntriesAb =
        "{\"player_id\":\"p-1\",\"ticket\":\"" + TicketP1 + "\","
        + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"edge-a.example.com:7100\"}]},"
        + "{\"player_id\":\"p-2\",\"ticket\":\"" + TicketP2 + "\","
        + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"edge-b.example.com:7100\"}]}";

    // NotifyWithEntries 组装「带逐人票条目」的成局推送（battle_ticket = 本人那张票）。
    private static string NotifyWithEntries(string ownTicket, string entries)
    {
        return "{\"match_id\":\"m-1\",\"battle_id\":\"b-1\",\"player_ids\":[\"p-1\",\"p-2\"],"
            + "\"battle_ticket\":\"" + ownTicket + "\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"" + FirstInstance + "\"}],"
            + "\"tickets\":[" + entries + "]}";
    }

    //票条目带逐人 endpoints ⇒ 用它而不是回执级首实例（多实例真正分散的唯一依据）。
    [Fact]
    public void FromNotify_PrefersTicketEntryEndpoints()
    {
        var plan = DirectPlan.FromNotify(NotifyWithEntries(TicketP1, EntriesAb));

        Assert.Equal("edge-a.example.com:7100", plan.Endpoint(EdgeTransport.Ws));
        Assert.Equal(new byte[] { 0x01, 0x02, 0x03, 0x04 }, plan.Ticket);
    }

    //票条目没有逐人 endpoints（旧服务端）⇒ 回落回执级列表。
    [Fact]
    public void FromNotify_FallsBackWhenTicketEntryEndpointsMissing()
    {
        var cases = new Dictionary<string, string>
        {
            ["没有逐人票条目列表"] = NotifyWithEntries(TicketP1, ""),
            ["票条目无 endpoints 字段"] = "{\"battle_id\":\"b-1\",\"battle_ticket\":\"" + TicketP1 + "\","
                + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"" + FirstInstance + "\"}],"
                + "\"tickets\":[{\"player_id\":\"p-1\",\"ticket\":\"" + TicketP1 + "\"}]}",
            ["票条目 endpoints 为空"] = "{\"battle_id\":\"b-1\",\"battle_ticket\":\"" + TicketP1 + "\","
                + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"" + FirstInstance + "\"}],"
                + "\"tickets\":[{\"player_id\":\"p-1\",\"ticket\":\"" + TicketP1 + "\",\"endpoints\":[]}]}",
            ["票条目里没有本人"] = "{\"battle_id\":\"b-1\",\"battle_ticket\":\"" + TicketP1 + "\","
                + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"" + FirstInstance + "\"}],"
                + "\"tickets\":[{\"player_id\":\"p-2\",\"ticket\":\"" + TicketP2 + "\","
                + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"edge-b.example.com:7100\"}]}]}",
        };
        foreach (var pair in cases)
        {
            var plan = DirectPlan.FromNotify(pair.Value);
            Assert.Equal(FirstInstance, plan.Endpoint(EdgeTransport.Ws));
        }
    }

    //同一玩家多次解析 ⇒ 同一地址；不同玩家 ⇒ 各自实例（不得「取第一条」）。
    [Fact]
    public void FromNotify_SamePlayerSameAddress()
    {
        var first = DirectPlan.FromNotify(NotifyWithEntries(TicketP1, EntriesAb));
        var want = first.Endpoint(EdgeTransport.Ws);
        Assert.Equal("edge-a.example.com:7100", want);
        for (var i = 0; i < 3; i++)
        {
            var again = DirectPlan.FromNotify(NotifyWithEntries(TicketP1, EntriesAb));
            Assert.Equal(want, again.Endpoint(EdgeTransport.Ws));
        }
        var other = DirectPlan.FromNotify(NotifyWithEntries(TicketP2, EntriesAb));
        Assert.Equal("edge-b.example.com:7100", other.Endpoint(EdgeTransport.Ws));
        Assert.NotEqual(want, other.Endpoint(EdgeTransport.Ws));
    }

    // 事件形态的列表键名（battle_tickets）与 proto 字段名口径同样认。
    [Fact]
    public void FromNotify_AcceptsEventTicketListKey()
    {
        var payload = "{\"matchId\":\"m-1\",\"battleId\":\"b-1\",\"battleTicket\":\"" + TicketP1 + "\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"" + FirstInstance + "\"}],"
            + "\"battle_tickets\":[{\"playerId\":\"p-1\",\"ticket\":\"" + TicketP1 + "\","
            + "\"endpoints\":[{\"transport\":\"EDGE_TRANSPORT_WS\",\"address\":\"edge-a.example.com:7100\"}]}]}";
        var plan = DirectPlan.FromNotify(payload);

        Assert.Equal("edge-a.example.com:7100", plan.Endpoint(EdgeTransport.Ws));
    }
}
