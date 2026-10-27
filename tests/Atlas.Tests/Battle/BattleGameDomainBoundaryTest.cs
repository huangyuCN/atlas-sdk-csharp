using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using Atlas.Battle;
using Xunit;

namespace Atlas.Tests.Battle;

// BattleGameDomainBoundaryTest 是**依赖边界守卫**：游戏域消息（battle / game / matcher 的
// DTO 与 op 表）已迁出 SDK，改由游戏协议包（NuGet HuangyuCN.AtlasGame.Protocol，源自模板仓
// protocol/）按 .proto 生成；SDK 只留传输/会话 + 契约消息（frame / edge / session / common /
// route / lockstep）。
//
// 本用例把迁移的**结果**钉死（而不是钉实现）：仓内不得再有游戏域生成包与手写 op 表，
// 生成脚本也不得再把它们生成回来——手滑加回生成物或改回 gen-dto，这里即红。
public sealed class BattleGameDomainBoundaryTest
{
    // Name 用片段拼接被禁字面量：本文件自身不得出现这些字面量，否则「仓内零引用」的 grep
    // 证据会被守卫文件自己污染（与 Go SDK 边界守卫同一做法）。
    private static string Name(params string[] parts)
    {
        return string.Concat(parts);
    }

    // GameDomainFiles 是已迁出的游戏域生成物（相对仓根）：存在即说明生成物又回流了。
    private static readonly string[] GameDomainFiles =
    {
        Path.Combine("examples", "Smoke", "Proto", "gen", "imessage", Name("Battle", ".cs")),
        Path.Combine("examples", "Smoke", "Proto", "gen", "imessage", Name("Battle", "Service", ".cs")),
        Path.Combine("src", "Atlas", "Battle", Name("Battle", "Ops", ".cs")),
        Path.Combine("src", "Atlas", "Battle", "Gen", Name("Battle", "Service", "Ops.g.cs")),
        Path.Combine("src", "Atlas", "Battle", "Gen", Name("Player", "Service", "Ops.g.cs")),
    };

    // GameDomainTypes 是已迁出的游戏域类型/命名空间前缀：仓内任何 .cs 都不得再出现
    // （含测试与示例——SDK 自己的用例只落在裸字节与本地结构体上）。
    private static readonly string[] GameDomainTypes =
    {
        Name("Atlas.", "Battle", ".V1"),
        Name("Battle", "Service", "ProtocolOps"),
        Name("Battle", "Service", "PushOps"),
        Name("Player", "Service", "PushOps"),
        Name("JoinBattle", "Req"),
        Name("FrameInput", "Req"),
        Name("SyncFrames", "Req"),
        Name("Battle", "Ops"),
    };

    // gen-dto 的产出清单里不得再有游戏域生成项（否则「重生成无 diff」门禁会把它生成回来）。
    private static readonly string[] ForbiddenGenEntries =
    {
        Name("battle", "_service.proto"),
        Name("player", "_service.proto"),
        Name("api/", "battle"),
        Name("api/", "game"),
        Name("src/Atlas/Battle/", "Gen"),
    };

    // TestNoGameDomainArtifacts 断言游戏域生成物与手写 op 表都不在仓内。
    [Fact]
    public void TestNoGameDomainArtifacts()
    {
        var root = RepoRoot();
        foreach (var file in GameDomainFiles)
        {
            Assert.False(File.Exists(Path.Combine(root, file)),
                $"游戏域生成物仍在：{file}（应由游戏协议包提供，SDK 不再生成/手写）");
        }
    }

    // TestNoGameDomainTypeReferences 断言仓内没有任何 .cs 再引用游戏域类型
    // （不看 generated 头：生成物本身也在守卫范围内）。
    [Fact]
    public void TestNoGameDomainTypeReferences()
    {
        var root = RepoRoot();
        var offenders = new List<string>();
        foreach (var path in SourceFiles(root))
        {
            var text = File.ReadAllText(path);
            foreach (var type in GameDomainTypes)
            {
                if (text.Contains(type, StringComparison.Ordinal))
                {
                    offenders.Add($"{Relative(root, path)} 仍引用游戏域类型 {type}");
                }
            }
        }
        Assert.True(offenders.Count == 0, string.Join("\n", offenders));
    }

    // TestGenScriptDropsGameDomain 断言生成脚本不再产出游戏域 DTO/op 快照：
    // 否则门禁每次都会把删掉的东西生成回来（源码里删了也白删）。
    [Fact]
    public void TestGenScriptDropsGameDomain()
    {
        var script = File.ReadAllText(Path.Combine(RepoRoot(), "scripts", "gen-dto.sh"));

        foreach (var entry in ForbiddenGenEntries)
        {
            Assert.DoesNotContain(entry, script, StringComparison.Ordinal);
        }
        // 契约消息仍在生成清单里（会话 DTO 与 lockstep 契约消息保留）。
        Assert.Contains("api/gateway/v1/session.proto", script, StringComparison.Ordinal);
        Assert.Contains("api/lockstep/lockstep.proto", script, StringComparison.Ordinal);
    }

    // SourceFiles 枚举仓内 .cs 文件（跳过 bin/obj 与 CI 检出到工作区内的上游仓）。
    private static IEnumerable<string> SourceFiles(string root)
    {
        var stack = new Stack<string>();
        stack.Push(root);
        while (stack.Count > 0)
        {
            var dir = stack.Pop();
            foreach (var sub in Directory.GetDirectories(dir))
            {
                var name = Path.GetFileName(sub);
                if (name is "bin" or "obj" or ".git" or "atlas" or "atlas-game-layout")
                {
                    continue;
                }
                stack.Push(sub);
            }
            foreach (var file in Directory.GetFiles(dir, "*.cs"))
            {
                yield return file;
            }
        }
    }

    private static string Relative(string root, string path)
    {
        return Path.GetRelativePath(root, path);
    }

    // RepoRoot 返回本仓根目录（本文件在 tests/Atlas.Tests/Battle/ 下，向上三级）。
    private static string RepoRoot([CallerFilePath] string path = "")
    {
        return Path.GetFullPath(Path.Combine(Path.GetDirectoryName(path)!, "..", "..", ".."));
    }
}
