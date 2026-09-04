using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;
using System.Text.Json;
using Atlas.Errors;
using Atlas.Frame;
using Xunit;

namespace Atlas.Tests.Golden;

// golden 向量由 atlas 主仓生成；此测试只消费同一份字节与语言无关期望，防止 C# 解码器漂移。
public sealed class GoldenTest
{
    private const string ErrorNone = "";
    private const string ErrorNetwork = "network";
    private const string ErrorProtocol = "protocol";

    [Fact]
    public void Consume_AllCases_AssertionsPass()
    {
        var directory = GoldenDirectory();
        Assert.True(Directory.Exists(directory), $"golden 目录不存在: {directory}");
        using var manifest = JsonDocument.Parse(File.ReadAllText(Path.Combine(directory, "manifest.json")));
        var cases = manifest.RootElement.GetProperty("cases");
        var count = 0;
        foreach (var entry in cases.EnumerateObject())
        {
            AssertCase(directory, entry.Name, entry.Value);
            count++;
        }
        Assert.True(count > 0, "golden manifest 不含用例");
    }

    private static void AssertCase(string directory, string id, JsonElement metadata)
    {
        var caseDirectory = Path.Combine(directory, "cases", id);
        var input = File.ReadAllBytes(Path.Combine(caseDirectory, "input.bin"));
        var expectedBytes = File.ReadAllBytes(Path.Combine(caseDirectory, "expected.json"));
        Assert.Equal(metadata.GetProperty("sha256_input").GetString(), Sha256Hex(input));
        Assert.Equal(metadata.GetProperty("sha256_expected").GetString(), Sha256Hex(expectedBytes));
        using var expected = JsonDocument.Parse(expectedBytes);
        var maxBodySize = metadata.GetProperty("max_body_size").GetInt32();
        switch (metadata.GetProperty("kind").GetString())
        {
            case "frame":
                AssertFrame(id, input, maxBodySize, expected.RootElement);
                break;
            case "reply":
                AssertReply(id, input, expected.RootElement);
                break;
            case "status":
                AssertStatus(id, input, expected.RootElement);
                break;
            default:
                throw new Xunit.Sdk.XunitException($"{id}: 未知 golden kind");
        }
    }

    // frame input.bin 是完整帧：16B 头后接 body，与 Go Read(bytes.NewReader(input), max) 一致。
    private static void AssertFrame(string id, byte[] input, int maxBodySize, JsonElement expected)
    {
        Header header = default;
        byte[] body = Array.Empty<byte>();
        Exception? error = null;
        try
        {
            using var stream = new MemoryStream(input, writable: false);
            (header, body) = FrameIO.ReadFrameAsync(stream, maxBodySize, default).AsTask().GetAwaiter().GetResult();
        }
        catch (Exception exception)
        {
            error = exception;
        }
        AssertError(id, error, ExpectedError(expected));
        if (error != null)
        {
            return;
        }
        Assert.Equal(expected.GetProperty("type").GetInt32(), (int)header.Type);
        Assert.Equal(expected.GetProperty("seq").GetUInt32(), header.Seq);
        var (operation, payload) = Body.ParseRequestBody(body);
        Assert.Equal(expected.GetProperty("operation").GetString(), operation);
        Assert.Equal(HexBytes(expected.GetProperty("payloadHex").GetString()), payload);
    }

    private static void AssertReply(string id, byte[] input, JsonElement expected)
    {
        ReplyData? reply = null;
        Exception? error = null;
        try
        {
            reply = Reply.DecodeReply(input);
        }
        catch (Exception exception)
        {
            error = exception;
        }
        AssertError(id, error, ExpectedError(expected));
        if (error != null)
        {
            return;
        }
        Assert.Equal(expected.GetProperty("hasStatus").GetBoolean(), reply!.Status != null);
        if (expected.TryGetProperty("dataHex", out var dataHex))
        {
            Assert.Equal(HexBytes(dataHex.GetString()), reply.Data);
        }
        if (expected.TryGetProperty("status", out var status) && status.ValueKind != JsonValueKind.Null)
        {
            AssertStatusValue(id, reply.Status, status);
        }
    }

    private static void AssertStatus(string id, byte[] input, JsonElement expected)
    {
        Status? status = null;
        Exception? error = null;
        try
        {
            status = StatusWire.Decode(input);
        }
        catch (Exception exception)
        {
            error = exception;
        }
        AssertError(id, error, ExpectedError(expected));
        if (error == null)
        {
            AssertStatusValue(id, status, expected.GetProperty("status"));
        }
    }

    // 只比较 expected.json 中存在的字段；metadata=null 表示 Go 侧同样不比较空 map。
    private static void AssertStatusValue(string id, Status? actual, JsonElement expected)
    {
        Assert.NotNull(actual);
        Assert.Equal(expected.GetProperty("code").GetInt32(), actual!.Code);
        Assert.Equal(expected.GetProperty("reason").GetString(), actual.Reason);
        Assert.Equal(expected.GetProperty("message").GetString(), actual.Message);
        if (expected.TryGetProperty("metadata", out var metadata) && metadata.ValueKind == JsonValueKind.Object)
        {
            var expectedMetadata = ReadMetadata(metadata);
            Assert.Equal(expectedMetadata.Count, actual.Metadata.Count);
            foreach (var entry in expectedMetadata)
            {
                Assert.True(actual.Metadata.TryGetValue(entry.Key, out var value), $"{id}: metadata 缺少 {entry.Key}");
                Assert.Equal(entry.Value, value);
            }
        }
    }

    private static Dictionary<string, string> ReadMetadata(JsonElement metadata)
    {
        var result = new Dictionary<string, string>();
        foreach (var entry in metadata.EnumerateObject())
        {
            result.Add(entry.Name, entry.Value.GetString() ?? "");
        }
        return result;
    }

    private static void AssertError(string id, Exception? error, string expected)
    {
        var actual = ClassifyError(error);
        Assert.True(actual == expected, $"{id}: 错误分类 = {actual}（{error}），期望 {expected}");
    }

    // 与 Go classifyError 一致：ProtocolException 是 protocol，其余非空异常都是 network。
    private static string ClassifyError(Exception? error)
    {
        if (error == null)
        {
            return ErrorNone;
        }
        return error is ProtocolException ? ErrorProtocol : ErrorNetwork;
    }

    private static string ExpectedError(JsonElement expected)
    {
        return expected.TryGetProperty("error", out var error) ? error.GetString() ?? ErrorNone : ErrorNone;
    }

    private static byte[] HexBytes(string? hex)
    {
        return string.IsNullOrEmpty(hex) ? Array.Empty<byte>() : Convert.FromHexString(hex);
    }

    private static string Sha256Hex(byte[] bytes)
    {
        using var sha256 = SHA256.Create();
        return Convert.ToHexString(sha256.ComputeHash(bytes)).ToLowerInvariant();
    }

    private static string GoldenDirectory()
    {
        var environment = Environment.GetEnvironmentVariable("ATLAS_GOLDEN_DIR");
        if (!string.IsNullOrEmpty(environment))
        {
            return environment;
        }
        return FindGoldenDirectory(Directory.GetCurrentDirectory())
            ?? FindGoldenDirectory(AppContext.BaseDirectory)
            ?? Path.GetFullPath(Path.Combine("..", "atlas", "testdata", "golden"));
    }

    // 测试宿主的工作目录会随 dotnet/IDE 改变；逐级查找同级 atlas 主仓保持默认布局可用。
    private static string? FindGoldenDirectory(string start)
    {
        for (var current = new DirectoryInfo(Path.GetFullPath(start)); current != null; current = current.Parent)
        {
            var candidate = Path.Combine(current.FullName, "..", "atlas", "testdata", "golden");
            if (Directory.Exists(candidate))
            {
                return Path.GetFullPath(candidate);
            }
        }
        return null;
    }
}
