using System;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Atlas.Frame;
using Google.Protobuf;
using Google.Protobuf.Reflection;
using StjJsonSerializer = System.Text.Json.JsonSerializer;

namespace Atlas.Serialization;

// JsonSerializer 使用 Google.Protobuf 的 protojson 实现，载荷编码版本为 1。
// 序列化零值省略（JsonFormatter 默认）；反序列化 IgnoreUnknownFields——对齐
// Go contrib/protojson 的 DiscardUnknown 语义：服务端加字段不破坏旧客户端。
// 生成 stub 的 POCO DTO（无 protobuf 运行时）走 System.Text.Json 分支：字段名与
// 数值处理取自 DTO 上的 [JsonPropertyName]/[JsonNumberHandling]（protojson 语义）。
public sealed class JsonSerializer : ISerializer
{
    private static readonly JsonFormatter Formatter = JsonFormatter.Default;
    private static readonly JsonParser Parser = new JsonParser(
        JsonParser.Settings.Default.WithIgnoreUnknownFields(true));

    // PocoOptions 是 POCO 分支的 JSON 选项：省略未设置（null）字段——对齐 protojson
    // 默认不下发未设置字段的语义。
    private static readonly JsonSerializerOptions PocoOptions = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    // Version 是载荷编码版本（ver=1 protojson）：取值来自生成物 FrameGen，杜绝魔法值。
    public int Version => FrameGen.Version;

    public byte[] Serialize(IMessage message)
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        return Encoding.UTF8.GetBytes(Formatter.Format(message));
    }

    // Serialize 序列化 POCO DTO（生成 stub 的 DTO）：IMessage 仍走 protojson
    //（保持 int64 字符串等 protojson 形态），其余对象走 System.Text.Json 分支。
    public byte[] Serialize(object message)
    {
        if (message is IMessage proto)
        {
            return Serialize(proto);
        }

        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        return StjJsonSerializer.SerializeToUtf8Bytes(message, PocoOptions);
    }

    public object Deserialize(byte[] data, Type type)
    {
        if (data == null)
        {
            throw new ArgumentNullException(nameof(data));
        }

        if (type == null)
        {
            throw new ArgumentNullException(nameof(type));
        }

        if (!typeof(IMessage).IsAssignableFrom(type))
        {
            // POCO（生成 stub 的 DTO）分支：protojson 语义 JSON → POCO；格式非法
            // 由 System.Text.Json 抛 JsonException（调用方按协议错误处置）。
            return StjJsonSerializer.Deserialize(Encoding.UTF8.GetString(data), type, PocoOptions)!;
        }

        // 反射取消息描述符，用忽略未知字段的 JsonParser 解析（服务端加字段不破坏旧客户端）。
        var descriptor = (MessageDescriptor)type.GetProperty("Descriptor")!.GetValue(null)!;
        return Parser.Parse(Encoding.UTF8.GetString(data), descriptor);
    }
}
