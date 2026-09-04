using System;
using System.Text;
using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Atlas.Serialization;

// JsonSerializer 使用 Google.Protobuf 的 protojson 实现，载荷编码版本为 1。
// 序列化零值省略（JsonFormatter 默认）；反序列化 IgnoreUnknownFields——对齐
// Go contrib/protojson 的 DiscardUnknown 语义：服务端加字段不破坏旧客户端。
public sealed class JsonSerializer : ISerializer
{
    private static readonly JsonFormatter Formatter = JsonFormatter.Default;
    private static readonly JsonParser Parser = new JsonParser(
        JsonParser.Settings.Default.WithIgnoreUnknownFields(true));

    public int Version => 1;

    public byte[] Serialize(IMessage message)
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        return Encoding.UTF8.GetBytes(Formatter.Format(message));
    }

    public object Deserialize(byte[] data, Type type)
    {
        if (data == null)
        {
            throw new ArgumentNullException(nameof(data));
        }

        if (type == null || !typeof(IMessage).IsAssignableFrom(type))
        {
            throw new ArgumentException("目标类型必须实现 IMessage", nameof(type));
        }

        // 反射取消息描述符，用忽略未知字段的 JsonParser 解析（服务端加字段不破坏旧客户端）。
        var descriptor = (MessageDescriptor)type.GetProperty("Descriptor")!.GetValue(null)!;
        return Parser.Parse(Encoding.UTF8.GetString(data), descriptor);
    }
}
