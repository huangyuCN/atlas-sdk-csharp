using System;
using System.Reflection;
using Google.Protobuf;

namespace Atlas.Serialization;

// ProtobufSerializer 使用 Google.Protobuf 二进制 wire 编码，载荷编码版本为 2。
// DTO 为 protoc-gen-csharp 产出的 IMessage（.pb.cs）；序列化走 ToByteArray、
// 反序列化走生成消息的静态 Parser.ParseFrom——与 Go contrib/protobuf（protobuf-go
// 运行时）同一协议的官方实现，字节级互通。
public sealed class ProtobufSerializer : ISerializer
{
    public int Version => 2;

    public byte[] Serialize(IMessage message)
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        return message.ToByteArray();
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

        var parser = GetParser(type);
        var parseFrom = parser.GetType().GetMethod("ParseFrom", new[] { typeof(byte[]) });
        if (parseFrom == null)
        {
            throw new ArgumentException("目标类型未提供 protobuf 二进制解析器", nameof(type));
        }

        return parseFrom.Invoke(parser, new object[] { data })
            ?? throw new ArgumentException("protobuf 二进制解析结果为空", nameof(data));
    }

    private static object GetParser(Type type)
    {
        var property = type.GetProperty("Parser", BindingFlags.Public | BindingFlags.Static);
        var parser = property?.GetValue(null);
        if (parser == null)
        {
            throw new ArgumentException("目标类型未提供 protobuf 解析器", nameof(type));
        }

        return parser;
    }
}
