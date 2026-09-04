using System;
using System.Reflection;
using System.Text;
using Google.Protobuf;

namespace Atlas.Serialization;

// JsonSerializer 使用 Google.Protobuf 的 protojson 实现，载荷编码版本为 1。
public sealed class JsonSerializer : ISerializer
{
    private static readonly JsonFormatter Formatter = JsonFormatter.Default;

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

        var parser = GetParser(type);
        var parseJson = parser.GetType().GetMethod("ParseJson", new[] { typeof(string) });
        if (parseJson == null)
        {
            throw new ArgumentException("目标类型未提供 protobuf JSON 解析器", nameof(type));
        }

        return parseJson.Invoke(parser, new object[] { Encoding.UTF8.GetString(data) })
            ?? throw new ArgumentException("protobuf JSON 解析结果为空", nameof(data));
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
