using System;
using Google.Protobuf;

namespace Atlas.Serialization;

// ISerializer 是 Atlas 载荷编码插槽；Version 对应帧头的载荷编码协商位。
// 两个 Serialize 重载：IMessage（protobuf 官方运行时消息）与 object（生成 stub 的
// POCO DTO，R12 方案 A）——stub 经构造注入本接口编解码，不写死任何 JSON 库。
public interface ISerializer
{
    int Version { get; }

    byte[] Serialize(IMessage message);

    // Serialize 序列化任意消息对象：IMessage 走各实现的原生路径，POCO（生成 stub
    // 的 DTO）仅 ver=1（protojson 语义）实现支持——ver=2 只收 IMessage（R12 边界）。
    byte[] Serialize(object message);

    object Deserialize(byte[] data, Type type);
}
