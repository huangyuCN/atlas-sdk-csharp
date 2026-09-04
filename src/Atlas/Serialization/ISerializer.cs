using System;
using Google.Protobuf;

namespace Atlas.Serialization;

// ISerializer 是 Atlas 载荷编码插槽；Version 对应帧头的载荷编码协商位。
public interface ISerializer
{
    int Version { get; }

    byte[] Serialize(IMessage message);

    object Deserialize(byte[] data, Type type);
}
