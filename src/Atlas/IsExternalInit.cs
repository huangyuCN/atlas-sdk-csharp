// netstandard2.1 不含 init 访问器所需的 IsExternalInit 标记类型（.NET 5+ 才内置）。
// 本仓手写声明一份内部标记，使 S0.5 冻结的接缝形状（PushEnvelope 的 init 属性）在
// netstandard2.1/Unity 目标下可编译；类型为 internal，不进公开面。
namespace System.Runtime.CompilerServices;

internal static class IsExternalInit
{
}
