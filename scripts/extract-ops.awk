# extract-ops.awk 从 protoc-gen-atlas-client 的 C# 产物里抽取指定的顶层 op 常量类
#（连同其 /// 文档注释），供 gen-dto.sh 生成 SDK 侧的 op 常量快照——快照正文**逐字**
# 来自上游生成物，SDK 不手写任何 op 字面量（协议漂移由 CI 的「重生成无 diff」门禁兜住）。
#
# 用法：awk -v wants="ClassA ClassB" -f extract-ops.awk 产物.g.cs [产物2.g.cs ...]
#   wants：要抽取的顶层静态类名（空格分隔），抽取顺序 = 各输入文件中的出现顺序。
# 约束：只认生成器的固定版式（/// 注释 + "public static class X" + 行首 } 收尾）；
#       版式变化即抽不到内容，由调用方的「类缺失」检查失败暴露（不静默产出空快照）。
BEGIN {
    count = split(wants, list, " ")
    for (i = 1; i <= count; i++) {
        want[list[i]] = 1
    }
    doc = ""
    printing = 0
    emitted = 0
}

# 每个输入文件从干净状态开始：多文件时不得把上一文件的缓冲带过来。
FNR == 1 {
    doc = ""
    printing = 0
}

# 累积 /// 文档注释（类声明前的注释属于该类，随类一起抽取）。
/^\/\/\/ / {
    doc = doc $0 "\n"
    next
}

# 顶层类声明：命中期望名单即开始输出；未命中则丢弃已累积的注释。
/^public static class / {
    name = $4
    sub(/\{.*$/, "", name)
    if (name in want) {
        if (emitted > 0) {
            print ""
        }
        printf "%s", doc
        print
        printing = 1
        emitted = 1
        doc = ""
        next
    }
    printing = 0
    doc = ""
    next
}

{
    if (printing) {
        print
        if ($0 == "}") {
            printing = 0
        }
    } else {
        doc = ""
    }
}
