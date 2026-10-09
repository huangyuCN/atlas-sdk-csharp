using System;
using System.Collections.Generic;
using Atlas.Errors;

namespace Atlas.Client;

// Channel 的在途表面：换代结算（本代失败）、关闭结算（全部失败）与按 key 摘除。
public sealed partial class Channel
{
    // FailGeneration 结算某一代的全部在途请求并置 Disconnected（代不匹配则只摘表不置状态）。
    private void FailGeneration(uint epoch, Exception cause)
    {
        var failed = new List<Inflight>();
        lock (_gate)
        {
            foreach (var pair in _inflight)
            {
                if (pair.Key.Epoch == epoch)
                {
                    failed.Add(pair.Value);
                }
            }
            foreach (var inflight in failed)
            {
                RemoveInflightLocked(inflight);
            }
            if (_epoch == epoch)
            {
                _transport = null;
                _generationFault = cause; // 本代已死：记录退出原因（settle 前核对用）。
                SetState(ClientState.Disconnected);
            }
        }
        foreach (var inflight in failed)
        {
            inflight.Completion.TrySetException(cause);
        }
    }

    // FailAllInflight 关闭时结算全部在途请求（对齐 Go failAllInflight）。
    private void FailAllInflight(Exception cause)
    {
        List<Inflight> failed;
        lock (_gate)
        {
            failed = new List<Inflight>(_inflight.Values);
            _inflight.Clear();
        }
        foreach (var inflight in failed)
        {
            inflight.Completion.TrySetException(cause);
        }
    }

    // SettlePending 立即以给定错误结算本代全部在途与排队请求（不关连接、不改通道状态）：
    // 战斗会话进入终态（对局已结束/入局被拒等）时调用——收尾窗口内连接仍要可读
    //（继续收尾随推送与补投的结算通知），但已发出的请求不会再有结果，
    // 让调用方等回执/等到超时都没有意义（对齐 TS 的 failPending(terminalStatus)）。
    internal void SettlePending(Exception cause)
    {
        FailAllInflight(cause);
        FailAllQueued(cause);
    }

    // RemoveInflight 按 (key, 实例) 摘除在途项：仅当表内仍是同一实例时摘除（返回 true），
    // 否则返回 false（已被响应或断连认领）——超时路径据此判定结算权。
    private bool RemoveInflight(InflightKey key, Inflight inflight)
    {
        lock (_gate)
        {
            if (_inflight.TryGetValue(key, out var current) && ReferenceEquals(current, inflight))
            {
                _inflight.Remove(key);
                return true;
            }
            return false;
        }
    }

    // RemoveInflightLocked 按实例反查 key 并摘除（调用方持 _gate）。
    private void RemoveInflightLocked(Inflight inflight)
    {
        InflightKey? found = null;
        foreach (var pair in _inflight)
        {
            if (ReferenceEquals(pair.Value, inflight))
            {
                found = pair.Key;
                break;
            }
        }
        if (found.HasValue)
        {
            _inflight.Remove(found.Value);
        }
    }
}
