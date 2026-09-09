"""工具链处理 —— tool_calls 规范化、ID 修复、孤儿消息清理。

从 main.py 拆出的工具调用处理逻辑，不依赖热更新配置、不碰数据库。
处理 Claude XML 格式、工具 ID 对齐、use_package 链保留等。
"""

import re
import json
import uuid


def _normalize_tool_chains_by_id(messages: list) -> list:
    """按 tool_call_id 把历史工具结果归位到对应 assistant(tool_calls) 后面；同 id 多次出现时按发生次数顺序消耗。"""
    if not messages:
        return messages

    tools_by_id = {}
    all_call_ids = set()
    for msg in messages:
        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            for tc in msg.get("tool_calls", []):
                if tc.get("id"):
                    all_call_ids.add(tc.get("id"))
        elif msg.get("role") == "tool" and msg.get("tool_call_id"):
            tools_by_id.setdefault(msg.get("tool_call_id"), []).append(msg)

    if not tools_by_id:
        return messages

    normalized = []
    emitted_tool_obj_ids = set()
    moved_tools = 0

    def _tool_obj_key(tool_msg):
        return id(tool_msg)

    for msg in messages:
        if msg.get("role") == "tool":
            tool_call_id = msg.get("tool_call_id")
            # 如果本批消息里存在对应 assistant(tool_calls)，tool 不在原位置输出；
            # 等遇到对应 assistant 时按发生次数消耗一条，避免同 id 多轮时只归位一次。
            if tool_call_id in all_call_ids:
                continue
            normalized.append(msg)
            continue

        normalized.append(msg)

        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            for tc in msg.get("tool_calls", []):
                call_id = tc.get("id")
                if not call_id:
                    continue
                queue = tools_by_id.get(call_id) or []
                while queue:
                    tool_msg = queue.pop(0)
                    key = _tool_obj_key(tool_msg)
                    if key in emitted_tool_obj_ids:
                        continue
                    normalized.append(tool_msg)
                    emitted_tool_obj_ids.add(key)
                    moved_tools += 1
                    break

    if moved_tools:
        print(f"🔧 分区模式: 按tool_call_id发生次数归位{moved_tools}条历史tool结果")
    return normalized


def _normalize_incoming_xml_tool_messages(messages: list) -> tuple:
    """只把入口尾部当前轮 XML 工具窗口转成 OpenAI 标准 tool_calls/tool 消息。"""
    if not isinstance(messages, list):
        return messages, 0

    def _is_xml_tool_msg(msg):
        if not isinstance(msg, dict):
            return False
        content = msg.get("content")
        if not isinstance(content, str):
            return False
        text = content.strip()
        return bool(re.match(r'^<tool\s+name="[^"]+"\s*>', text) or re.match(r'^<tool_result[\w-]*\s+[^>]*>', text))

    # 只处理尾部当前工具窗口。
    # 兼容尾部最后一条是普通重复 user、倒数第二条才是 XML tool_result 的请求。
    end_idx = len(messages)
    if end_idx > 0:
        last = messages[end_idx - 1]
        if isinstance(last, dict) and last.get("role") == "user" and not _is_xml_tool_msg(last):
            prev = messages[end_idx - 2] if end_idx >= 2 else None
            if _is_xml_tool_msg(prev):
                end_idx -= 1

    start_idx = end_idx
    while start_idx > 0 and _is_xml_tool_msg(messages[start_idx - 1]):
        start_idx -= 1

    if start_idx == end_idx:
        return messages, 0

    prefix = list(messages[:start_idx])
    window = messages[start_idx:end_idx]
    suffix = list(messages[end_idx:])

    normalized = []
    converted = 0
    pending_xml_call_indexes = []

    def _set_pending_call_id(call_index: int, call_id: str):
        try:
            calls = normalized[call_index].get("tool_calls") or []
            if calls:
                calls[0]["id"] = call_id
        except Exception:
            pass

    for local_idx, msg in enumerate(window):
        content = msg.get("content")
        text = content.strip()

        call_open = re.match(r'^<tool\s+name="([^"]+)"\s*>', text)
        call_close = "<" + "/tool>"
        if call_open and text.endswith(call_close):
            body_text = text[call_open.end(): -len(call_close)]
            params = {}
            param_close = "<" + "/param>"
            param_re = re.compile(r'<param\s+name="([^"]+)"\s*>([\s\S]*?)' + re.escape(param_close))
            for pm in param_re.finditer(body_text or ""):
                params[pm.group(1)] = pm.group(2) or ""
            call_id = "xml_tool_pending_" + re.sub(r'[^\w-]', "_", str(msg.get("id") or msg.get("created_at") or (start_idx + local_idx)))
            m = dict(msg)
            m["role"] = "assistant"
            m["content"] = None
            m["tool_calls"] = [{
                "id": call_id,
                "type": "function",
                "function": {
                    "name": call_open.group(1),
                    "arguments": json.dumps(params, ensure_ascii=False, indent=2)
                }
            }]
            m.pop("name", None)
            m.pop("tool_call_id", None)
            normalized.append(m)
            pending_xml_call_indexes.append(len(normalized) - 1)
            converted += 1
            continue

        result_open = re.match(r'^<tool_result([\w-]*)\s+([^>]*)>', text)
        if result_open:
            suffix_raw = result_open.group(1) or ""
            result_close = "<" + "/tool_result" + suffix_raw + ">"
            if text.endswith(result_close):
                attrs = dict(re.findall(r'([A-Za-z_][\w-]*)="([^"]*)"', result_open.group(2) or ""))
                body_text = text[result_open.end(): -len(result_close)]
                content_open = "<content>"
                content_close = "<" + "/content>"
                if body_text.startswith(content_open) and body_text.endswith(content_close):
                    body_text = body_text[len(content_open): -len(content_close)]
                result_id = attrs.get("tool_call_id") or attrs.get("id") or suffix_raw.lstrip("_") or ("xml_tool_result_" + re.sub(r'[^\w-]', "_", str(msg.get("id") or (start_idx + local_idx))))
                if pending_xml_call_indexes:
                    _set_pending_call_id(pending_xml_call_indexes.pop(0), result_id)
                m = dict(msg)
                m["role"] = "tool"
                m["content"] = body_text
                m["tool_call_id"] = result_id
                m["name"] = attrs.get("name") or "工具结果"
                normalized.append(m)
                converted += 1
                continue

        normalized.append(msg)

    return prefix + normalized + suffix, converted


def _log_tool_chain_snapshot(label: str, messages: list, session_id: str = "", enabled: bool = False, extra: str = "", log_fn=None):
    """向 Dashboard 输出工具链结构快照；只记录结构和短 head，不记录完整内容。"""
    if not enabled:
        return
    try:
        # 降噪：没有任何工具链信号时不打日志，避免普通对话刷屏。
        has_tool_signal = False
        for _m in messages or []:
            _content = _m.get("content")
            if _m.get("role") == "tool" or _m.get("tool_calls") or _m.get("tool_call_id"):
                has_tool_signal = True
                break
            if isinstance(_content, str) and _content.strip().startswith("<tool"):
                has_tool_signal = True
                break
        if not has_tool_signal:
            return

        lines = []
        for idx, msg in enumerate(messages or []):
            role = msg.get("role")
            content = msg.get("content")
            if isinstance(content, str):
                content_len = len(content)
                head = content.replace("\n", "\\n")[:24]
            elif content is None:
                content_len = 0
                head = ""
            else:
                content_len = len(str(content))
                head = str(content).replace("\n", "\\n")[:24]

            parts = [f"{idx}:{role}"]
            if msg.get("tool_calls"):
                ids = []
                names = []
                for tc in msg.get("tool_calls") or []:
                    ids.append(str(tc.get("id") or "?"))
                    fn = tc.get("function") or {}
                    names.append(str(fn.get("name") or tc.get("name") or "?"))
                parts.append("tc=[" + ",".join(ids[:6]) + "]")
                parts.append("fn=[" + ",".join(names[:6]) + "]")
            if msg.get("tool_call_id"):
                parts.append("id=" + str(msg.get("tool_call_id")))
            if msg.get("name"):
                parts.append("name=" + str(msg.get("name")))
            if isinstance(content, str):
                stripped = content.strip()
                if stripped.startswith("<tool_result"):
                    parts.append("xml_tool_result")
                elif stripped.startswith("<tool"):
                    parts.append("xml_tool")
            parts.append(f"len={content_len}")
            if head:
                parts.append(f'head="{head}"')
            lines.append(" ".join(parts))

        preview = "\n".join(lines[:20])
        if len(lines) > 20:
            preview += f"\n... ({len(lines)-20} more)"
        msg = f"🔧 tool_chain[{label}] n={len(messages or [])}" + (f" {extra}" if extra else "") + "\n" + preview
        try:
            if log_fn: log_fn("info", msg, category="chat", session_id=session_id)
        except Exception:
            print(msg)
    except Exception as e:
        try:
            if log_fn: log_fn("error", f"⚠️ tool_chain[{label}] 日志生成失败: {e}", category="chat", session_id=session_id)
        except Exception:
            print(f"⚠️ tool_chain[{label}] 日志生成失败: {e}")


def _repair_tool_call_ids_by_adjacency(messages: list, session_id: str = "", reason: str = "", log_fn=None) -> list:
    """
    修复同一条历史链里 assistant(tool_calls).id 与紧随其后的 tool.tool_call_id 不一致的问题。

    不靠字符串相似度；只按 OpenAI 工具协议的邻接关系修：
        assistant(tool_calls=[A])
        tool(tool_call_id=B)
    若 B 不属于 A 集合，则按顺序改成 A。
    """
    if not messages:
        return messages

    def _is_synthetic_xml_tool_id(value):
        return isinstance(value, str) and value.startswith("xml_tool")

    def _is_real_short_tool_id(value):
        return isinstance(value, str) and value and not value.startswith("xml_tool")

    repaired = []
    pending_ids = []
    pending_set = set()
    repairs = []

    for msg in messages:
        m = dict(msg)

        if m.get("role") == "assistant" and m.get("tool_calls"):
            pending_ids = [tc.get("id") for tc in (m.get("tool_calls") or []) if tc.get("id")]
            pending_set = set(pending_ids)
            repaired.append(m)
            continue

        if m.get("role") == "tool":
            old_id = m.get("tool_call_id")
            if pending_ids:
                if old_id in pending_set:
                    if old_id in pending_ids:
                        pending_ids.remove(old_id)
                else:
                    new_id = pending_ids.pop(0)
                    if _is_real_short_tool_id(old_id) and _is_synthetic_xml_tool_id(new_id):
                        pending_ids.insert(0, new_id)
                    else:
                        m["tool_call_id"] = new_id
                        repairs.append(f"{old_id or 'MISSING'}->{new_id}")
                repaired.append(m)
                continue

            repaired.append(m)
            continue

        # assistant(tool_calls) 后如果不是 tool，说明这条链已经结束/不完整，停止邻接映射。
        pending_ids = []
        pending_set = set()
        repaired.append(m)

    if repairs:
        log_msg = f"🔧 tool_call_id邻接修复{f'({reason})' if reason else ''}: " + " | ".join(repairs[:20])
        try:
            if log_fn: log_fn("info", log_msg, category="chat", session_id=session_id)
        except Exception:
            print(log_msg)

    return repaired


def _map_tool_ids_to_db_pending(db_msgs: list, tool_messages: list) -> dict:
    """
    保存 tool 结果前，把客户端 tool_call_id 映射回 DB 中仍未满足的 assistant(tool_calls).id。
    关键点：同一个 tool_call_id 在一次会话里可能重复出现，不能用全局 saved_tool_ids 判定已满足；
    必须按历史顺序把 tool 结果消耗到 assistant(tool_calls) 的发生次数上。
    """
    if not db_msgs or not tool_messages:
        return {}

    pending_ids = []
    for m in db_msgs:
        if m.get("role") == "assistant" and m.get("tool_calls"):
            for tc in (m.get("tool_calls") or []):
                cid = tc.get("id")
                if cid:
                    pending_ids.append(cid)
            continue
        if m.get("role") == "tool" and m.get("tool_call_id"):
            tid = m.get("tool_call_id")
            # 按发生顺序消耗一条对应 pending；同 id 多次出现时只消耗其中一次。
            if tid in pending_ids:
                pending_ids.remove(tid)
            elif pending_ids:
                # 历史里有错配/短 id 映射过来的 tool 时，也消耗最早 pending，保持 occurrence 对齐。
                pending_ids.pop(0)

    if not pending_ids:
        return {}

    mapping = {}
    for tm in tool_messages:
        cid = tm.get("tool_call_id")
        if not cid or not pending_ids:
            continue
        if cid in pending_ids:
            mapping[cid] = cid
            pending_ids.remove(cid)
        else:
            mapping[cid] = pending_ids.pop(0)
    return mapping


def _drop_orphan_tool_messages(messages: list) -> list:
    """
    清理会触发上游 tool_call_id 错误的消息，但不静默丢历史信息。
    完整 assistant(tool_calls)+tool 链按协议保留；不完整/孤立的历史工具信息降级成普通 assistant 文本。
    """
    cleaned = []
    pending_ast = None
    pending_tools = []
    pending_tool_ids = set()
    sanitized_tools = 0
    sanitized_ast = 0
    orphan_tools_by_id = {}

    def _tool_call_summary(ast: dict, tools: list) -> str:
        lines = []
        if ast and ast.get("tool_calls"):
            for tc in ast.get("tool_calls", []):
                fn = tc.get("function") or {}
                name = fn.get("name") or tc.get("name") or "unknown"
                args = fn.get("arguments") or tc.get("arguments") or ""
                lines.append(f"工具调用: {name}" + (f" 参数: {args}" if args else ""))
        for tool in tools:
            content = tool.get("content") or ""
            tool_call_id = tool.get("tool_call_id") or "unknown"
            lines.append(f"工具结果({tool_call_id}): {content}")
        return "\n".join(lines).strip() or " "

    def flush_pending():
        nonlocal pending_ast, pending_tools, pending_tool_ids, sanitized_ast
        if not pending_ast:
            return
        if pending_tool_ids:
            summary = _tool_call_summary(pending_ast, pending_tools)
            if summary:
                cleaned.append({"role": "assistant", "content": summary})
            sanitized_ast += 1
        else:
            cleaned.append(pending_ast)
            cleaned.extend(pending_tools)
        pending_ast = None
        pending_tools = []
        pending_tool_ids = set()

    for msg in messages or []:
        role = msg.get("role")

        if role == "assistant" and msg.get("tool_calls"):
            flush_pending()
            call_ids = {tc.get("id") for tc in msg.get("tool_calls", []) if tc.get("id")}
            matched_orphans = []
            for call_id in list(call_ids):
                matched_orphans.extend(orphan_tools_by_id.pop(call_id, []))
            if matched_orphans:
                # 找到对应工具结果时，保持 OpenAI 工具协议格式，不能降级成普通文本。
                cleaned.append(msg)
                cleaned.extend(matched_orphans)
                continue
            pending_ast = msg
            pending_tools = []
            pending_tool_ids = call_ids
            if not pending_tool_ids:
                cleaned.append({"role": "assistant", "content": _tool_call_summary(msg, [])})
                pending_ast = None
            continue

        if role == "tool":
            tool_call_id = msg.get("tool_call_id")
            if pending_ast and tool_call_id in pending_tool_ids:
                pending_tools.append(msg)
                pending_tool_ids.discard(tool_call_id)
                continue
            # 只有后面还存在对应 assistant(tool_calls) 时才暂存等待归组；
            # 否则原地降级，避免工具结果被统一追加到整段消息末尾、跑到新user下面。
            has_future_ast = False
            if tool_call_id:
                for future in messages[(messages.index(msg) + 1):]:
                    if future.get("role") == "assistant" and future.get("tool_calls"):
                        future_ids = {tc.get("id") for tc in future.get("tool_calls", []) if tc.get("id")}
                        if tool_call_id in future_ids:
                            has_future_ast = True
                            break
            if has_future_ast:
                orphan_tools_by_id.setdefault(tool_call_id or "unknown", []).append(msg)
            else:
                content = msg.get("content") or ""
                cleaned.append({"role": "assistant", "content": f"工具结果({tool_call_id or 'unknown'}): {content}"})
            sanitized_tools += 1
            continue

        flush_pending()
        cleaned.append(msg)

    flush_pending()
    for orphan_list in orphan_tools_by_id.values():
        for tool in orphan_list:
            content = tool.get("content") or ""
            tool_call_id = tool.get("tool_call_id") or "unknown"
            cleaned.append({"role": "assistant", "content": f"工具结果({tool_call_id}): {content}"})

    if sanitized_tools or sanitized_ast:
        print(f"🔧 分区模式: 发上游前降级不完整工具历史 assistant={sanitized_ast} tool={sanitized_tools}，保留内容并避免tool_call_id不匹配")
    return cleaned


def _extract_use_package_chains(messages: list) -> list:
    """Capture complete use_package chains by order inside an evicted A batch; IDs are not repaired here."""
    retained = []
    items = messages or []
    i = 0
    while i < len(items):
        msg = items[i]
        if msg.get("role") != "assistant" or not msg.get("tool_calls"):
            i += 1
            continue

        calls = list(msg.get("tool_calls") or [])
        package_positions = [
            pos for pos, tc in enumerate(calls)
            if (tc.get("function") or {}).get("name") == "use_package"
        ]
        if not package_positions:
            i += 1
            continue

        # OpenAI tool results follow the assistant call group in the same order.
        following_tools = []
        j = i + 1
        while j < len(items) and items[j].get("role") == "tool":
            following_tools.append(items[j])
            j += 1

        # Only retain use_package calls whose positional result is present.
        available_positions = [pos for pos in package_positions if pos < len(following_tools)]
        if available_positions:
            package_calls = [calls[pos] for pos in available_positions]
            package_tools = [following_tools[pos] for pos in available_positions]
            assistant = {k: v for k, v in msg.items() if k not in ("id", "created_at")}
            assistant["tool_calls"] = package_calls
            retained.append({
                "assistant": assistant,
                "tools": [
                    {k: v for k, v in tool.items() if k not in ("id", "created_at")}
                    for tool in package_tools
                ],
            })

        i = j if j > i + 1 else i + 1
    return retained


def _retained_use_package_name(chain: dict) -> str:
    """Read package_name from a retained use_package chain."""
    if not isinstance(chain, dict):
        return ""
    assistant = chain.get("assistant") or {}
    for tc in assistant.get("tool_calls") or []:
        fn = tc.get("function") or {}
        if fn.get("name") != "use_package":
            continue
        arguments = fn.get("arguments") or "{}"
        try:
            parsed = json.loads(arguments) if isinstance(arguments, str) else arguments
        except Exception:
            parsed = {}
        if isinstance(parsed, dict):
            package_name = str(parsed.get("package_name") or "").strip()
            if package_name:
                return package_name
    return ""


def _dedupe_retained_use_package_chains(chains: list) -> list:
    """Keep only the newest complete use_package chain for each package_name."""
    newest_by_package = {}
    newest_positions = {}
    unnamed = []
    for position, chain in enumerate(chains or []):
        if not isinstance(chain, dict):
            continue
        package_name = _retained_use_package_name(chain)
        if package_name:
            newest_by_package[package_name] = chain
            newest_positions[package_name] = position
        else:
            # Unknown argument shape cannot be safely considered the same package.
            unnamed.append((position, chain))
    ordered = [(newest_positions[name], chain) for name, chain in newest_by_package.items()]
    ordered.extend(unnamed)
    ordered.sort(key=lambda item: item[0])
    return [chain for _, chain in ordered]


def _retained_package_names(chains: list) -> list:
    """Compact package-name list for dashboard logs."""
    names = []
    for chain in chains or []:
        name = _retained_use_package_name(chain)
        if name:
            names.append(name)
    return names


def _flatten_retained_tool_chains(chains: list) -> list:
    messages = []
    for chain in _dedupe_retained_use_package_chains(chains):
        assistant = chain.get("assistant") if isinstance(chain, dict) else None
        tools = chain.get("tools") if isinstance(chain, dict) else None
        if assistant and isinstance(tools, list):
            messages.append(dict(assistant))
            messages.extend(dict(m) for m in tools if isinstance(m, dict))
    return messages


def _extract_xml_tool_calls_from_content(content: str):
    """If assistant content ends with XML tool call, extract and return (clean_content, tool_calls_list)."""
    if not content or not isinstance(content, str):
        return content, None
    tool_close_tag = "<" + "/tool>"
    match = re.search(r'<tool\\s+name="([^"]+)"\\s*>([\\s\\S]*?)' + re.escape(tool_close_tag) + r'\\s*$', content)
    if not match:
        return content, None
    tool_name = match.group(1)
    tool_body = match.group(2)
    clean_content = content[:match.start()].rstrip()
    params = {}
    param_close_tag = "<" + "/param>"
    for pm in re.finditer(r'<param\\s+name="([^"]+)"\\s*>([\\s\\S]*?)' + re.escape(param_close_tag), tool_body):
        params[pm.group(1)] = pm.group(2) or ""
    call_id = "xml_call_" + re.sub(r'[^\\w-]', "_", tool_name)[:20] + "_" + uuid.uuid4().hex[:6]
    tool_calls = [{
        "id": call_id,
        "type": "function",
        "function": {
            "name": tool_name,
            "arguments": json.dumps(params, ensure_ascii=False, indent=2)
        }
    }]
    return clean_content or None, tool_calls
