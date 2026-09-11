"""记忆宫殿解析工具 —— JSON 解析、时间/日期处理、事件链接与纠正。

从 main.py 拆出的纯解析逻辑，不依赖热更新配置、不碰数据库。
处理模型返回的 JSON、事件链接、时间模糊引用、tag 合并等。
"""

import re
import json
import os
from datetime import datetime, timedelta, timezone


# 时区偏移（小时），用于记忆注入时的日期显示，默认 UTC+8
TIMEZONE_HOURS = int(os.getenv("TIMEZONE_HOURS", "8"))


_MEMORY_PALACE_ROOM_LABELS = {
    "living_room": "客厅",
    "bedroom": "卧室",
    "study": "书房",
    "user_room": "用户房间",
    "self_room": "自我房间",
    "attic": "阁楼",
    "windowsill": "窗台",
}


def _memory_palace_parse_args(arg: str):
    limit = None
    room = None
    arg = (arg or "").strip()
    if not arg:
        return limit, room
    if arg.isdigit():
        return int(arg), room
    if arg in _MEMORY_PALACE_ROOM_LABELS:
        return limit, arg
    parts = [p.strip() for p in arg.split(",") if p.strip()]
    for part in parts:
        if "=" not in part:
            if part.isdigit():
                limit = int(part)
            elif part in _MEMORY_PALACE_ROOM_LABELS:
                room = part
            continue
        key, value = [x.strip() for x in part.split("=", 1)]
        if key == "limit" and value.isdigit():
            limit = int(value)
        elif key == "room" and value in _MEMORY_PALACE_ROOM_LABELS:
            room = value
    return limit, room


def _memory_palace_month_range(year: int, month: int):
    start = datetime(year, month, 1).date()
    if month == 12:
        end = datetime(year + 1, 1, 1).date()
    else:
        end = datetime(year, month + 1, 1).date()
    return start, end


def _memory_palace_resolve_fuzzy_date_references(text: str):
    """只解析模糊时间词，不解析具体数字日期，避免系统时间戳每轮误触发。"""
    text = text or ""
    today = datetime.now(timezone(timedelta(hours=TIMEZONE_HOURS))).date()
    ranges = []
    seen = set()

    def add(label, start, end):
        if not start or not end or start >= end:
            return
        key = (label, start.isoformat(), end.isoformat())
        if key in seen:
            return
        seen.add(key)
        ranges.append({"label": label, "start": start, "end": end})

    if "今天" in text:
        add("今天", today, today + timedelta(days=1))
    if "昨天" in text:
        d = today - timedelta(days=1)
        add("昨天", d, d + timedelta(days=1))
    if "前天" in text:
        d = today - timedelta(days=2)
        add("前天", d, d + timedelta(days=1))
    if "这周" in text or "本周" in text:
        start = today - timedelta(days=today.weekday())
        add("本周", start, start + timedelta(days=7))
    if "上周" in text:
        start = today - timedelta(days=today.weekday() + 7)
        add("上周", start, start + timedelta(days=7))
    if "这个月" in text or "本月" in text:
        add("本月", *_memory_palace_month_range(today.year, today.month))
    if "上个月" in text:
        y, m = today.year, today.month - 1
        if m == 0:
            y, m = y - 1, 12
        add("上个月", *_memory_palace_month_range(y, m))
    if "今年" in text:
        add("今年", datetime(today.year, 1, 1).date(), datetime(today.year + 1, 1, 1).date())
    if "去年" in text:
        add("去年", datetime(today.year - 1, 1, 1).date(), datetime(today.year, 1, 1).date())
    if "最近" in text or "近期" in text:
        add("近期", today - timedelta(days=14), today + timedelta(days=1))
    return ranges


_MEMORY_PALACE_ALLOWED_ROOMS = {"living_room", "bedroom", "study", "user_room", "self_room", "attic", "windowsill"}


_MEMORY_PALACE_ALLOWED_MOODS = {
    "neutral", "happy", "sad", "angry", "anxious", "calm", "excited",
    "tender", "nostalgic", "confused", "hopeful", "hurt", "peaceful", "grateful"
}


def safe_parse_memory_palace_json_array(text: str) -> list:
    """稳健解析提取模型输出的 JSON 数组。失败返回空数组，不影响主流程。"""
    if not text:
        return []
    raw = str(text).strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
    raw = re.sub(r"\s*```$", "", raw)
    start = raw.find("[")
    end = raw.rfind("]")
    if start < 0 or end < start:
        return []
    raw = raw[start:end + 1]
    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"⚠️ 记忆宫殿提取 JSON 解析失败: {e}; raw={raw[:500]}")
        return []
    return data if isinstance(data, list) else []


def safe_parse_digest_actions_json(text: str) -> list:
    """Loosely parse cognitive digestion output.

    Accepts JSON array, {"actions":[...]}, single action object, and fenced JSON.
    Returns [] on failure.
    """
    if not text:
        return []
    raw = str(text).strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
    raw = re.sub(r"\s*```$", "", raw)

    def _normalize(obj):
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            for key in ("actions", "items", "results", "data"):
                val = obj.get(key)
                if isinstance(val, list):
                    return val
            if obj.get("id") and obj.get("action"):
                return [obj]
        return []

    candidates = [raw]
    a0, a1 = raw.find("["), raw.rfind("]")
    if a0 >= 0 and a1 > a0:
        candidates.append(raw[a0:a1 + 1])
    o0, o1 = raw.find("{"), raw.rfind("}")
    if o0 >= 0 and o1 > o0:
        candidates.append(raw[o0:o1 + 1])

    seen = set()
    for cand in candidates:
        cand = cand.strip()
        if not cand or cand in seen:
            continue
        seen.add(cand)
        try:
            parsed = json.loads(cand)
        except Exception:
            continue
        normalized = _normalize(parsed)
        if normalized:
            return normalized
    return []


def _memory_palace_float_or_none(v):
    if v is None or v == "":
        return None
    try:
        return max(-1.0, min(1.0, float(v)))
    except Exception:
        return None


def _normalize_memory_palace_item(item: dict) -> dict:
    if not isinstance(item, dict):
        return {}
    content = str(item.get("content") or "").strip()
    if not content:
        return {}
    room = str(item.get("room") or "living_room").strip()
    if room not in _MEMORY_PALACE_ALLOWED_ROOMS:
        room = "living_room"
    mood = str(item.get("mood") or "neutral").strip()
    if mood not in _MEMORY_PALACE_ALLOWED_MOODS:
        mood = "neutral"
    try:
        importance = int(item.get("importance") or 5)
    except Exception:
        importance = 5
    importance = max(1, min(10, importance))
    tags = item.get("tags") or ""
    if isinstance(tags, list):
        tags = "、".join(str(t).strip() for t in tags if str(t).strip())
    else:
        tags = str(tags or "").strip()

    # 便利贴只认 pinDays。
    # pinDays=0/空/缺失 时必须清空 pinned_until；不能把 date 或模型误输出的 pinned_until 当成便利贴。
    # 到期时间按该条记忆的 date 计算：pinned_until = date + pinDays，而不是按入库时间计算。
    raw_pin_days = item.get("pinDays", item.get("pin_days", 0))
    try:
        pin_days = int(float(str(raw_pin_days).strip() or "0"))
    except Exception:
        pin_days = 0
    pin_days = max(0, min(pin_days, 30))
    memory_date_text = str(item.get("date") or "").strip()
    pinned_until = None
    if pin_days > 0:
        try:
            base_date = datetime.strptime(memory_date_text[:10], "%Y-%m-%d").date() if memory_date_text else datetime.now(timezone.utc).date()
        except Exception:
            base_date = datetime.now(timezone.utc).date()
        pinned_until = datetime.combine(base_date + timedelta(days=pin_days), datetime.min.time(), tzinfo=timezone.utc)
    return {
        "content": content,
        "room": room,
        "tags": tags,
        "importance": importance,
        "mood": mood,
        "valence": _memory_palace_float_or_none(item.get("valence")),
        "arousal": _memory_palace_float_or_none(item.get("arousal")),
        "pinned_until": pinned_until,
        "date": str(item.get("date") or "").strip(),
        "relatedTo": item.get("relatedTo"),
        "sameAs": item.get("sameAs"),
        "eventName": item.get("eventName"),
        "eventTags": item.get("eventTags"),
    }


def parse_memory_palace_unpin_ids(raw_items: list, pinned_refs: list) -> list:
    """解析模型输出的 {"unpin": "P0"}，映射为真实 memory id。"""
    if not raw_items or not pinned_refs:
        return []
    result = []
    seen = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        raw = item.get("unpin")
        if not isinstance(raw, str):
            continue
        m = re.match(r"^\s*P(\d+)\s*$", raw, flags=re.I)
        if not m:
            continue
        idx = int(m.group(1))
        if 0 <= idx < len(pinned_refs):
            node_id = pinned_refs[idx]["id"]
            if node_id not in seen:
                seen.add(node_id)
                result.append(node_id)
    return result


def _memory_palace_clean_query_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip())


def _memory_palace_sample_evenly(items: list, max_items: int) -> list:
    if len(items) <= max_items:
        return items
    step = len(items) / max_items
    return [items[int(i * step)] for i in range(max_items)]


def split_memory_palace_extraction_snippets(messages_text: str = "", source_messages: list = None, max_snippets: int = 25) -> list:
    """提取 relatedTo 候选用 query：优先每条用户消息，兜底按文本分段。"""
    snippets = []
    seen = set()
    if source_messages:
        for row in source_messages:
            try:
                role = row.get("role") if hasattr(row, "get") else row["role"]
                content = row.get("content") if hasattr(row, "get") else row["content"]
            except Exception:
                continue
            if role != "user":
                continue
            text = _memory_palace_clean_query_text(content)
            if len(re.sub(r"\W+", "", text, flags=re.UNICODE)) < 4:
                continue
            if text not in seen:
                seen.add(text)
                snippets.append(text[:300])
    if not snippets:
        text = str(messages_text or "").strip()
        parts = [p.strip() for p in re.split(r"\n{2,}|(?<=[。！？!?])\s+", text) if p.strip()]
        if len(parts) <= 1 and text:
            size = 300
            parts = [text[i:i + size] for i in range(0, len(text), size)]
        for part in parts:
            cleaned = _memory_palace_clean_query_text(part)
            if len(cleaned) < 8 or cleaned in seen:
                continue
            seen.add(cleaned)
            snippets.append(cleaned[:300])
    return _memory_palace_sample_evenly(snippets, max(1, int(max_snippets or 25)))


def _memory_palace_strip_ref_internals(refs: list) -> list:
    """去掉内部排序字段，保留 id / room / content 和来源标记。

    source 不带下划线前缀，因为 prompt 要用它把「当时真的想起过」和
    「系统补充的」分段展示。其它消费方（event links / corrections）只读
    id / room / content，多一个键无影响。
    """
    cleaned = []
    for ref in refs or []:
        item = {k: v for k, v in ref.items() if not k.startswith("_")}
        source = ref.get("_source") or ref.get("source")
        if source:
            item["source"] = source
        if item.get("content"):
            cleaned.append(item)
    return cleaned


def _memory_palace_item_related_ref_ids(item: dict, fallback_refs: list) -> list:
    """取这一条记忆当初看到的 O 编号表（O0 = 返回列表的第 0 项）。

    预览链路里每个 item 都带着自己那一组的 related_ref_ids（有序全量）。
    必须优先用它，不能用调用方合并出来的那份：一次预览可以同时勾选多个对话线，
    每个对话线有各自独立的 O0..On 编号，合并去重之后靠后那组的编号会整体错位，
    relatedTo 就会指向别的记忆。没带这个字段的（自动提取 / 文本提取）
    模型输出和 related_refs 本来就是同一次调用里的，直接用 fallback。
    """
    ids = item.get("related_ref_ids") if isinstance(item, dict) else None
    if isinstance(ids, list) and ids:
        return [str(x).strip() for x in ids if str(x or "").strip()]
    return [str((r or {}).get("id") or "").strip() for r in (fallback_refs or [])]


def parse_memory_palace_event_links(raw_items: list, created_nodes: list, related_refs: list) -> tuple:
    """解析 relatedTo/sameAs/eventName/eventTags，返回 (links, hints)。

    sameAs 写的是「模型本次输出里的第几条」。预览导入允许用户取消勾选，
    剩下的条目位置会整体前移，再按位置去认这个编号，就会把事件盒关联
    绑到别的记忆上（从被取消那条开始全部错位一格）。所以只要条目带了
    raw_index（预览链路会带上模型原始序号），就按 raw_index 认；
    没带的（自动/文本提取，模型输出和创建节点一一对应）仍按顺序认。
    """
    links = []
    hints = {}
    if not raw_items or not created_nodes:
        return links, hints

    # 第一步：把「模型输出序号 -> 实际创建的节点」定下来。
    # 位置仍用来找对应的节点（created_nodes 按同一顺序生成），
    # 但 sameAs 解析只认序号，不认位置。
    pairs = []
    node_by_raw_index = {}
    position = 0
    for item in raw_items:
        if not isinstance(item, dict) or not item.get("content") or not item.get("room"):
            continue
        if position >= len(created_nodes):
            break
        raw_index = item.get("raw_index")
        if raw_index is None:
            own_idx = position
        else:
            try:
                own_idx = int(raw_index)
            except Exception:
                position += 1
                continue
        new_id = created_nodes[position]["id"]
        pairs.append((own_idx, item, new_id))
        node_by_raw_index[own_idx] = new_id
        position += 1

    # 第二步：解析关联。
    for own_idx, item, new_id in pairs:
        has_link = False
        # O 编号按「这一条当初看到的那份列表」翻译，不用跨组合并后的列表。
        ref_ids = _memory_palace_item_related_ref_ids(item, related_refs)
        rels = item.get("relatedTo")
        if isinstance(rels, str):
            rels = [rels]
        if isinstance(rels, list):
            for ref in rels:
                m = re.match(r"^\s*O(\d+)\s*$", str(ref), flags=re.I)
                if m:
                    idx = int(m.group(1))
                    if 0 <= idx < len(ref_ids):
                        target_id = ref_ids[idx]
                        if target_id and target_id != new_id:
                            links.append({"newMemoryId": new_id, "existingMemoryId": target_id})
                            has_link = True
        same = item.get("sameAs")
        if isinstance(same, str):
            same = [same]
        if isinstance(same, list):
            for ref in same:
                m = re.match(r"^\s*N?(\d+)\s*$", str(ref), flags=re.I)
                if not m:
                    continue
                idx = int(m.group(1))
                # 只认指向前面条目的编号，避免自指和向后指。
                if idx < 0 or idx >= own_idx:
                    continue
                target_id = node_by_raw_index.get(idx)
                # 取不到说明它指的那条这次没导入（被取消勾选），
                # 这条就不该因此单独开一个盒。
                if not target_id or target_id == new_id:
                    continue
                links.append({"newMemoryId": new_id, "existingMemoryId": target_id})
                has_link = True
        if has_link:
            tags = item.get("eventTags") or []
            if isinstance(tags, str):
                tags = [t.strip() for t in re.split(r"[,，、/\s]+", tags) if t.strip()]
            hints[new_id] = {
                "eventName": str(item.get("eventName") or "").strip(),
                "eventTags": [str(t).strip() for t in tags if str(t).strip()][:8],
            }
    return links, hints


def parse_memory_palace_corrections(raw_items: list, related_refs: list) -> list:
    """解析模型输出的 {"correct":"O0","note":"..."}，映射到真实旧记忆 id。"""
    if not raw_items or not related_refs:
        return []
    corrections = []
    seen = set()
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        raw = item.get("correct")
        note = str(item.get("note") or "").strip()
        if not isinstance(raw, str) or not note:
            continue
        m = re.match(r"^\s*O(\d+)\s*$", raw, flags=re.I)
        if not m:
            continue
        idx = int(m.group(1))
        if 0 <= idx < len(related_refs):
            target_id = related_refs[idx].get("id")
            key = (target_id, note)
            if target_id and key not in seen:
                seen.add(key)
                corrections.append({"targetId": target_id, "note": note})
    return corrections


def serialize_memory_palace_correction_previews(corrections: list, related_refs: list, session_id: str = None, group_index: int = None, source_message_ids: list = None) -> list:
    items = []
    by_id = {str(r.get("id")): r for r in (related_refs or []) if r.get("id")}
    for corr in corrections or []:
        target_id = str(corr.get("targetId") or "").strip()
        note = str(corr.get("note") or "").strip()
        if not target_id or not note:
            continue
        ref = by_id.get(target_id) or {}
        items.append({
            "type": "correct",
            "target_id": target_id,
            "note": note,
            "content": ref.get("content") or target_id,
            "session_id": session_id,
            "group_index": group_index,
            "source_message_ids": [int(x) for x in (source_message_ids or []) if str(x).isdigit()],
        })
    return items


def _merge_text_tags(*values) -> str:
    seen = []
    for value in values:
        parts = value if isinstance(value, list) else re.split(r"[,，、/\s]+", str(value or ""))
        for part in parts:
            p = str(part).strip()
            if p and p not in seen:
                seen.append(p)
    return "、".join(seen[:12])


def _memory_palace_split_event_links_by_target(event_links: list, created_nodes: list) -> tuple:
    """按目标把 link 分成「指向旧记忆」和「指向本批新记忆」两组。

    parse_memory_palace_event_links 把 relatedTo 和 sameAs 解析成同一种
    {newMemoryId, existingMemoryId} 结构，区别只在 existingMemoryId 是不是
    本批刚创建的节点：是就来自 sameAs，不是就来自 relatedTo。

    自动提取只采纳 relatedTo：它是往既有记忆上挂，盒子数量受历史记忆约束；
    sameAs 是批内两条新记忆互相配对，一次提取就能凭空开新盒，涨得太快，
    压缩（活节点 >= 4 就调一次 LLM）会跟着频繁触发。

    返回 (to_existing, to_new_batch)。
    """
    new_ids = {str((n or {}).get("id") or "") for n in (created_nodes or [])}
    new_ids.discard("")
    to_existing = []
    to_new_batch = []
    for link in event_links or []:
        target = str((link or {}).get("existingMemoryId") or "").strip()
        if not target:
            continue
        (to_new_batch if target in new_ids else to_existing).append(link)
    return to_existing, to_new_batch


def _memory_palace_event_link_shadow_report(
    event_links: list,
    event_hints: dict,
    created_nodes: list,
    related_refs: list,
) -> list:
    """把解析出的事件盒关联渲染成人能读的日志行。

    列出每条新记忆挂到了哪里、盒名是什么，便于事后核对模型判断的质量。
    渲染的是「解析结果」而不是「绑定结果」：调用方可能只采纳其中一部分
    （自动提取默认只采纳 relatedTo），跳过的条数由调用方另行记录。
    返回若干行文本，调用方逐行写日志。
    """
    if not event_links:
        return []

    def _brief(text, size=24):
        t = re.sub(r"\s+", " ", str(text or "")).strip()
        return (t[:size] + "…") if len(t) > size else t

    new_by_id = {}
    for idx, node in enumerate(created_nodes or []):
        nid = str((node or {}).get("id") or "")
        if nid:
            new_by_id[nid] = {"pos": idx, "content": (node or {}).get("content") or ""}
    old_by_id = {}
    for idx, ref in enumerate(related_refs or []):
        rid = str((ref or {}).get("id") or "")
        if rid:
            old_by_id[rid] = {"pos": idx, "content": (ref or {}).get("content") or ""}

    # 按新记忆分组，一条新记忆一行，和 bind 的分组口径一致。
    grouped = {}
    order = []
    for link in event_links:
        new_id = str((link or {}).get("newMemoryId") or "")
        existing_id = str((link or {}).get("existingMemoryId") or "")
        if not new_id or not existing_id:
            continue
        if new_id not in grouped:
            grouped[new_id] = []
            order.append(new_id)
        if existing_id not in grouped[new_id]:
            grouped[new_id].append(existing_id)

    lines = []
    to_old = 0
    to_new = 0
    for new_id in order:
        targets = []
        for existing_id in grouped[new_id]:
            if existing_id in old_by_id:
                to_old += 1
                info = old_by_id[existing_id]
                targets.append(f"relatedTo O{info['pos']}「{_brief(info['content'])}」")
            elif existing_id in new_by_id:
                to_new += 1
                info = new_by_id[existing_id]
                targets.append(f"sameAs #{info['pos']}「{_brief(info['content'])}」")
            else:
                targets.append(f"未知目标 {existing_id}")
        hint = event_hints.get(new_id) or {}
        name = str(hint.get("eventName") or "").strip() or "（模型没给盒名）"
        tags = "、".join(str(t) for t in (hint.get("eventTags") or []))
        self_brief = _brief((new_by_id.get(new_id) or {}).get("content"))
        lines.append(
            f"   · 新记忆「{self_brief}」→ " + " + ".join(targets)
            + f"｜盒名「{name}」" + (f"｜标签 {tags}" if tags else "")
        )

    header = (
        f"📦 事件盒关联解析：{len(event_links)} 条关联"
        f"（relatedTo→旧记忆 {to_old} / sameAs→本批新记忆 {to_new}）"
        f"，涉及 {len(order)} 条新记忆，带盒名 {len(event_hints or {})} 条。"
    )
    return [header] + lines


def memory_palace_summary_has_reasoning_leak(content: str) -> bool:
    """检测模型把「怎么算字数 / 怎么压缩」的过程误塞进了整合回忆正文。

    典型污染长这样：Paragraph 1: 31 chars / Total: 280 / Still too long。
    JSON 合法、长度也没超限，但语义上根本不是回忆。主要兜 Gemini 之类
    把未标记的 reasoning 混进 content 的中转。

    只凭一个英文短语就拒会误伤真实对话（比如聊天里真的在讨论字数），
    所以要求至少命中两种信号；显式 think 标签可以直接判定。
    """
    text = str(content or "").strip()
    if not text:
        return False
    if re.search(r"<(?:think|thinking|thought)>", text, re.I):
        return True
    signals = [
        r"\bparagraph\s*\d+\s*:\s*\d+\s*(?:chars?|characters?)\b",
        r"\btotal\s*:\s*[\d\s+]+\s*(?:chars?|characters?)\b",
        r"\bstill\s+too\s+long\b",
        r"\bneed\s+to\s+get\s+under\s+\d+\b",
        r"\blet(?:'|\u2019)s\s+(?:count|condense|compress|shorten)\b",
        r"(?:^|\n)\s*(?:analysis|reasoning)\s*:",
    ]
    hits = sum(1 for p in signals if re.search(p, text, re.I))
    return hits >= 2


def recover_memory_palace_summary_fields(raw: str) -> dict:
    """字段级兜底解析：content 里含未转义的半角双引号时按 schema 逐个抠字段。

    中文输出高发——模型在正文里写了 "某某"，标准 json.loads 直接挂。
    提示词里已经要求用「」，但模型不总听。

    策略：content 抠到下一个顶层键（name/tags/room/importance/mood）出现之前
    为止，这样能正确跳过中间所有裸引号。任何关键字段失败就返回空 dict，
    让上层走原本的失败路径。
    """
    text = str(raw or "")
    if not text.strip():
        return {}
    text = re.sub(r"^```(?:json|JSON)?\s*\n?", "", text, flags=re.M)
    text = re.sub(r"\n?```\s*$", "", text, flags=re.M).strip()

    head = re.search(r'"content"\s*:\s*"', text)
    if not head:
        return {}
    value_start = head.end()
    tail = re.search(r'"\s*,\s*"(?:name|tags|room|importance|mood)"\s*:', text[value_start:])
    if not tail:
        return {}
    raw_content = text[value_start:value_start + tail.start()]

    # 还原 JSON 字符串转义。\\ 先用占位符暂存，避免 \" 被拆成 \ + \"。
    # 残留的裸 " 保留不动：走到这里说明模型就是塞了未转义引号，
    # 最终 summary 是普通字符串，留着无害。
    placeholder = "\u0001"
    content = (raw_content
               .replace("\\\\", placeholder)
               .replace("\\n", "\n")
               .replace("\\t", "\t")
               .replace("\\r", "\r")
               .replace('\\"', '"')
               .replace(placeholder, "\\"))
    if not content.strip():
        return {}

    out = {"content": content}
    for key in ("name", "room", "mood"):
        m = re.search(r'"%s"\s*:\s*"([^"]*)"' % key, text)
        if m:
            out[key] = m.group(1).strip()
    m = re.search(r'"importance"\s*:\s*(-?\d+(?:\.\d+)?)', text)
    if m:
        try:
            out["importance"] = int(float(m.group(1)))
        except Exception:
            pass
    m = re.search(r'"tags"\s*:\s*\[([^\]]*)\]', text)
    if m:
        tags = [t.strip().strip('"').strip("'") for t in m.group(1).split(",")]
        out["tags"] = [t for t in tags if t]
    return out


def _memory_palace_parse_summary_json(text: str) -> dict:
    try:
        data = json.loads(str(text or ""))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    parsed = safe_parse_memory_palace_json_array(text)
    if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
        return parsed[0]
    # 标准解析和数组解析都失败 → 按 schema 逐字段抠（content 内含裸引号的情况）
    recovered = recover_memory_palace_summary_fields(text)
    if recovered.get("content"):
        print("🗜️ 事件盒 summary JSON 解析失败，字段级兜底成功（疑似 content 内含未转义半角引号）")
        return recovered
    return {}
