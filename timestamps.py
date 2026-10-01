"""时间戳注入与格式化 —— 从 main.py 拆出的时间处理逻辑。

不依赖热更新配置、不碰数据库。处理消息时间戳、间隔判断、友好格式化。
"""

import re
from datetime import datetime, timedelta, timezone

# TIMEZONE_HOURS 在 memory_palace_parsing 模块中定义（启动时从环境变量读取）
from memory_palace_parsing import TIMEZONE_HOURS

# TIMEZONE_HOURS 从 memory_palace_parsing 导入


def _memory_palace_friendly_date(row: dict) -> str:
    """记忆日期转成人话：今天 / 昨天 / 原始日期。

    「2026-08-02」这种写法要模型自己跟当前日期做减法才知道是多久以前，
    容易算错，也读不出「刚刚发生」的感觉。今天和昨天直接写字面意思。
    更早的仍给具体日期——那时候「几天前」反而不如日期精确。

    datetime 也是 date 的子类，所以要先判 datetime 再取 .date()，
    否则拿到的是带时分秒的对象，跟 today 相减会差一截。
    """
    raw = row.get("date") or row.get("created_at") or ""
    d = None
    if isinstance(raw, datetime):
        d = (raw.astimezone(timezone(timedelta(hours=TIMEZONE_HOURS)))
             if raw.tzinfo else raw).date()
    elif hasattr(raw, "toordinal") and hasattr(raw, "year"):
        d = raw
    else:
        raw_str = str(raw)[:10]
        try:
            d = datetime.strptime(raw_str, "%Y-%m-%d").date()
        except Exception:
            return raw_str
    try:
        today = (datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_HOURS)).date()
        diff = (today - d).days
        if diff == 0:
            return "今天"
        if diff == 1:
            return "昨天"
        return d.strftime("%Y-%m-%d")
    except Exception:
        return str(raw)[:10]


def _memory_palace_format_node_line(row: dict) -> str:
    date_text = _memory_palace_friendly_date(row)
    meta = f"{date_text}｜重要性:{row.get('importance') or 5}｜情绪:{row.get('mood') or 'neutral'}"
    content = str(row.get("content") or "").strip()
    return f"- {meta}\n  {content}"


def _memory_palace_indent(text: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line for line in str(text or "").splitlines())


def build_time_injection(history: list = None) -> str:
    """构建轻量时间注入。
    第一轮/跨天显示日期：[06-08 17:23]
    同一天内只显示时间：[17:23]
    """
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc + timedelta(hours=TIMEZONE_HOURS)
    show_date = True

    if history:
        # 找最近一条带 created_at 的历史消息，若与当前日期相同则省略日期
        for msg in reversed(history):
            t = msg.get('created_at')
            if not t:
                continue
            try:
                if isinstance(t, str):
                    prev_utc = datetime.fromisoformat(t.replace('Z', '+00:00'))
                else:
                    prev_utc = t
                if prev_utc.tzinfo is None:
                    prev_utc = prev_utc.replace(tzinfo=timezone.utc)
                prev_local = prev_utc.astimezone(timezone.utc) + timedelta(hours=TIMEZONE_HOURS)
                show_date = prev_local.date() != now_local.date()
                break
            except Exception:
                continue

    if show_date:
        return f"[{now_local.strftime('%m-%d %H:%M')}]"
    return f"[{now_local.strftime('%H:%M')}]"


def _format_hm_duration(text: str) -> str:
    return (text or "").strip().replace(" ", "")


def _to_local_dt(t):
    if not t:
        return None
    try:
        if isinstance(t, str):
            dt = datetime.fromisoformat(t.replace('Z', '+00:00'))
        else:
            dt = t
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc) + timedelta(hours=TIMEZONE_HOURS)
    except Exception:
        return None


def _shorten_client_timestamp(timestamp: str, history: list = None) -> str:
    """附件/proxy 自带时间戳：同一天只显示 [HH:MM]，跨天保留 [MM-DD HH:MM]。"""
    if not timestamp or not history:
        return timestamp
    try:
        if not (timestamp.startswith("[") and timestamp.endswith("]")):
            return timestamp
        inner = timestamp[1:-1]
        if len(inner) != 11 or inner[2] != "-" or inner[5] != " " or inner[8] != ":":
            return timestamp
        month = int(inner[0:2])
        day = int(inner[3:5])
        hm = inner[6:11]
        for msg in reversed(history):
            local_dt = _to_local_dt(msg.get('created_at'))
            if local_dt:
                if local_dt.month == month and local_dt.day == day:
                    return f"[{hm}]"
                return timestamp
    except Exception:
        return timestamp
    return timestamp


_TS_PREFIX_RE = re.compile(r"^\[[0-9]{2}(?:-[0-9]{2})? [0-9]{2}:[0-9]{2}\]|^\[[0-9]{2}:[0-9]{2}\]")


_WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


SPARSE_TS_GAP_MINUTES = 15      # 同天内间隔达到多少分钟才打戳


SPARSE_TS_LONG_GAP_HOURS = 6    # 超过多少小时，断点时长只取整小时


SPARSE_TS_CALIBRATE_MINUTES = 60   # 距上一个标记多久没有断点，就补一个校准戳


def _content_has_timestamp_prefix(content) -> bool:
    """判断 content 开头是否已带 Operit 附件写入的时间戳。"""
    if isinstance(content, str):
        return bool(_TS_PREFIX_RE.match(content))
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return bool(_TS_PREFIX_RE.match(block.get("text", "") or ""))
    return False


def _prepend_text_to_content(content, prefix: str):
    """把前缀插到 content 最前面；list content 落到第一个 text 块。"""
    if isinstance(content, str):
        return prefix + content
    if isinstance(content, list):
        new_blocks = list(content)
        for idx, block in enumerate(new_blocks):
            if isinstance(block, dict) and block.get("type") == "text":
                nb = dict(block)
                nb["text"] = prefix + (block.get("text", "") or "")
                new_blocks[idx] = nb
                return new_blocks
        new_blocks.insert(0, {"type": "text", "text": prefix.rstrip()})
        return new_blocks
    return content


def _format_gap_note(minutes: int) -> str:
    """把间隔分钟数转成人话；不足阈值返回空串。"""
    if minutes < SPARSE_TS_LONG_GAP_HOURS * 60:
        return ""
    days = minutes // 1440
    if days >= 1:
        return f"（距上次对话约 {days} 天）"
    hours = round(minutes / 60)
    return f"（距上次对话约 {hours} 小时）"


def _last_message_dt(*message_lists):
    """从若干消息列表里取最后一条带 created_at 的消息时间（本地时区）。"""
    for msgs in reversed([m for m in message_lists if m]):
        for msg in reversed(msgs):
            if not isinstance(msg, dict):
                continue
            dt = _to_local_dt(msg.get("created_at"))
            if dt:
                return dt
    return None


def _format_gap_duration(minutes: int) -> str:
    """间隔分钟数 → 断点戳里的时长文字。

    <1 小时写分钟；6 小时以内精确到分钟（「1小时20分钟」），
    再长就只取整小时——那时候差几十分钟已经不影响理解；超过一天换成天。
    """
    minutes = max(0, int(minutes))
    if minutes < 60:
        return f"{minutes}分钟"
    if minutes < 1440:
        h, m = divmod(minutes, 60)
        if m == 0:
            return f"{h}小时"
        if h >= SPARSE_TS_LONG_GAP_HOURS:
            return f"{round(minutes / 60)}小时"
        return f"{h}小时{m}分钟"
    d, rem = divmod(minutes, 1440)
    h = rem // 60
    if h and d < 3:
        return f"{d}天{h}小时"
    return f"{d}天"


def _format_stamp_time(dt, with_date: bool) -> str:
    """戳里的时间：跨天/首条带「月-日 星期」，否则只给时分。"""
    if with_date:
        return f"{dt.strftime('%m-%d')} {_WEEKDAY_CN[dt.weekday()]} {dt.strftime('%H:%M')}"
    return dt.strftime('%H:%M')


def _sparse_stamp_lines(local_dt, state: dict, has_prefix: bool = False) -> list:
    """稀疏时间戳的唯一判定点：决定这条消息前要加哪些标记行，并就地更新 state。

    三种标记：
      - 首条锚点：[07-29 周三 18:00]
      - 断点戳：相邻间隔 ≥ 15 分钟 → —— 30分钟后（18:30）——
                跨天时括号里带日期和星期
      - 校准戳：没有断点、但距上一个标记已 ≥ 60 分钟，或悄悄跨了天 → [19:00]
                只写时间不写间隔，用来防止锚点漂移

    has_prefix：消息正文已带 Operit 附件时间戳。此时不再重复写时间，
    断点只写「—— 30分钟后 ——」；附件戳本身也算一次锚点。

    只依赖「这条及之前」的消息，新消息不会回头改旧消息，A 区缓存前缀稳定。

    state 字段：prev_dt / last_date / first_seen / anchor_dt（最近一次标记的时间）
    """
    prev_dt = state.get("prev_dt")
    last_date = state.get("last_date")
    anchor_dt = state.get("anchor_dt")
    first = not state.get("first_seen")

    gap = None if prev_dt is None else max(0, int((local_dt - prev_dt).total_seconds() // 60))
    crossed_day = (not first) and last_date is not None and last_date != local_dt.date()

    lines = []
    anchored = has_prefix
    if first:
        if not has_prefix:
            lines.append(f"[{_format_stamp_time(local_dt, True)}]")
        anchored = True
    elif gap is not None and gap >= SPARSE_TS_GAP_MINUTES:
        dur = _format_gap_duration(gap)
        if has_prefix:
            lines.append(f"—— {dur}后 ——")
        else:
            lines.append(f"—— {dur}后（{_format_stamp_time(local_dt, crossed_day)}）——")
        anchored = True
    elif crossed_day or (
        anchor_dt is not None
        and (local_dt - anchor_dt).total_seconds() >= SPARSE_TS_CALIBRATE_MINUTES * 60
    ):
        if not has_prefix:
            lines.append(f"[{_format_stamp_time(local_dt, crossed_day)}]")
        anchored = True

    if anchored:
        state["anchor_dt"] = local_dt
    state["first_seen"] = True
    state["last_date"] = local_dt.date()
    state["prev_dt"] = local_dt
    return lines


def build_current_message_timestamp_prefix(prev_dt, content, anchor_dt=None) -> str:
    """给当前轮 user 消息算时间戳前缀（时间取现在），规则同 _sparse_stamp_lines。

    prev_dt：上一条消息时间；anchor_dt：历史里最近一次标记的时间（用于校准戳）。
    不传 anchor_dt 时只会出首条锚点/断点戳，不会出校准戳。
    """
    now_local = datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_HOURS)
    state = {
        "prev_dt": prev_dt,
        "last_date": prev_dt.date() if prev_dt else None,
        "first_seen": prev_dt is not None,
        "anchor_dt": anchor_dt,
    }
    lines = _sparse_stamp_lines(now_local, state, _content_has_timestamp_prefix(content))
    if not lines:
        return ""
    return "\n".join(lines) + "\n\n"


def _prepend_timestamp_to_user_messages(messages: list, sparse: bool = False,
                                        state: dict = None, return_state: bool = False):
    """给历史消息加时间戳。

    sparse=False（默认，兼容旧行为）：只给 user 消息打紧凑戳，每条都打。
    sparse=True：按 _sparse_stamp_lines 的规则稀疏打戳，user/assistant 都参与；
      标记与正文之间空一行；tool 不打戳但参与间隔计算。

    state / return_state：分区模式分两次调用（A 区、B 区），中间状态必须接上，
    否则 B 区首条会被当成整段对话的第一条，A→B 之间的间隔也算不出来。
    """
    st = dict(state or {})
    stamped = []
    for msg in messages:
        m = dict(msg)
        role = m.get("role")
        local_dt = _to_local_dt(m.get("created_at"))

        if not sparse:
            if role == "user" and local_dt:
                show_date = st.get("last_date") != local_dt.date()
                stamp = (f"[{local_dt.strftime('%m-%d %H:%M')}]" if show_date
                         else f"[{local_dt.strftime('%H:%M')}]")
                if not _content_has_timestamp_prefix(m.get("content")):
                    m["content"] = _prepend_text_to_content(m.get("content"), stamp)
                st["last_date"] = local_dt.date()
        elif role in ("user", "assistant") and local_dt:
            has_prefix = _content_has_timestamp_prefix(m.get("content"))
            lines = _sparse_stamp_lines(local_dt, st, has_prefix)
            if lines:
                m["content"] = _prepend_text_to_content(m.get("content"), "\n".join(lines) + "\n\n")
        elif local_dt:
            # tool 等其他 role 不打戳，但参与间隔计算
            st["prev_dt"] = local_dt

        m.pop("id", None)
        m.pop("created_at", None)
        stamped.append(m)
    if return_state:
        return stamped, st
    return stamped
