"""特点画像（测试中）：把对用户的理解写成几条「特点」，每条讲一个稳定的特点。

和旧画像（user_impressions，标签式）并行存在，互不影响：
- 存在 user_portraits 表；
- 提示词变量 {{user_portrait}}（旧的 {{user_impression}} 不变）。

材料按日期区间取，不放人设和记忆宫殿：
- 区间内的日印象；
- 区间内的聊天原文（脱水：用户消息保留原文，角色回复只留头 50 字 + 尾 40 字）；
- 可选附上当前旧画像的标签（旧画像已经提纯过记忆）。

生成分两步写在同一次输出里：先列「线索」（照实写的具体事情），再用线索写「特点」，
特点是在不同事情上反复出现的样子，按字面理解，不往深处挖动机、不往和角色的关系上套。
生成时用旁观者视角、省略主语；注入提示词时标题写成「我眼中的…」。
线索只在预览里显示，不保存。

更新时模型不重写整份画像，只输出改动（edit / merge / add / delete），
没提到的条目由代码原样保留。

本模块不 import main，main 启动时调用 configure_user_portrait() 注入依赖。
"""
import re
import json
import uuid
import asyncio
from datetime import datetime, timezone, timedelta, date

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from database import get_conversation_messages_by_date
from timestamps import _to_local_dt, _split_timestamp_prefix

router = APIRouter()

_DEPS = {}
_TABLE_READY = False
_LOCKS = {}

MIN_TRAITS = 3
MAX_TRAITS = 8
TITLE_MAX = 20
CONTENT_MAX = 500
MAX_CLUES = 40
ASSISTANT_KEEP = 100      # 角色回复不超过这个字数就原样保留
ASSISTANT_HEAD = 50
ASSISTANT_TAIL = 40
MAX_RANGE_DAYS = 366
PROMPT_MAX_CHARS = 120000  # 超过就让用户缩短日期范围，免得上游 400 或超时
TIME_MARK_GAP = timedelta(minutes=30)

_OLD_TAG_LABELS = {
    "core_values": "核心价值观", "likes": "喜好", "dislikes": "雷点/反感", "money_attitude": "金钱观",
    "aesthetic": "审美偏好", "decision_style": "决策风格", "knowledge_map": "知识地图",
    "thinking_pattern": "思维模式", "humor_style": "幽默偏好", "learning_style": "学习方式",
    "comfort_zone": "舒适区", "stress_signals": "压力信号", "emotional_triggers": "情绪触发点",
    "soothing_methods": "有效安抚方式", "expression_habit": "表达习惯", "life_rhythm": "作息节律",
    "current_focus": "近期关注", "social_pattern": "社交模式", "attitude_to_me": "对我的态度", "others": "其他",
}


def configure_user_portrait(**deps):
    """main 启动时注入依赖。需要的键见 _dep() 的调用处。"""
    _DEPS.update(deps)


def _dep(name):
    fn = _DEPS.get(name)
    if fn is None:
        raise RuntimeError(f"user_portrait 依赖未注入：{name}")
    return fn


def _now():
    return datetime.now(timezone.utc)


def _iso(v):
    if not v:
        return None
    try:
        return v.isoformat()
    except Exception:
        return str(v)


def _local_today() -> date:
    return _to_local_dt(_now()).date()


def _parse_date(v):
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    try:
        return datetime.strptime(str(v or "").strip()[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _new_trait_id():
    return f"tr_{int(_now().timestamp() * 1000)}_{uuid.uuid4().hex[:6]}"


def _clean_title(v) -> str:
    t = re.sub(r"\s+", " ", str(v or "")).strip()
    t = t.strip("「」『』【】[]《》\"'“”‘’").strip()
    return t[:TITLE_MAX]


def _clean_content(v) -> str:
    t = str(v or "").replace("\r\n", "\n").strip()
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t[:CONTENT_MAX]


def _lock(cid: str) -> asyncio.Lock:
    lk = _LOCKS.get(cid)
    if lk is None:
        lk = asyncio.Lock()
        _LOCKS[cid] = lk
    return lk


async def _ensure_table(conn):
    global _TABLE_READY
    if _TABLE_READY:
        return
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS user_portraits (
            character_id TEXT PRIMARY KEY,
            traits JSONB NOT NULL DEFAULT '[]'::jsonb,
            revision INTEGER NOT NULL DEFAULT 0,
            last_consumed_node_id TEXT,
            source_mode TEXT DEFAULT 'initial',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    await conn.execute("""
        ALTER TABLE user_portraits
        ADD COLUMN IF NOT EXISTS last_range_start DATE,
        ADD COLUMN IF NOT EXISTS last_range_end DATE
    """)
    _TABLE_READY = True


def _parse_traits(raw) -> list:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    out = []
    for t in (raw if isinstance(raw, list) else []):
        if isinstance(t, dict) and str(t.get("content") or "").strip():
            out.append({
                "id": str(t.get("id") or _new_trait_id()),
                "title": _clean_title(t.get("title")) or "未命名",
                "content": _clean_content(t.get("content")),
                "created_at": t.get("created_at"),
                "updated_at": t.get("updated_at"),
            })
    return out


def _date_text(v):
    d = _parse_date(v)
    return d.isoformat() if d else None


async def get_user_portrait(character_id: str = "default"):
    pool = await _dep("get_pool")()
    async with pool.acquire() as conn:
        await _ensure_table(conn)
        row = await conn.fetchrow("""
            SELECT character_id, traits, revision, source_mode, last_range_start, last_range_end, created_at, updated_at
            FROM user_portraits WHERE character_id = $1
        """, character_id)
    if not row:
        return None
    return {
        "character_id": row["character_id"],
        "traits": _parse_traits(row["traits"]),
        "revision": int(row["revision"] or 0),
        "source_mode": row["source_mode"],
        "last_range_start": _date_text(row["last_range_start"]),
        "last_range_end": _date_text(row["last_range_end"]),
        "created_at": _iso(row["created_at"]),
        "updated_at": _iso(row["updated_at"]),
    }


async def _save(conn, character_id: str, traits: list, source_mode: str, range_start=None, range_end=None, keep_range=True):
    now = _iso(_now())
    for t in traits:
        t["created_at"] = t.get("created_at") or now
        t.pop("clues", None)
    payload = json.dumps(traits, ensure_ascii=False)
    rs, re_ = _parse_date(range_start), _parse_date(range_end)
    if keep_range:
        row = await conn.fetchrow("""
            INSERT INTO user_portraits (character_id, traits, revision, source_mode, last_range_start, last_range_end, created_at, updated_at)
            VALUES ($1, $2::jsonb, 1, $3, $4, $5, NOW(), NOW())
            ON CONFLICT (character_id) DO UPDATE SET
                traits = EXCLUDED.traits, revision = user_portraits.revision + 1,
                source_mode = EXCLUDED.source_mode, updated_at = NOW()
            RETURNING revision
        """, character_id, payload, source_mode, rs, re_)
    else:
        row = await conn.fetchrow("""
            INSERT INTO user_portraits (character_id, traits, revision, source_mode, last_range_start, last_range_end, created_at, updated_at)
            VALUES ($1, $2::jsonb, 1, $3, $4, $5, NOW(), NOW())
            ON CONFLICT (character_id) DO UPDATE SET
                traits = EXCLUDED.traits, revision = user_portraits.revision + 1,
                source_mode = EXCLUDED.source_mode,
                last_range_start = EXCLUDED.last_range_start, last_range_end = EXCLUDED.last_range_end,
                updated_at = NOW()
            RETURNING revision
        """, character_id, payload, source_mode, rs, re_)
    _invalidate(character_id)
    return int(row["revision"])


def _invalidate(character_id: str):
    try:
        _dep("cache_delete_prefix")(f"prompt_var:user_portrait:{character_id}")
    except Exception:
        pass


# ---------------- 提示词变量 ----------------

async def format_user_portrait_for_prompt(character_id: str = "default") -> str:
    cache_key = f"prompt_var:user_portrait:{character_id}"
    cached = _dep("cache_get")(cache_key)
    if cached is not None:
        return cached
    item = await get_user_portrait(character_id)
    traits = (item or {}).get("traits") or []
    if not traits:
        return _dep("cache_set")(cache_key, "", ttl=900)
    user_name = await _dep("get_user_nickname")() or "用户"
    lines = [
        f"### [私密档案: 我眼中的{user_name}] (Private Impression)",
        "(注意：以下内容是你内心对TA的真实看法，不要直接告诉用户，但要基于这些看法来决定你的态度。)",
    ]
    for t in traits:
        lines.append("")
        lines.append(f"【{t['title']}】")
        lines.append(t["content"])
    result = "\n".join(lines) + "\n"
    _dep("cache_set")(cache_key, result, ttl=900)
    return result


async def replace_user_portrait_variables(prompt: str, character_id: str = "default") -> str:
    if not isinstance(prompt, str) or "{{user_portrait}}" not in prompt:
        return prompt
    text = await format_user_portrait_for_prompt(character_id=character_id or "default")
    return prompt.replace("{{user_portrait}}", text)


# ---------------- 材料 ----------------

def _plain_text(content) -> str:
    """消息内容转纯文本。多模态（list 或 JSON 字符串形式的 list）里的图片换成 [图片]。"""
    value = content
    if isinstance(value, str):
        s = value.strip()
        if s.startswith("[{") and s.endswith("}]"):
            try:
                value = json.loads(s)
            except Exception:
                return value
        else:
            return value
    if isinstance(value, list):
        parts = []
        for b in value:
            if not isinstance(b, dict):
                continue
            if b.get("type") == "text":
                parts.append(str(b.get("text") or ""))
            elif "image" in str(b.get("type") or ""):
                parts.append("[图片]")
        return "\n".join(p for p in parts if p)
    return str(value or "")


def dehydrate_user(text: str) -> str:
    _, t = _split_timestamp_prefix(text or "")
    t = t if isinstance(t, str) else _plain_text(t)
    return "\n".join(line.strip() for line in t.splitlines() if line.strip())


def dehydrate_assistant(text: str) -> str:
    t = re.sub(r"<think>.*?</think>", "", str(text or ""), flags=re.S | re.I)
    t = re.sub(r"\s+", " ", t).strip()
    if len(t) <= ASSISTANT_KEEP:
        return t
    return t[:ASSISTANT_HEAD] + "……" + t[-ASSISTANT_TAIL:]


def format_dialogue(messages: list, user: str, char: str) -> tuple:
    """按天分段；隔了 30 分钟以上才标一次时间。返回 (文本, {user,assistant} 条数, 有聊天的天数)。"""
    by_day = {}
    for m in messages or []:
        role = m.get("role")
        if role not in ("user", "assistant"):
            continue
        dt = _to_local_dt(m.get("created_at"))
        if not dt:
            continue
        raw = _plain_text(m.get("content"))
        text = dehydrate_user(raw) if role == "user" else dehydrate_assistant(raw)
        if not text:
            continue
        by_day.setdefault(dt.date(), []).append((dt, role, text))
    counts = {"user": 0, "assistant": 0}
    sections = []
    for day in sorted(by_day):
        lines = [f"===== {day.month}月{day.day}日 ====="]
        last = None
        for dt, role, text in sorted(by_day[day], key=lambda x: x[0]):
            if last is None or dt - last >= TIME_MARK_GAP:
                lines.append(f"[{dt.strftime('%H:%M')}]")
            last = dt
            lines.append(f"{user if role == 'user' else char}: {text}")
            counts[role] += 1
        sections.append("\n".join(lines))
    return "\n\n".join(sections), counts, len(by_day)


async def _daily_impressions_in_range(start: date, end: date) -> list:
    pool = await _dep("get_pool")()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT impression_date, summary, tags, mood
                FROM daily_impressions
                WHERE impression_date >= $1 AND impression_date <= $2
                ORDER BY impression_date ASC
            """, start, end)
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[UserPortrait] 读取日印象失败，忽略: {e}")
        return []


def format_daily(rows: list) -> str:
    lines = []
    for r in rows:
        d = _parse_date(r.get("impression_date"))
        if not d:
            continue
        meta = f"{d.month}月{d.day}日"
        if str(r.get("tags") or "").strip():
            meta += f"｜标签：{str(r['tags']).strip()}"
        if str(r.get("mood") or "").strip():
            meta += f"｜氛围：{str(r['mood']).strip()}"
        summary = str(r.get("summary") or "").strip()
        if summary:
            lines.append(f"- {meta}\n  {summary}")
    return "\n".join(lines)


def format_old_impression(imp: dict) -> str:
    """只取旧画像的标签，不取整体印象 / 近期状态 / 最近变化。"""
    tags = imp.get("tags") if isinstance(imp, dict) and isinstance(imp.get("tags"), dict) else {}
    lines = []
    for k, v in tags.items():
        label = _OLD_TAG_LABELS.get(k, k)
        if isinstance(v, list):
            items = [str(x).strip() for x in v if str(x or "").strip()]
            if items:
                lines.append(f"- {label}：" + "；".join(items))
        elif str(v or "").strip():
            lines.append(f"- {label}：{str(v).strip()}")
    return "\n".join(lines)


def _check_range(start, end):
    s, e = _parse_date(start), _parse_date(end)
    if not s or not e:
        raise ValueError("请选择开始和结束日期")
    if s > e:
        raise ValueError("开始日期不能晚于结束日期")
    if (e - s).days + 1 > MAX_RANGE_DAYS:
        raise ValueError(f"日期范围最多 {MAX_RANGE_DAYS} 天")
    return s, e


async def build_materials(character_id: str, start: date, end: date, include_old: bool) -> dict:
    user = await _dep("get_user_nickname")() or "用户"
    char = await _dep("get_character_name")() or "角色"
    daily_rows = await _daily_impressions_in_range(start, end)
    if start < end:
        messages = await get_conversation_messages_by_date(end, start_hour=0, start_date=start)
    else:
        messages = await get_conversation_messages_by_date(end, start_hour=0)
    dialogue, counts, chat_days = format_dialogue(messages, user, char)
    old_text = ""
    if include_old:
        try:
            old = await _dep("get_old_impression")(character_id)
            imp = (old or {}).get("impression") if old else None
            old_text = format_old_impression(imp) if isinstance(imp, dict) else ""
        except Exception as e:
            print(f"[UserPortrait] 读取旧画像失败，忽略: {e}")
    range_text = f"{start.isoformat()} ~ {end.isoformat()}"
    sections = []
    daily_text = format_daily(daily_rows)
    sections.append(f"【日印象（{range_text}）】\n" + (daily_text or "（这段时间没有日印象）"))
    sections.append(
        f"【聊天原文（{range_text}，已脱水）】\n"
        f"{user}的消息是原文；{char}的回复超过 {ASSISTANT_KEEP} 字的只保留开头和结尾，中间用「……」省略。\n\n"
        + (dialogue or "（这段时间没有聊天记录）")
    )
    if old_text:
        sections.append(
            "【旧画像（标签式，只作参考线索）】\n"
            "这是以前按标签整理的画像，可能有重复、过期或拆得很散的内容。可以从中找方向，"
            "但每条线索都要能在上面的日印象或聊天原文里找到对应的事。\n" + old_text
        )
    return {
        "user": user, "char": char,
        "range_start": start.isoformat(), "range_end": end.isoformat(),
        "material_text": "\n\n".join(sections),
        "stats": {
            "daily_count": len([r for r in daily_rows if str(r.get("summary") or "").strip()]),
            "chat_days": chat_days,
            "user_messages": counts["user"],
            "assistant_messages": counts["assistant"],
            "used_old_impression": bool(old_text),
        },
        "empty": not daily_text and not dialogue,
    }


# ---------------- 提示词 ----------------

def _clue_guide() -> str:
    return """第一步·线索：从材料里挑出能看出TA是什么样的人的具体事情，写进 clues，编号 C1、C2……每条写清是什么事、大概什么时候（≤60 字），照实写，不加解读。优先找这几类：
- 反复做的事、反复聊起的话题
- 让TA开心、兴奋、投入的事
- 擅长的事，做事和玩的方式
- 说话、开玩笑、表达情绪的方式
- 在意或坚持的东西，不喜欢、会躲开的东西
- 说的和做的明显对不上的地方（只在材料里很明显时才写）"""


def _trait_rules(m: dict) -> str:
    user, char = m["user"], m["char"]
    return f"""每条特点的要求：
1. 特点是在不同的事情上反复出现的样子：怎么做事、怎么玩、怎么说话、在意什么、喜欢什么。至少用 2 条来自不同事情的线索支撑，在 clues 里写出编号。只出现在一类事情上的算不上特点，不要写。
2. 先按字面理解。好玩就是好玩，不在乎就是不在乎。只有材料里有明确证据（TA自己说过，或前后矛盾很明显）时，才写表面之下的动机。
3. TA在聊天里做的事，默认就是TA本来的样子，不是做给{char}看的。不写TA需要{char}怎样、想让{char}怎样、{char}应该怎样，除非材料明确显示是冲着{char}来的。
4. 用平常描述人的说法。不用心理分析和诊断的词（合理化、防御、补偿、投射、创伤、不安全感、渴望被认可之类），不用带评判的词（不堪、窝囊、笨拙、包装、掩饰之类）。
5. 省略主语：不用「她」「他」「TA」「{user}」做主语，直接写。例如「她喜欢甜食」写成「喜欢甜食」，「她玩游戏不在乎输赢」写成「玩游戏不在乎输赢」。
6. title：4-10 字，说出这个特点本身，同样省略主语。
7. content：80-200 字。写这个样子是什么、在哪些事情上冒出来。提到经历时写出是什么事，不要只说「有一次」。
8. 只写稳定的样子，不写最近的状态和变化。
9. 一个特点只写一条。几条其实在说同一面时，合成一条。"""


def build_initial_prompt(m: dict) -> str:
    return f"""{m['material_text']}

【你要写什么】
你是一个旁观者。读完上面{m['user']}和{m['char']}的聊天和日印象，写下{m['user']}是一个什么样的人。用第三人称的旁观视角，按下面的规则省略主语。
不要把相似的材料归类精简一遍，也不要往深处挖动机。要找的是在不同事情上反复出现的样子。

{_clue_guide()}
写 15-30 条。

第二步·特点：用线索写出 {MIN_TRAITS}-{MAX_TRAITS} 条特点，写进 traits。第一条写最能概括TA的底色，后面每条讲不同的方面（比如做事、玩、说话、在意的事、和人相处），不要几条都在讲同一面。材料不够就少写，宁缺毋滥。

{_trait_rules(m)}

只输出 JSON 对象，不要 markdown，不要解释：
{{"clues":[{{"id":"C1","text":"……"}}],"traits":[{{"title":"……","content":"……","clues":["C1","C4"]}}]}}""".strip()


def build_update_prompt(m: dict, traits: list) -> str:
    cur = "\n\n".join(f"T{i}【{t['title']}】\n{t['content']}" for i, t in enumerate(traits, 1)) or "（空）"
    return f"""【当前画像】
{cur}

{m['material_text']}

【你要做什么】
你是一个旁观者。上面的当前画像写的是{m['user']}是一个什么样的人。根据 {m['range_start']} ~ {m['range_end']} 的新材料更新它。
不要把相似的材料归类精简一遍，也不要往深处挖动机。要找的是在不同事情上反复出现的样子。

{_clue_guide()}
写 5-20 条。

第二步·改动：对照当前画像，只在线索带来新证据时才改。可用的操作：
- edit：补充或修正某一条，按下面的规则重写整条，输出完整的 title 和 content。
- merge：几条其实在说同一面，合成一条。ids 写被合并的条目，输出合并后的 title 和 content。
- add：线索揭示了现有条目都没覆盖的一面。
- delete：某一条已经被新材料证明不成立。
没写到的条目会原样保留，不需要动的条目不要写。总数不超过 {MAX_TRAITS} 条，各条讲不同的方面。edit、merge、add 也要写 clues。

{_trait_rules(m)}

只输出 JSON 对象，不要 markdown，不要解释。每个操作写一句 reason：
{{"clues":[{{"id":"C1","text":"……"}}],"ops":[
  {{"op":"edit","id":"T2","title":"……","content":"……","clues":["C1","C3"],"reason":"……"}},
  {{"op":"merge","ids":["T3","T5"],"title":"……","content":"……","clues":["C2","C5"],"reason":"……"}},
  {{"op":"add","title":"……","content":"……","clues":["C4","C6"],"reason":"……"}},
  {{"op":"delete","id":"T4","reason":"……"}}
]}}
没有需要改的就输出 {{"clues":[...],"ops":[]}}。""".strip()


# ---------------- 模型调用 ----------------

async def _call_llm(prompt: str) -> str:
    base_url = await _dep("get_memory_base_url")()
    model = await _dep("get_memory_model")()
    if not base_url or not model:
        raise RuntimeError("记忆模型未配置（MEMORY_API_BASE_URL / MEMORY_MODEL）")
    api_key = await _dep("get_memory_api_key")()
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    headers.update(_DEPS.get("extra_headers", lambda u: {})(base_url) or {})
    body = {
        "model": model,
        "messages": [
            {"role": "system", "content": "你只输出严格 JSON 对象，不要 markdown，不要解释。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.5,
        "max_tokens": 12000,
        "stream": True,
    }
    print(f"[UserPortrait] Calling LLM(stream): model={model}, prompt_chars={len(prompt)}")
    parts, finish = [], None
    async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=30.0)) as client:
        async with client.stream("POST", base_url, headers=headers, json=body) as resp:
            if resp.status_code != 200:
                raw = (await resp.aread()).decode("utf-8", errors="ignore")
                raise RuntimeError(f"上游 HTTP {resp.status_code}：{raw[:500]}（model={model}，提示词 {len(prompt)} 字）")
            ctype = (resp.headers.get("content-type") or "").lower()
            if "event-stream" not in ctype:
                raw = (await resp.aread()).decode("utf-8", errors="ignore")
                try:
                    ch = (json.loads(raw).get("choices") or [{}])[0]
                    parts.append(str((ch.get("message") or {}).get("content") or ""))
                    finish = ch.get("finish_reason")
                except Exception:
                    parts.append(raw)
            else:
                async for line in resp.aiter_lines():
                    line = line.strip()
                    if not line.startswith("data:"):
                        continue
                    payload = line[5:].strip()
                    if payload == "[DONE]":
                        break
                    try:
                        ev = json.loads(payload)
                    except Exception:
                        continue
                    ch = (ev.get("choices") or [{}])[0]
                    piece = (ch.get("delta") or {}).get("content")
                    if piece:
                        parts.append(piece)
                    if ch.get("finish_reason"):
                        finish = ch.get("finish_reason")
    text = "".join(parts)
    if finish in ("length", "max_tokens"):
        raise RuntimeError("模型输出达到长度上限被截断，没有生成完整画像；原画像没有改动")
    if finish == "content_filter":
        raise RuntimeError("模型输出被服务商内容过滤拦下；原画像没有改动")
    if not text.strip():
        raise RuntimeError("模型没有返回内容")
    return text


def parse_json_object(text: str):
    t = re.sub(r"<think>.*?</think>", "", str(text or ""), flags=re.S | re.I).strip()
    t = re.sub(r"^```(?:json)?\s*|\s*```$", "", t).strip()
    s, e = t.find("{"), t.rfind("}")
    if s < 0 or e <= s:
        return None
    try:
        d = json.loads(t[s:e + 1])
    except Exception:
        return None
    return d if isinstance(d, dict) else None


# ---------------- 线索 / 操作整理 / 执行 ----------------

def normalize_clues(data: dict) -> list:
    out, seen = [], set()
    for c in (data.get("clues") if isinstance(data.get("clues"), list) else []):
        if isinstance(c, dict):
            cid, text = str(c.get("id") or "").strip().upper(), str(c.get("text") or "").strip()
        else:
            cid, text = "", str(c or "").strip()
        if not text:
            continue
        if not cid or cid in seen:
            cid = f"C{len(out) + 1}"
            while cid in seen:
                cid += "'"
        seen.add(cid)
        out.append({"id": cid, "text": text[:200]})
        if len(out) >= MAX_CLUES:
            break
    return out


def _clue_refs(o: dict, valid: set) -> list:
    refs = []
    for x in (o.get("clues") if isinstance(o.get("clues"), list) else []):
        cid = str(x or "").strip().upper()
        if cid in valid and cid not in refs:
            refs.append(cid)
    return refs


def _thin_warning(title: str, refs: list):
    if len(refs) < 2:
        return f"「{title}」只引用了 {len(refs)} 条线索，可能只是在复述某件事"
    return None


def normalize_initial(data: dict, clues: list = None) -> tuple:
    clues = clues if clues is not None else normalize_clues(data)
    valid = {c["id"] for c in clues}
    warnings, ops = [], []
    for t in (data.get("traits") if isinstance(data.get("traits"), list) else []):
        if not isinstance(t, dict):
            continue
        title, content = _clean_title(t.get("title")), _clean_content(t.get("content"))
        if not content:
            warnings.append("有一条特点内容为空，已忽略")
            continue
        refs = _clue_refs(t, valid)
        w = _thin_warning(title or "未命名", refs)
        if w:
            warnings.append(w)
        ops.append({"op": "add", "title": title or "未命名", "content": content, "clues": refs, "reason": ""})
    if len(ops) > MAX_TRAITS:
        warnings.append(f"模型写了 {len(ops)} 条，超过 {MAX_TRAITS} 条，可以在预览里取消勾选一些")
    return ops, warnings


def normalize_update(data: dict, traits: list, clues: list = None) -> tuple:
    """把模型输出的操作整理成合法操作；T 标签换成真实 id。每条现有特点只能被一个操作引用。"""
    clues = clues if clues is not None else normalize_clues(data)
    valid = {c["id"] for c in clues}
    warnings, ops, used = [], [], set()
    label_to_id = {f"T{i}": t["id"] for i, t in enumerate(traits, 1)}

    def ref(x):
        lab = str(x or "").strip().upper()
        if lab not in label_to_id:
            if lab:
                warnings.append(f"引用了不存在的条目 {lab}，已忽略")
            return None
        tid = label_to_id[lab]
        if tid in used:
            warnings.append(f"条目 {lab} 被多个操作引用，按第一个处理")
            return None
        return tid

    by_id = {t["id"]: t for t in traits}
    for o in (data.get("ops") if isinstance(data.get("ops"), list) else []):
        if not isinstance(o, dict):
            continue
        kind = str(o.get("op") or "").strip().lower()
        reason = str(o.get("reason") or "").strip()[:200]
        title, content = _clean_title(o.get("title")), _clean_content(o.get("content"))
        refs = _clue_refs(o, valid)
        if kind == "merge":
            ids = []
            for x in (o.get("ids") if isinstance(o.get("ids"), list) else []):
                tid = ref(x)
                if tid and tid not in ids:
                    ids.append(tid)
            if not content or not ids:
                warnings.append("有一个合并操作缺内容或条目，已忽略")
                continue
            used.update(ids)
            t_title = title or by_id[ids[0]]["title"]
            w = _thin_warning(t_title, refs)
            if w:
                warnings.append(w)
            if len(ids) == 1:
                ops.append({"op": "edit", "id": ids[0], "title": t_title, "content": content, "clues": refs, "reason": reason})
            else:
                ops.append({"op": "merge", "ids": ids, "title": t_title, "content": content, "clues": refs, "reason": reason})
        elif kind in ("edit", "delete"):
            tid = ref(o.get("id"))
            if not tid:
                continue
            if kind == "edit":
                if not content:
                    warnings.append("有一个修改操作内容为空，已忽略")
                    continue
                old = by_id[tid]
                if content == old["content"] and (title or old["title"]) == old["title"]:
                    continue
                used.add(tid)
                ops.append({"op": "edit", "id": tid, "title": title or old["title"], "content": content, "clues": refs, "reason": reason})
            else:
                used.add(tid)
                ops.append({"op": "delete", "id": tid, "reason": reason})
        elif kind == "add":
            if not content:
                warnings.append("有一个新增操作内容为空，已忽略")
                continue
            w = _thin_warning(title or "未命名", refs)
            if w:
                warnings.append(w)
            ops.append({"op": "add", "title": title or "未命名", "content": content, "clues": refs, "reason": reason})
        elif kind in ("keep", ""):
            continue
        else:
            warnings.append(f"不认识的操作 {kind}，已忽略")
    return ops, warnings


def apply_ops(traits: list, ops: list) -> tuple:
    """在 traits 上执行 ops，返回 (新列表, 跳过的操作说明)。不修改入参。"""
    now = _iso(_now())
    result = [dict(t) for t in traits]
    skipped = []

    def idx(tid):
        for i, t in enumerate(result):
            if t["id"] == tid:
                return i
        return -1

    for o in ops:
        kind = o.get("op")
        title, content = _clean_title(o.get("title")), _clean_content(o.get("content"))
        if kind == "add":
            if not content:
                skipped.append("新增内容为空")
                continue
            result.append({"id": _new_trait_id(), "title": title or "未命名", "content": content, "created_at": now, "updated_at": now})
        elif kind == "edit":
            i = idx(o.get("id"))
            if i < 0 or not content:
                skipped.append("修改的条目不存在或内容为空")
                continue
            result[i] = {**result[i], "title": title or result[i]["title"], "content": content, "updated_at": now}
        elif kind == "delete":
            i = idx(o.get("id"))
            if i < 0:
                skipped.append("删除的条目不存在")
                continue
            result.pop(i)
        elif kind == "merge":
            ids = [x for x in (o.get("ids") or []) if idx(x) >= 0]
            if len(ids) < 1 or not content:
                skipped.append("合并的条目不存在或内容为空")
                continue
            pos = min(idx(x) for x in ids)
            merged = {"id": _new_trait_id(), "title": title or result[pos]["title"], "content": content,
                      "created_at": result[pos].get("created_at") or now, "updated_at": now}
            result = [t for t in result if t["id"] not in set(ids)]
            result.insert(min(pos, len(result)), merged)
        else:
            skipped.append(f"不认识的操作 {kind}")
    return result, skipped


# ---------------- 接口 ----------------

def _enabled() -> bool:
    fn = _DEPS.get("memory_enabled")
    return bool(fn()) if fn else True


async def _read_json(request: Request) -> dict:
    try:
        data = await request.json()
    except Exception:
        data = {}
    return data if isinstance(data, dict) else {}


@router.get("/api/user-portrait")
async def api_get_user_portrait(character_id: str = "default"):
    if not _enabled():
        return {"error": "记忆系统未启用"}
    try:
        item = await get_user_portrait(character_id or "default")
        today = _local_today().isoformat()
        if not item:
            return {"status": "not_found", "traits": [], "today": today}
        return {"status": "ok", **item, "today": today}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@router.post("/api/user-portrait/materials")
async def api_user_portrait_materials(request: Request):
    """材料预览：只收集材料，不调用模型。"""
    if not _enabled():
        return {"error": "记忆系统未启用"}
    data = await _read_json(request)
    cid = data.get("character_id") or "default"
    try:
        start, end = _check_range(data.get("start"), data.get("end"))
    except ValueError as e:
        return {"status": "error", "error": str(e)}
    try:
        m = await build_materials(cid, start, end, bool(data.get("include_old")))
        return {"status": "ok", "range_start": m["range_start"], "range_end": m["range_end"], "stats": m["stats"],
                "material_chars": len(m["material_text"]), "prompt_max_chars": PROMPT_MAX_CHARS,
                "material_text": m["material_text"]}
    except Exception as e:
        return {"status": "error", "error": f"{type(e).__name__}: {e}"}


@router.post("/api/user-portrait/generate-preview")
async def api_user_portrait_generate_preview(request: Request):
    if not _enabled():
        return {"error": "记忆系统未启用"}
    data = await _read_json(request)
    cid = data.get("character_id") or "default"
    mode = "update" if data.get("mode") == "update" else "initial"
    try:
        start, end = _check_range(data.get("start"), data.get("end"))
    except ValueError as e:
        return {"status": "error", "error": str(e)}
    lk = _lock(cid)
    if lk.locked():
        return JSONResponse({"status": "error", "error": "特点画像正在生成中，请等当前任务完成"}, status_code=429)
    async with lk:
        try:
            current = await get_user_portrait(cid)
            traits = (current or {}).get("traits") or []
            if mode == "update" and not traits:
                return {"status": "error", "error": "还没有特点画像，请先「从零生成」"}
            m = await build_materials(cid, start, end, bool(data.get("include_old")))
            if m["empty"]:
                return {"status": "error", "error": "这段时间没有聊天记录，也没有日印象"}
            prompt = build_initial_prompt(m) if mode == "initial" else build_update_prompt(m, traits)
            if len(prompt) > PROMPT_MAX_CHARS:
                return {"status": "error", "error": f"材料太长（提示词 {len(prompt)} 字，上限 {PROMPT_MAX_CHARS}），请缩短日期范围"}
            raw = await _call_llm(prompt)
            parsed = parse_json_object(raw)
            if parsed is None:
                return {"status": "error", "error": "模型返回的内容解析不出 JSON", "raw": str(raw)[:4000]}
            clues = normalize_clues(parsed)
            if mode == "initial":
                ops, warnings = normalize_initial(parsed, clues)
                if not ops:
                    return {"status": "error", "error": "模型没有写出任何特点", "raw": str(raw)[:4000]}
            else:
                ops, warnings = normalize_update(parsed, traits, clues)
            if not clues:
                warnings.insert(0, "模型没有列出线索")
            return {
                "status": "ok", "mode": mode, "character_id": cid,
                "base_revision": int((current or {}).get("revision") or 0),
                "current_traits": traits,
                "clues": clues, "ops": ops, "warnings": warnings,
                "range_start": m["range_start"], "range_end": m["range_end"],
                "material_summary": {**m["stats"], "prompt_chars": len(prompt)},
            }
        except Exception as e:
            print(f"⚠️ 特点画像生成失败: {type(e).__name__}: {e}")
            return {"status": "error", "error": str(e) if isinstance(e, RuntimeError) else f"{type(e).__name__}: {e}"}


@router.post("/api/user-portrait/confirm")
async def api_user_portrait_confirm(request: Request):
    if not _enabled():
        return {"error": "记忆系统未启用"}
    data = await _read_json(request)
    cid = data.get("character_id") or "default"
    mode = "update" if data.get("mode") == "update" else "initial"
    ops = [o for o in (data.get("ops") or []) if isinstance(o, dict)]
    try:
        pool = await _dep("get_pool")()
        async with pool.acquire() as conn:
            await _ensure_table(conn)
            async with conn.transaction():
                row = await conn.fetchrow("SELECT traits, revision FROM user_portraits WHERE character_id = $1 FOR UPDATE", cid)
                rev = int(row["revision"]) if row else 0
                if rev != int(data.get("base_revision") or 0):
                    return JSONResponse({"status": "error", "error": "预览之后画像被改过了，请重新生成预览"}, status_code=409)
                base = [] if mode == "initial" else _parse_traits(row["traits"] if row else [])
                if mode == "initial":
                    ops = [o for o in ops if o.get("op") == "add"]
                    if not ops:
                        return JSONResponse({"status": "error", "error": "至少要保留一条特点"}, status_code=400)
                new_traits, skipped = apply_ops(base, ops)
                revision = await _save(conn, cid, new_traits, mode, data.get("range_start"), data.get("range_end"), keep_range=False)
        return {"status": "ok", "revision": revision, "traits": new_traits, "skipped": skipped}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@router.put("/api/user-portrait")
async def api_user_portrait_save(request: Request):
    """手动编辑：整份替换条目，不改上次用到的日期范围。"""
    if not _enabled():
        return {"error": "记忆系统未启用"}
    data = await _read_json(request)
    cid = data.get("character_id") or "default"
    try:
        incoming = data.get("traits") if isinstance(data.get("traits"), list) else []
        pool = await _dep("get_pool")()
        async with pool.acquire() as conn:
            await _ensure_table(conn)
            async with conn.transaction():
                row = await conn.fetchrow("SELECT traits, revision FROM user_portraits WHERE character_id = $1 FOR UPDATE", cid)
                if row and int(row["revision"]) != int(data.get("base_revision") or 0):
                    return JSONResponse({"status": "error", "error": "画像在你编辑期间被改过了，请刷新后再改"}, status_code=409)
                old = {t["id"]: t for t in _parse_traits(row["traits"] if row else [])}
                now = _iso(_now())
                traits = []
                for t in incoming:
                    if not isinstance(t, dict):
                        continue
                    content = _clean_content(t.get("content"))
                    if not content:
                        continue
                    title = _clean_title(t.get("title")) or "未命名"
                    prev = old.get(str(t.get("id") or ""))
                    if prev:
                        changed = prev["title"] != title or prev["content"] != content
                        traits.append({**prev, "title": title, "content": content, "updated_at": now if changed else prev.get("updated_at")})
                    else:
                        traits.append({"id": _new_trait_id(), "title": title, "content": content, "created_at": now, "updated_at": now})
                revision = await _save(conn, cid, traits, "manual", keep_range=True)
        return {"status": "ok", "revision": revision, "traits": traits}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@router.delete("/api/user-portrait")
async def api_user_portrait_delete(character_id: str = "default"):
    if not _enabled():
        return {"error": "记忆系统未启用"}
    try:
        pool = await _dep("get_pool")()
        async with pool.acquire() as conn:
            await _ensure_table(conn)
            await conn.execute("DELETE FROM user_portraits WHERE character_id = $1", character_id or "default")
        _invalidate(character_id or "default")
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)
