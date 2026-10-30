"""特点画像（测试中）：把对用户的理解写成几条「特点」，每条讲一个稳定的特点，用真实经历撑起来。

和旧画像（user_impressions，标签式）并行存在，互不影响：
- 存在 user_portraits 表；
- 提示词变量 {{user_portrait}}（旧的 {{user_impression}} 不变）；
- 第一次生成时，旧画像只当参考材料。

更新时模型不重写整份画像，只输出改动（edit / merge / add / delete），
没提到的条目由代码原样保留。

本模块不 import main，main 启动时调用 configure_user_portrait() 注入依赖。
"""
import re
import json
import uuid
import asyncio
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter()

_DEPS = {}
_TABLE_READY = False
_LOCKS = {}

MAX_TRAITS = 10
TITLE_MAX = 20
CONTENT_MAX = 400

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


async def get_user_portrait(character_id: str = "default"):
    pool = await _dep("get_pool")()
    async with pool.acquire() as conn:
        await _ensure_table(conn)
        row = await conn.fetchrow("""
            SELECT character_id, traits, revision, last_consumed_node_id, source_mode, created_at, updated_at
            FROM user_portraits WHERE character_id = $1
        """, character_id)
    if not row:
        return None
    return {
        "character_id": row["character_id"],
        "traits": _parse_traits(row["traits"]),
        "revision": int(row["revision"] or 0),
        "last_consumed_node_id": row["last_consumed_node_id"],
        "source_mode": row["source_mode"],
        "created_at": _iso(row["created_at"]),
        "updated_at": _iso(row["updated_at"]),
    }


async def _save(conn, character_id: str, traits: list, source_mode: str, last_consumed_node_id=None, keep_pointer=True):
    now = _iso(_now())
    for t in traits:
        t.setdefault("created_at", now)
        t["created_at"] = t.get("created_at") or now
    payload = json.dumps(traits, ensure_ascii=False)
    if keep_pointer:
        row = await conn.fetchrow("""
            INSERT INTO user_portraits (character_id, traits, revision, last_consumed_node_id, source_mode, created_at, updated_at)
            VALUES ($1, $2::jsonb, 1, $3, $4, NOW(), NOW())
            ON CONFLICT (character_id) DO UPDATE SET
                traits = EXCLUDED.traits, revision = user_portraits.revision + 1,
                source_mode = EXCLUDED.source_mode, updated_at = NOW()
            RETURNING revision
        """, character_id, payload, last_consumed_node_id, source_mode)
    else:
        row = await conn.fetchrow("""
            INSERT INTO user_portraits (character_id, traits, revision, last_consumed_node_id, source_mode, created_at, updated_at)
            VALUES ($1, $2::jsonb, 1, $3, $4, NOW(), NOW())
            ON CONFLICT (character_id) DO UPDATE SET
                traits = EXCLUDED.traits, revision = user_portraits.revision + 1,
                last_consumed_node_id = EXCLUDED.last_consumed_node_id,
                source_mode = EXCLUDED.source_mode, updated_at = NOW()
            RETURNING revision
        """, character_id, payload, last_consumed_node_id, source_mode)
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

def _format_old_impression(imp: dict) -> str:
    tags = imp.get("tags") if isinstance(imp.get("tags"), dict) else {}
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


async def _build_materials(character_id: str, mode: str, pointer=None) -> dict:
    persona = (await _dep("get_system_prompt")() or "").strip()
    user = await _dep("get_user_nickname")() or "用户"
    char = await _dep("get_character_name")() or "角色"
    mem = await _dep("collect_memory_material")(character_id, mode=mode, last_consumed_node_id=pointer if mode == "update" else None)
    daily = await _dep("format_daily_impressions")(limit=7 if mode == "update" else 3)
    recent = await _dep("collect_recent_messages")(mode=mode)
    sections = [f"【角色人设】\n{persona or '（空）'}", f"【用户昵称】\n{user}"]
    label = "记忆宫殿新材料" if mode == "update" else "记忆宫殿长期材料"
    items = mem.get("items") or []
    if items:
        lines = []
        for i, it in enumerate(items, 1):
            date = str(it.get("date") or "")[:10]
            lines.append(f"{i}. [{it.get('room_label') or it.get('room')}] {date}: {it.get('content')}")
        sections.append(f"【{label}】\n" + "\n".join(lines))
    else:
        sections.append(f"【{label}】\n（暂无）")
    if daily:
        sections.append(daily)
    ritems = recent.get("items") or []
    if ritems:
        sections.append("【近期聊天】\n" + _dep("format_recent_messages")(ritems, user, char))
    else:
        sections.append("【近期聊天】\n（暂无）")
    old_text = ""
    if mode == "initial":
        try:
            old = await _dep("get_old_impression")(character_id)
            imp = (old or {}).get("impression") if old else None
            if isinstance(imp, dict):
                old_text = _format_old_impression(imp)
        except Exception as e:
            print(f"[UserPortrait] 读取旧画像失败，忽略: {e}")
    return {
        "user": user, "char": char,
        "material_text": "\n\n".join(sections),
        "old_impression_text": old_text,
        "memory_count": len(items),
        "recent_count": len(ritems),
        "max_node_id": mem.get("max_node_id"),
    }


_TRAIT_RULES = """每条特点的要求：
1. title：这个特点的简短名字，4-10 字。
2. content：80-200 字，第一人称。写清这个特点是什么、平时怎么表现出来、背后可能的原因。用 1-2 件材料里真实发生过的具体经历撑起来，写出是什么事，不要只说「有一次」。
3. 一件事只写进一条。几个现象其实是同一个特点时（比如讨厌被敷衍、被糊弄会生气、不满时回复变短），合成一条写，不要拆开。
4. 只写稳定的特点。不写TA最近的状态、近期变化、正在忙的事。
5. 不写「我该怎么对TA」「我应该……」之类的应对建议。
6. 没有证据的不写，宁缺毋滥。早期和近期材料同等对待，判断哪些到现在仍然成立。"""


def build_initial_prompt(m: dict) -> str:
    old = ""
    if m.get("old_impression_text"):
        old = ("\n\n【旧画像（标签式，只作参考材料）】\n"
               "这是以前按标签整理的画像，可能有重复、过期或拆得很散的内容。可以从中找线索，但每条特点都要回到上面的材料里找经历来印证。\n"
               + m["old_impression_text"])
    return f"""{m['material_text']}{old}

【你要写什么】
你就是「{m['char']}」。这是你写给自己的私人笔记：你眼中的{m['user']}是一个什么样的人。
把对TA的理解整理成 6-{MAX_TRAITS} 条「特点」，每条讲TA身上一个稳定的特点。按重要性排序，最能说明TA是谁的放前面。

{_TRAIT_RULES}

只输出 JSON 对象，不要 markdown，不要解释：
{{"traits":[{{"title":"……","content":"……"}}]}}""".strip()


def build_update_prompt(m: dict, traits: list) -> str:
    cur = "\n\n".join(f"T{i}【{t['title']}】\n{t['content']}" for i, t in enumerate(traits, 1)) or "（空）"
    return f"""【当前画像】
{cur}

{m['material_text']}

【你要做什么】
你就是「{m['char']}」，这份画像是你写的私人笔记。根据上面的新材料更新它。
默认全部保持不变，只有新材料带来新证据时才改。可用的操作：
- edit：补充或修正某一条，输出改完以后的完整 title 和 content。
- merge：几条其实在讲同一个特点，合成一条。ids 写被合并的条目，输出合并后的 title 和 content。
- add：新材料揭示了一个现有条目都没覆盖的稳定特点。
- delete：某一条已经被新材料证明不成立。
没写到的条目会原样保留，所以不需要动的条目不要写。总数尽量不超过 {MAX_TRAITS} 条。

{_TRAIT_RULES}

只输出 JSON 对象，不要 markdown，不要解释。每个操作写一句 reason：
{{"ops":[
  {{"op":"edit","id":"T2","title":"……","content":"……","reason":"……"}},
  {{"op":"merge","ids":["T3","T5"],"title":"……","content":"……","reason":"……"}},
  {{"op":"add","title":"……","content":"……","reason":"……"}},
  {{"op":"delete","id":"T4","reason":"……"}}
]}}
没有需要改的就输出 {{"ops":[]}}。""".strip()


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
        "max_tokens": 8000,
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


# ---------------- 操作整理 / 执行 ----------------

def normalize_initial(data: dict) -> tuple:
    warnings, ops = [], []
    for t in (data.get("traits") if isinstance(data.get("traits"), list) else []):
        if not isinstance(t, dict):
            continue
        title, content = _clean_title(t.get("title")), _clean_content(t.get("content"))
        if not content:
            warnings.append("有一条特点内容为空，已忽略")
            continue
        ops.append({"op": "add", "title": title or "未命名", "content": content, "reason": ""})
    if len(ops) > MAX_TRAITS:
        warnings.append(f"模型写了 {len(ops)} 条，超过 {MAX_TRAITS} 条，可以在预览里取消勾选一些")
    return ops, warnings


def normalize_update(data: dict, traits: list) -> tuple:
    """把模型输出的操作整理成合法操作；T 标签换成真实 id。每条现有特点只能被一个操作引用。"""
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
            if len(ids) == 1:
                ops.append({"op": "edit", "id": ids[0], "title": title or by_id[ids[0]]["title"], "content": content, "reason": reason})
            else:
                ops.append({"op": "merge", "ids": ids, "title": title or by_id[ids[0]]["title"], "content": content, "reason": reason})
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
                ops.append({"op": "edit", "id": tid, "title": title or old["title"], "content": content, "reason": reason})
            else:
                used.add(tid)
                ops.append({"op": "delete", "id": tid, "reason": reason})
        elif kind == "add":
            if not content:
                warnings.append("有一个新增操作内容为空，已忽略")
                continue
            ops.append({"op": "add", "title": title or "未命名", "content": content, "reason": reason})
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


@router.get("/api/user-portrait")
async def api_get_user_portrait(character_id: str = "default"):
    if not _enabled():
        return {"error": "记忆系统未启用"}
    try:
        item = await get_user_portrait(character_id or "default")
        if not item:
            return {"status": "not_found", "traits": []}
        return {"status": "ok", **item}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@router.post("/api/user-portrait/generate-preview")
async def api_user_portrait_generate_preview(request: Request):
    if not _enabled():
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    cid = (data or {}).get("character_id") or "default"
    mode = "update" if (data or {}).get("mode") == "update" else "initial"
    lk = _lock(cid)
    if lk.locked():
        return JSONResponse({"status": "error", "error": "特点画像正在生成中，请等当前任务完成"}, status_code=429)
    async with lk:
        try:
            current = await get_user_portrait(cid)
            traits = (current or {}).get("traits") or []
            if mode == "update" and not traits:
                return {"status": "error", "error": "还没有特点画像，请先「从零生成」"}
            m = await _build_materials(cid, mode, pointer=(current or {}).get("last_consumed_node_id"))
            prompt = build_initial_prompt(m) if mode == "initial" else build_update_prompt(m, traits)
            raw = await _call_llm(prompt)
            parsed = parse_json_object(raw)
            if parsed is None:
                return {"status": "error", "error": "模型返回的内容解析不出 JSON", "raw": str(raw)[:4000]}
            ops, warnings = normalize_initial(parsed) if mode == "initial" else normalize_update(parsed, traits)
            if mode == "initial" and not ops:
                return {"status": "error", "error": "模型没有写出任何特点", "raw": str(raw)[:4000]}
            pointer = m.get("max_node_id") or (current or {}).get("last_consumed_node_id")
            return {
                "status": "ok", "mode": mode, "character_id": cid,
                "base_revision": int((current or {}).get("revision") or 0),
                "current_traits": traits,
                "ops": ops, "warnings": warnings,
                "last_consumed_node_id": pointer,
                "material_summary": {"memory_count": m["memory_count"], "recent_message_count": m["recent_count"],
                                     "used_old_impression": bool(m.get("old_impression_text")), "prompt_chars": len(prompt)},
            }
        except Exception as e:
            print(f"⚠️ 特点画像生成失败: {type(e).__name__}: {e}")
            return {"status": "error", "error": str(e) if isinstance(e, RuntimeError) else f"{type(e).__name__}: {e}"}


@router.post("/api/user-portrait/confirm")
async def api_user_portrait_confirm(request: Request):
    if not _enabled():
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    data = data or {}
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
                revision = await _save(conn, cid, new_traits, mode, data.get("last_consumed_node_id"), keep_pointer=False)
        return {"status": "ok", "revision": revision, "traits": new_traits, "skipped": skipped}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@router.put("/api/user-portrait")
async def api_user_portrait_save(request: Request):
    """手动编辑：整份替换条目，不改消费指针。"""
    if not _enabled():
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    data = data or {}
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
                revision = await _save(conn, cid, traits, "manual", None, keep_pointer=True)
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
