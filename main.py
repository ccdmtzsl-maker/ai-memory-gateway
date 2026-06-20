"""
AI Memory Gateway — 带记忆系统的 LLM 转发网关
=============================================
让你的 AI 拥有长期记忆。

工作原理：
1. 接收客户端（Kelivo / ChatBox / 任何 OpenAI 兼容客户端）的消息
2. 自动搜索数据库中的相关记忆，注入 system prompt
3. 转发给 LLM API（支持 OpenRouter / OpenAI / 任何兼容接口）
4. 后台自动存储对话 + 用 AI 提取新记忆

环境变量 MEMORY_ENABLED=false 时退化为纯转发网关（第一阶段）。
"""

import os
import json
import uuid
import asyncio
import re
import httpx
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from collections import deque
from fastapi import Request
from fastapi.responses import JSONResponse
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from database import init_tables, close_pool, save_message, search_memories, save_memory, get_all_memories_count, get_recent_memories, get_all_memories, get_pool, get_all_memories_detail, update_memory, delete_memory, delete_memories_batch, get_gateway_config, set_gateway_config, get_all_gateway_config, get_conversation_messages, get_session_cache_state, save_session_cache_state, delete_session_cache_state, save_token_usage, ensure_token_usage_table, get_conversations_paginated, delete_conversation, batch_delete_conversations, merge_sessions_to_target, list_all_session_cache_states, export_all_conversations, import_conversations, get_last_user_content, update_last_assistant_message, db_row_to_message, backfill_memory_embeddings, get_pending_memory_embedding_count, search_conversations, update_message_content, rename_session_id, get_fragments_by_date, get_conversation_messages_by_date, get_fragments_by_date_range, create_event_memory, deactivate_memories, promote_to_core, merge_memories, check_duplicate_memory, update_memory_with_layer, get_layer_statistics, cleanup_old_fragments, revert_merge, upsert_daily_impression, get_daily_impression, list_daily_impressions
import database as _db_module  # 用于 /api/settings 热更新 database.py 全局变量
from memory_extractor import extract_memories, score_memories, get_extraction_prompt, set_extraction_prompt, _DEFAULT_EXTRACTION_PROMPT

# ============================================================
# 配置项 —— 全部从环境变量读取，部署时在云平台面板里设置
# ============================================================

# 你的 API Key（OpenRouter / OpenAI / 其他兼容服务）
API_KEY = os.getenv("API_KEY", "")

# 你的环境变量名可以自己定，比如就叫 MY_SECRET_KEY
SECRET_KEY = os.environ.get("SECRET_KEY", "")

# API 地址（改这个就能切换不同的 LLM 服务商）
# OpenRouter: https://openrouter.ai/api/v1/chat/completions
# OpenAI:     https://api.openai.com/v1/chat/completions
# 本地 Ollama: http://localhost:11434/v1/chat/completions
API_BASE_URL = os.getenv("API_BASE_URL", "https://openrouter.ai/api/v1/chat/completions")

# 默认模型（如果客户端没指定就用这个）
DEFAULT_MODEL = os.getenv("DEFAULT_MODEL", "anthropic/claude-sonnet-4")

# 主聊天温度参数；留空则不覆盖客户端请求
CHAT_TEMPERATURE = os.getenv("CHAT_TEMPERATURE", "")

# 网关端口
PORT = int(os.getenv("PORT", "8080"))

# 记忆系统开关（数据库出问题时可以临时关掉）
MEMORY_ENABLED = os.getenv("MEMORY_ENABLED", "false").lower() == "true"

# 每次注入的最大记忆条数
MAX_MEMORIES_INJECT = int(os.getenv("MAX_MEMORIES_INJECT", "15"))

# 记忆提取间隔（0 = 禁用自动提取，1 = 每轮提取，N = 每 N 轮提取一次）
MEMORY_EXTRACT_INTERVAL = int(os.getenv("MEMORY_EXTRACT_INTERVAL", "1"))

# 记忆提取+注入总开关（false时数据库仍连接、消息仍存储，但不提取也不注入记忆）
MEMORY_EXTRACT_ENABLED = os.getenv("MEMORY_EXTRACT_ENABLED", "true").lower() == "true"

# 分区缓存
CACHE_PARTITION_ENABLED = os.getenv("CACHE_PARTITION_ENABLED", "false").lower() == "true"
CACHE_PARTITION_X = int(os.getenv("CACHE_PARTITION_X", "15"))
CACHE_SUMMARY_MODEL = os.getenv("CACHE_SUMMARY_MODEL", "anthropic/claude-haiku-4.5")
CACHE_PARTITION_TRIGGER = os.getenv("CACHE_PARTITION_TRIGGER", "rounds")  # rounds=按轮次 | time=按时间窗口
CACHE_PARTITION_WINDOW = int(os.getenv("CACHE_PARTITION_WINDOW", "30"))  # 时间窗口（分钟），仅 trigger=time 时生效
PARTITION_SESSION_ID = os.getenv("PARTITION_SESSION_ID", "")

def get_active_session_id() -> str:
    return PARTITION_SESSION_ID

# 时区偏移（小时），用于记忆注入时的日期显示，默认 UTC+8
TIMEZONE_HOURS = int(os.getenv("TIMEZONE_HOURS", "8"))

# 轮次计数器（仅作为数据库统计失败时的兜底）
_round_counter = 0

# Dashboard 后台日志：只保留最近若干条，避免占内存。
_dashboard_logs = deque(maxlen=200)

# Dashboard 调试：只保留最近一次实际转发给上游模型的请求体。
# 不主动打印，避免日志刷屏；需要时由后台日志页手动查看。
_last_upstream_request_body = None
_last_upstream_request_meta = {}

def add_dashboard_log(level: str, message: str, category: str = "memory", session_id: str = ""):
    item = {
        "time": (datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_HOURS)).strftime("%m-%d %H:%M:%S"),
        "level": level,
        "category": category,
        "session_id": session_id or "",
        "message": message,
    }
    _dashboard_logs.appendleft(item)
    print(message)

# 强制流式传输（部分客户端不发stream=true导致thinking数据丢失，开启后强制所有请求走流式）
FORCE_STREAM = os.getenv("FORCE_STREAM", "false").lower() == "true"

# 非流式响应文本正则转换。流式响应不处理，避免 chunk 拆分导致误替换。
RESPONSE_TRANSFORM_ENABLED = os.getenv("RESPONSE_TRANSFORM_ENABLED", "false").lower() == "true"
RESPONSE_TRANSFORM_RULES = os.getenv("RESPONSE_TRANSFORM_RULES", "")

# 推理/思维链参数（部分客户端走网关时不会自动添加reasoning参数，导致上游不返回thinking数据）
# 设为 low/medium/high 会在转发请求时注入 reasoning_effort 参数
REASONING_EFFORT = os.getenv("REASONING_EFFORT", "")

# 记忆模型专用 API 地址。留空时不会自动回退到主 API_BASE_URL，由调用方决定是否跳过。
MEMORY_API_BASE_URL = os.getenv("MEMORY_API_BASE_URL", "")

# 记忆模型专用 API Key（不设则回退到主 API_KEY）
# 适用于中转站按模型分组、不同模型需要不同 Key 的场景
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "")

def get_memory_api_key() -> str:
    return MEMORY_API_KEY or API_KEY

def get_memory_api_base_url() -> str:
    return MEMORY_API_BASE_URL

# 额外的请求头（有些 API 需要，比如 OpenRouter 需要 Referer）
EXTRA_REFERER = os.getenv("EXTRA_REFERER", "https://ai-memory-gateway.local")
EXTRA_TITLE = os.getenv("EXTRA_TITLE", "AI Memory Gateway")


# ============================================================
# 人设加载
# ============================================================

def load_system_prompt():
    """从 system_prompt.txt 文件读取人设内容"""
    prompt_path = os.path.join(os.path.dirname(__file__), "system_prompt.txt")
    try:
        with open(prompt_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if content:
                return content
    except FileNotFoundError:
        pass
    print("ℹ️  未找到 system_prompt.txt 或文件为空，将不注入 system prompt")
    return ""


SYSTEM_PROMPT = load_system_prompt()
_DEFAULT_SYSTEM_PROMPT = SYSTEM_PROMPT  # 保留文件原始版本
if SYSTEM_PROMPT:
    print(f"✅ 人设已加载，长度：{len(SYSTEM_PROMPT)} 字符")
else:
    print("ℹ️  无人设，纯转发模式")

# System Prompt 缓存（支持设置面板热更新）
_cached_system_prompt = None
_cached_system_prompt_loaded = False

async def get_system_prompt() -> str:
    """获取 system prompt（数据库优先，fallback 到文件）"""
    global _cached_system_prompt, _cached_system_prompt_loaded
    if _cached_system_prompt_loaded:
        return _cached_system_prompt or ""
    try:
        db_prompt = await get_gateway_config("systemPrompt", "")
        if db_prompt:
            _cached_system_prompt = db_prompt
        else:
            _cached_system_prompt = _DEFAULT_SYSTEM_PROMPT
            if _DEFAULT_SYSTEM_PROMPT:
                await set_gateway_config("systemPrompt", _DEFAULT_SYSTEM_PROMPT)
        _cached_system_prompt_loaded = True
        return _cached_system_prompt or ""
    except Exception:
        _cached_system_prompt = _DEFAULT_SYSTEM_PROMPT
        _cached_system_prompt_loaded = True
        return _cached_system_prompt or ""

def invalidate_system_prompt_cache():
    """清除 system prompt 缓存（设置面板更新后调用）"""
    global _cached_system_prompt, _cached_system_prompt_loaded
    _cached_system_prompt = None
    _cached_system_prompt_loaded = False


# ============================================================
# 应用生命周期管理
# ============================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用启动时初始化数据库，关闭时断开连接"""
    global PARTITION_SESSION_ID
    if MEMORY_ENABLED:
        try:
            await init_tables()
            await ensure_token_usage_table()
            count = await get_all_memories_count()
            print(f"✅ 记忆系统已启动，当前记忆数量：{count}")
            
            # 从数据库恢复面板配置（重启后保持Dashboard修改过的值）
            try:
                db_cfg = await get_all_gateway_config()
                if db_cfg:
                    _RESTORE_MAIN = {
                        "API_BASE_URL": str, "API_KEY": str, "DEFAULT_MODEL": str, "CHAT_TEMPERATURE": str,
                        "MEMORY_ENABLED": lambda v: _parse_bool(v),
                        "MAX_MEMORIES_INJECT": int, "MEMORY_EXTRACT_INTERVAL": int,
                        "CACHE_PARTITION_ENABLED": lambda v: _parse_bool(v),
                        "CACHE_PARTITION_X": int, "CACHE_PARTITION_TRIGGER": str,
                        "CACHE_PARTITION_WINDOW": int, "CACHE_SUMMARY_MODEL": str,
                        "FORCE_STREAM": lambda v: _parse_bool(v),
                        "RESPONSE_TRANSFORM_ENABLED": lambda v: _parse_bool(v),
                        "RESPONSE_TRANSFORM_RULES": str,
                        "REASONING_EFFORT": str,
                    }
                    _RESTORE_DB = {
                        "EMBEDDING_API_KEY": str, "EMBEDDING_BASE_URL": str,
                        "EMBEDDING_MODEL": str, "EMBEDDING_DIM": int,
                        "MIN_SCORE_THRESHOLD": float,
                        "MEMORY_VECTOR_ENABLED": lambda v: _parse_bool(v),
                        "MEMORY_HW_KEYWORD": float, "MEMORY_HW_SEMANTIC": float,
                        "MEMORY_HW_IMPORTANCE": float, "MEMORY_HW_RECENCY": float,
                        "MEMORY_SEMANTIC_THRESHOLD": float,
                    }
                    restored = []
                    for key, val in db_cfg.items():
                        if not val:
                            continue
                        # 跳过被误存为打码值的 Key 字段
                        if key in ("API_KEY", "MEMORY_API_KEY", "EMBEDDING_API_KEY") and _is_masked(str(val)):
                            print(f"⚠️  跳过恢复 {key}：DB 中存储的是打码值，将使用环境变量")
                            continue
                        if key in _RESTORE_MAIN:
                            globals()[key] = _RESTORE_MAIN[key](val)
                            restored.append(key)
                        elif key in _RESTORE_DB:
                            setattr(_db_module, key, _RESTORE_DB[key](val))
                            restored.append(key)
                        elif key == "MEMORY_MODEL":
                            os.environ["MEMORY_MODEL"] = str(val)
                            import memory_extractor as _me_mod
                            _me_mod.MEMORY_MODEL = str(val)
                            restored.append(key)
                        elif key == "MEMORY_API_KEY":
                            if not _is_masked(str(val)):
                                globals()[key] = str(val)
                                import memory_extractor as _me_mod
                                _me_mod.MEMORY_API_KEY = str(val)
                                restored.append(key)
                            else:
                                print(f"⚠️  跳过恢复 MEMORY_API_KEY：DB 中存储的是打码值")
                        elif key == "MEMORY_API_BASE_URL":
                            globals()[key] = str(val)
                            import memory_extractor as _me_mod
                            _me_mod.MEMORY_API_BASE_URL = str(val)
                            restored.append(key)
                    if restored:
                        print(f"🔄 从数据库恢复 {len(restored)} 项面板配置: {', '.join(restored)}")
            except Exception as e:
                print(f"[warning] 恢复面板配置失败: {e}")
            
            if not MEMORY_EXTRACT_ENABLED:
                print(f"ℹ️  记忆提取+注入已关闭（MEMORY_EXTRACT_ENABLED=false）")
            
            # 分区缓存：从DB读取活跃对话线ID
            if CACHE_PARTITION_ENABLED:
                db_sid = await get_gateway_config("partition_session_id", "")
                if db_sid:
                    PARTITION_SESSION_ID = db_sid
                    print(f"🔗 活跃对话线(DB): {PARTITION_SESSION_ID}")
                elif PARTITION_SESSION_ID:
                    await set_gateway_config("partition_session_id", PARTITION_SESSION_ID)
                    print(f"🔗 活跃对话线(ENV→DB): {PARTITION_SESSION_ID}")
                print(f"🔒 分区缓存已启用: X={CACHE_PARTITION_X}, 摘要模型=MEMORY_MODEL({os.getenv('MEMORY_MODEL', 'anthropic/claude-haiku-4')})")
        except Exception as e:
            print(f"⚠️  数据库初始化失败: {e}")
            print("⚠️  记忆系统将不可用，但网关仍可正常转发")
    else:
        print("ℹ️  记忆系统已关闭（设置 MEMORY_ENABLED=true 开启）")
    
    yield
    
    if MEMORY_ENABLED:
        await close_pool()


app = FastAPI(title="AI Memory Gateway", version="2.0.0", lifespan=lifespan)

# 静态文件和模板配置
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# ============================================================
# 鉴权
# ============================================================
@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    # 放行健康检查和静态资源
    if request.url.path == "/" or request.url.path.startswith("/static/"):
        return await call_next(request)
    
    # 尝试从多个位置获取密钥：Cookie、请求头、查询参数
    auth_key = (
        request.cookies.get("api_key") or
        request.headers.get("X-API-Key") or
        request.headers.get("X-Gateway-Key") or
        request.query_params.get("api_key") or
        request.query_params.get("gateway_key")
    )
    
    # 如果没有密钥或者密钥不匹配
    if not SECRET_KEY or auth_key != SECRET_KEY:
        return JSONResponse(
            status_code=403,
            content={"error": "Forbidden", "message": "Missing or invalid API key"}
        )
    
    # 处理请求
    response = await call_next(request)
    
    # 如果本次是从查询参数获取到的密钥，就设置一个 cookie（有效期1天）
    if request.query_params.get("api_key") or request.query_params.get("gateway_key"):
        response.set_cookie(
            key="api_key", 
            value=auth_key, 
            httponly=True, 
            max_age=86400,  # 1天，你可以改成 3600（1小时）或 None（浏览器关闭失效）
            samesite="lax"
        )
    return response
    
# ============================================================
# 记忆注入
# ============================================================

async def build_system_prompt_with_memories(user_message: str) -> str:
    """
    构建带记忆的 system prompt
    1. 用用户消息搜索相关记忆
    2. 格式化成文本拼接到人设后面
    """
    if not MEMORY_ENABLED or not MEMORY_EXTRACT_ENABLED:
        return SYSTEM_PROMPT
    
    if MAX_MEMORIES_INJECT <= 0:
        return SYSTEM_PROMPT
    
    try:
        memories = await search_memories(user_message, limit=MAX_MEMORIES_INJECT)
        
        if not memories:
            return SYSTEM_PROMPT
        
        # 格式化记忆文本（带日期，帮助模型判断新旧）
        memory_lines = []
        for mem in memories:
            date_str = ""
            if mem.get("created_at"):
                try:
                    utc_str = str(mem['created_at'])[:19]
                    utc_dt = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    local_dt = utc_dt + timedelta(hours=TIMEZONE_HOURS)
                    date_str = f"[{local_dt.strftime('%Y-%m-%d')}] "
                except:
                    date_str = f"[{str(mem['created_at'])[:10]}] "
            memory_lines.append(f"- {date_str}{mem['content']}")
        memory_text = "\n".join(memory_lines)
        
        enhanced_prompt = f"""{SYSTEM_PROMPT}

【从过往对话中检索到的相关记忆】
{memory_text}

# 记忆应用
- 像朋友般自然运用这些记忆，不刻意展示
- 仅在相关话题出现时引用，避免主动提及
- 对重要信息（如健康、日期、约定）保持一致性
- 新信息与记忆冲突时，以新信息为准
- 模糊记忆可表达不确定性："记得你似乎说过..."

# 交流方式
- 自然引用："记得你说过..."或"上次我们聊到..."
- 避免机械式表达如"根据我的记忆..."或"检索到的信息显示..."
- 共同经历可温情回忆："上次那个事挺好玩的"

记忆是丰富对话的工具，而非对话焦点。"""
        
        print(f"📚 注入了 {len(memories)} 条相关记忆")
        return enhanced_prompt
        
    except Exception as e:
        print(f"⚠️  记忆检索失败: {e}，使用纯人设")
        return SYSTEM_PROMPT


# ============================================================
# 分区缓存（Partition Cache）
# ============================================================

def _is_anthropic_model(model: str) -> bool:
    """判断是否为 Anthropic Claude 系列模型（只有 Claude 支持 cache_control）"""
    model_lower = model.lower()
    return "claude" in model_lower or "anthropic" in model_lower


def _strip_cache_control(messages: list):
    """
    剥掉消息中的 cache_control 字段，非 Claude 模型用不了。
    如果 content 数组只剩纯文本 block，降级回字符串格式。
    """
    stripped = 0
    for msg in messages:
        content = msg.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and "cache_control" in block:
                del block["cache_control"]
                stripped += 1
        if len(content) == 1 and isinstance(content[0], dict) and content[0].get("type") == "text":
            msg["content"] = content[0]["text"]
    if stripped > 0:
        print(f"🔧 兼容性处理: 剥离了 {stripped} 个 cache_control 字段（非 Claude 模型）")


def _normalize_tool_chains_by_id(messages: list) -> list:
    """按 tool_call_id 把历史工具结果归位到对应 assistant(tool_calls) 后面。"""
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
    emitted_tool_ids = set()
    moved_tools = 0

    for msg in messages:
        if msg.get("role") == "tool":
            tool_call_id = msg.get("tool_call_id")
            # 只要本批消息里存在对应 assistant(tool_calls)，tool 就不要在原位置输出；
            # 等遇到对应 assistant 时再统一输出到它下面，避免“结果跑到调用上面”。
            if tool_call_id in all_call_ids:
                continue
            normalized.append(msg)
            continue

        normalized.append(msg)

        if msg.get("role") == "assistant" and msg.get("tool_calls"):
            for tc in msg.get("tool_calls", []):
                call_id = tc.get("id")
                if not call_id or call_id in emitted_tool_ids:
                    continue
                tools = tools_by_id.get(call_id) or []
                if tools:
                    normalized.extend(tools)
                    emitted_tool_ids.add(call_id)
                    moved_tools += len(tools)

    if moved_tools:
        print(f"🔧 分区模式: 按tool_call_id归位{moved_tools}条历史tool结果")
    return normalized







def _repair_tool_call_ids_by_adjacency(messages: list, session_id: str = "", reason: str = "") -> list:
    """
    修复同一条历史链里 assistant(tool_calls).id 与紧随其后的 tool.tool_call_id 不一致的问题。

    不靠字符串相似度；只按 OpenAI 工具协议的邻接关系修：
        assistant(tool_calls=[A])
        tool(tool_call_id=B)
    若 B 不属于 A 集合，则按顺序改成 A。
    """
    if not messages:
        return messages

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
            add_dashboard_log("info", log_msg, category="chat", session_id=session_id)
        except Exception:
            print(log_msg)

    return repaired


def _map_tool_ids_to_db_pending(db_msgs: list, tool_messages: list) -> dict:
    """
    保存 tool 结果前，把客户端 tool_call_id 映射回 DB 中最近仍在等待结果的 assistant(tool_calls).id。
    不使用字符串后缀/包含关系，只按 DB 中最近未满足的 assistant(tool_calls) 和本轮 tool 顺序配对。
    """
    if not db_msgs or not tool_messages:
        return {}

    saved_tool_ids = {
        m.get("tool_call_id")
        for m in db_msgs
        if m.get("role") == "tool" and m.get("tool_call_id")
    }

    pending_ids = []
    for m in reversed(db_msgs):
        if m.get("role") == "assistant" and m.get("tool_calls"):
            ids = [tc.get("id") for tc in (m.get("tool_calls") or []) if tc.get("id")]
            remaining = [i for i in ids if i not in saved_tool_ids]
            if remaining:
                pending_ids = remaining
                break

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






def _convert_replacement_groups(replacement: str) -> str:
    """把 Dashboard 里更直观的 $1/$2 替换写法转成 Python re.sub 的 \\1/\\2。"""
    return re.sub(r'\$(\d+)', r'\\\1', replacement or "")


def apply_response_transform_rules(text: str) -> str:
    """按配置的正则规则转换非流式 assistant 文本。规则格式：pattern => replacement，一行一条。"""
    if not RESPONSE_TRANSFORM_ENABLED or not isinstance(text, str) or not text:
        return text

    rules_text = RESPONSE_TRANSFORM_RULES or ""
    if not rules_text.strip():
        return text

    transformed = text
    applied = 0
    for raw_line in rules_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=>" not in line:
            continue
        pattern, replacement = line.split("=>", 1)
        pattern = pattern.strip()
        replacement = _convert_replacement_groups(replacement.strip())
        if not pattern:
            continue
        try:
            new_text = re.sub(pattern, replacement, transformed, flags=re.S)
            if new_text != transformed:
                applied += 1
                transformed = new_text
        except Exception as e:
            print(f"⚠️ 响应转换规则无效，已跳过: {pattern} ({e})")

    if applied:
        print(f"🔁 非流式响应转换: 应用 {applied} 条规则")
    return transformed


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


def _clean_current_user_content_preserve_multimodal(content, history: list = None, shorten_time: bool = False) -> tuple:
    """
    清理当前用户消息里的 Operit 环境/记忆/proxy 文本附件，同时保留多模态内容。

    规则：
    - str content：沿用旧逻辑，清理白名单附件，不匹配的附件原样保留。
    - list content：只处理 type=text 的文本块；image_url/input_image/file 等非文本块原样保留。
    - 不匹配环境/记忆/proxy 规则的 <attachment> 由 extract_* 内部原样返回，不删除。
    """
    env_parts = []
    operit_memory_parts = []
    time_text = ""

    def _clean_one_text(text: str) -> str:
        nonlocal time_text
        if not isinstance(text, str):
            return text

        cleaned, env_text, attachment_time = extract_environment_bundle_from_text(text)
        cleaned, operit_memory_text = extract_operit_memory_attachment_from_text(cleaned)
        cleaned, proxy_env_text, proxy_time = extract_proxy_sender_context_from_text(cleaned)

        if env_text:
            env_parts.append(env_text)
        if proxy_env_text:
            env_parts.append(proxy_env_text)
        if operit_memory_text:
            operit_memory_parts.append(operit_memory_text)

        raw_time = attachment_time or proxy_time
        if raw_time and not time_text:
            time_text = _shorten_client_timestamp(raw_time, history) if shorten_time else raw_time

        return cleaned

    if isinstance(content, list):
        new_blocks = []
        first_text_index = None

        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                new_text = _clean_one_text(block.get("text", ""))
                if new_text and new_text.strip():
                    new_block = dict(block)
                    new_block["text"] = new_text
                    if first_text_index is None:
                        first_text_index = len(new_blocks)
                    new_blocks.append(new_block)
            else:
                # 非文本块原样保留：image_url / input_image / file / 任何自定义附件
                new_blocks.append(block)

        if time_text:
            if first_text_index is not None:
                blk = dict(new_blocks[first_text_index])
                blk["text"] = f"{time_text}{blk.get('text', '')}"
                new_blocks[first_text_index] = blk
            else:
                new_blocks.insert(0, {"type": "text", "text": time_text})

        cleaned_content = new_blocks if new_blocks else ""
    else:
        cleaned_text = _clean_one_text(content if isinstance(content, str) else str(content or ""))
        if time_text:
            cleaned_text = f"{time_text}{cleaned_text}"
        cleaned_content = cleaned_text

    env_text_final = "\n\n".join(part for part in env_parts if part)
    operit_memory_final = "\n\n".join(part for part in operit_memory_parts if part)
    return cleaned_content, env_text_final, operit_memory_final


def extract_environment_bundle_from_text(text: str) -> tuple[str, str, str]:
    """识别并压缩 Operit 注入的 text/plain 环境附件。
    返回: (清理后的用户文本, 轻量环境上下文, 附件时间戳)
    """
    if not isinstance(text, str) or "<attachment" not in text:
        return text, "", ""

    env_lines = []
    attachment_time = ""

    def repl(match):
        nonlocal attachment_time, env_lines
        attrs = match.group(1) or ""
        body = match.group(2) or ""
        filename_match = re.search(r'filename="([^"]+)"', attrs)
        filename = filename_match.group(1) if filename_match else ""

        markers = ("【当前时间】", "【当前电量】", "【当前天气】", "【应用使用时长】")
        if not any(m in body for m in markers) and not filename.startswith("Time:"):
            return match.group(0)

        time_match = re.search(r'【当前时间】\s*([0-9]{4}-[0-9]{2}-[0-9]{2})\s+([0-9]{2}:[0-9]{2})', body)
        if time_match:
            try:
                dt = datetime.strptime(time_match.group(1) + " " + time_match.group(2), "%Y-%m-%d %H:%M")
                attachment_time = f"[{dt.strftime('%m-%d %H:%M')}]"
            except Exception:
                attachment_time = f"[{time_match.group(2)}]"

        battery_match = re.search(r'【当前电量】.*?电量:\s*([^\n]+).*?状态:\s*([^\n]+)', body, re.S)
        if battery_match:
            env_lines.append(f"电量: {battery_match.group(1).strip()}，{battery_match.group(2).strip()}")

        weather_block = re.search(r'【当前天气】(.*?)(?:【|$)', body, re.S)
        if weather_block:
            wb = weather_block.group(1).strip()
            if wb and "错误:" not in wb and "无法获取" not in wb:
                one_line = "；".join(line.strip() for line in wb.splitlines() if line.strip())
                if one_line:
                    env_lines.append(f"天气: {one_line}")

        app_block = re.search(r'【应用使用时长】(.*?)(?:$)', body, re.S)
        if app_block:
            apps = []
            for app, duration in re.findall(r'应用:\s*([^\n]+).*?使用时长:\s*([^\n]+)', app_block.group(1), re.S):
                apps.append(f"{app.strip()} {_format_hm_duration(duration)}")
                if len(apps) >= 3:
                    break
            if apps:
                env_lines.append("应用使用: " + "，".join(apps))

        return ""

    cleaned = re.sub(r'<attachment([^>]*)>(.*?)</attachment>', repl, text, flags=re.S).strip()
    env_text = "【当前环境】\n" + "\n".join(env_lines) if env_lines else ""
    return cleaned, env_text, attachment_time


def extract_operit_memory_attachment_from_text(text: str) -> tuple[str, str]:
    """识别 Operit 原生记忆库手动注入的相关记忆附件。
    返回: (清理后的用户文本, 格式化后的记忆上下文)
    """
    if not isinstance(text, str) or "<attachment" not in text or "相关记忆" not in text:
        return text, ""

    memory_bodies = []

    def repl(match):
        attrs = match.group(1) or ""
        body = (match.group(2) or "").strip()
        filename_match = re.search(r'filename="([^"]+)"', attrs)
        filename = filename_match.group(1) if filename_match else ""

        if filename != "相关记忆" and "【相关记忆】" not in body:
            return match.group(0)
        if not body:
            return ""

        memory_bodies.append(body)
        return ""

    cleaned = re.sub(r'<attachment([^>]*)>(.*?)</attachment>', repl, text, flags=re.S).strip()
    if not memory_bodies:
        return cleaned, ""

    memory_text = "\n\n".join(memory_bodies).strip()
    formatted = f"""【从operit记忆库中检索到的相关记忆】
{memory_text}

# 记忆应用
- 像朋友般自然运用这些记忆，不刻意展示
- 仅在相关话题出现时引用，避免主动提及
- 对重要信息（如健康、日期、约定）保持一致性
- 新信息与记忆冲突时，以新信息为准
- 模糊记忆可表达不确定性：\"记得你似乎说过...\"

# 交流方式
- 自然引用：\"记得你说过...\"或\"上次我们聊到...\"
- 避免机械式表达如\"根据我的记忆...\"或\"检索到的信息显示...\"
- 共同经历可温情回忆：\"上次那个事挺好玩的\"

记忆是丰富对话的工具，而非对话焦点。"""
    return cleaned, formatted


def extract_proxy_sender_context_from_text(text: str) -> tuple[str, str, str]:
    """识别 Operit 插件注入的 proxy_sender 上下文，例如一起听歌。
    返回: (用户真实文本, 轻量上下文, 附件时间戳)
    """
    if not isinstance(text, str) or "<proxy_sender" not in text or "用户说" not in text:
        return text, "", ""

    split_match = re.split(r'用户说[:：]\s*', text, maxsplit=1)
    if len(split_match) < 2:
        return text, "", ""

    header = split_match[0]
    user_text = split_match[1].strip()
    env_lines = []
    proxy_time = ""

    time_match = re.search(r'当前时间[:：]\s*([0-9]{4})/([0-9]{1,2})/([0-9]{1,2})\s+([0-9]{1,2}:[0-9]{2})', header)
    if time_match:
        try:
            month = int(time_match.group(2))
            day = int(time_match.group(3))
            hm = time_match.group(4)
            proxy_time = f"[{month:02d}-{day:02d} {hm}]"
        except Exception:
            proxy_time = ""

    if "一起听音乐" in header or "当前歌曲" in header or "附近歌词" in header:
        song_match = re.search(r'当前歌曲[:：]\s*([^\n]+)', header)
        play_match = re.search(r'播放时间[:：]\s*([^\n]+)', header)
        if song_match:
            song_line = song_match.group(1).strip()
            if play_match:
                song_line += f" {play_match.group(1).strip()}"
            env_lines.append(f"歌曲: {song_line}")

        lyrics_match = re.search(r'附近歌词[:：]\s*(.*?)(?:\n\s*用户说[:：]|$)', header, re.S)
        if lyrics_match:
            lyrics_lines = [line.rstrip() for line in lyrics_match.group(1).splitlines() if line.strip()]
            if lyrics_lines:
                env_lines.append("附近歌词:\n" + "\n".join(lyrics_lines))

    env_text = ""
    if env_lines:
        env_text = "【一起听歌】\n" + "\n".join(env_lines)

    return user_text, env_text, proxy_time


async def generate_summary(messages: list, session_id: str = "") -> str:
    """调用轻量模型压缩A区消息为摘要"""
    if not messages:
        return ""
    
    conversation_text = ""
    for msg in messages:
        role_label = "用户" if msg['role'] == 'user' else "AI"
        content = msg['content'] if isinstance(msg['content'], str) else str(msg['content'])
        conversation_text += f"{role_label}: {content}\n\n"
    
    prompt = f"""请为分区缓存生成一段可长期承接上下文的滚动摘要。

要求：
- 保留用户明确说过的重要事实、偏好、计划、约定、情绪变化、项目进展和关键互动
- 保留会影响后续对话理解的细节，不要只写一句泛泛总结
- 去掉纯寒暄、重复内容和无意义语气词
- 使用第三人称或中性叙述，避免加入模型没有看到的新信息
- 如果内容较多，按要点分段；不要强行压缩到几十字
- 目标长度约 600-1000 字，信息少时可以更短

---
{conversation_text}
---

滚动摘要："""
    
    try:
        headers = {
            "Authorization": f"Bearer {get_memory_api_key()}",
            "Content-Type": "application/json",
        }
        memory_api_base_url = get_memory_api_base_url()
        if not memory_api_base_url:
            print("⚠️ 摘要生成跳过: MEMORY_API_BASE_URL 未设置（不会回退到主 API_BASE_URL）")
            return ""

        summary_model = os.getenv("MEMORY_MODEL", "anthropic/claude-haiku-4")

        if "openrouter" in memory_api_base_url:
            headers["HTTP-Referer"] = EXTRA_REFERER
            headers["X-Title"] = EXTRA_TITLE

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(memory_api_base_url, headers=headers, json={
                "model": summary_model,
                "max_tokens": 9000,
                "temperature": 0.2,
                "messages": [{"role": "user", "content": prompt}],
            })
            if response.status_code == 200:
                data = response.json()
                if "choices" in data:
                    choice = data["choices"][0]
                    summary = choice["message"]["content"].strip()
                    finish_reason = choice.get("finish_reason", "")
                    if finish_reason:
                        print(f"📝 摘要生成完成: {len(summary)}字 (压缩{len(messages)}条消息, finish_reason={finish_reason})")
                    else:
                        print(f"📝 摘要生成完成: {len(summary)}字 (压缩{len(messages)}条消息)")
                    if finish_reason in ("length", "max_tokens"):
                        print("⚠️ 摘要可能被上游截断：finish_reason=length/max_tokens")
                    return summary

        print(f"⚠️ 摘要生成失败: HTTP {response.status_code} {response.text[:500]}")
        return ""
    except Exception as e:
        print(f"⚠️ 摘要生成异常: {e}")
        return ""


def group_by_rounds(history: list) -> list:
    """
    按逻辑轮分组：每个user消息开始一轮，到下一个user前结束。
    一轮可能包含: [user, assistant] 或 [user, assistant(tool_calls), tool, assistant] 等。
    """
    rounds = []
    current_round = []
    for msg in history:
        if msg['role'] == 'user' and current_round:
            rounds.append(current_round)
            current_round = []
        current_round.append(msg)
    if current_round:
        rounds.append(current_round)
    return rounds


def _should_rotate(b_rounds_count: int, X: int, a_msgs: list) -> bool:
    """
    判断是否应该触发A区→摘要的轮转。
    
    rounds模式（默认）：B区轮数 >= X 时触发
    time模式：A区最早消息距今 >= 时间窗口 时触发（短时间内大量消息不频繁摘要）
    """
    if b_rounds_count == 0:
        return False
    
    if CACHE_PARTITION_TRIGGER == "time":
        a_first_time = None
        for msg in a_msgs:
            t = msg.get('created_at')
            if t:
                a_first_time = t
                break
        
        if a_first_time:
            now = datetime.now(timezone.utc)
            if a_first_time.tzinfo is None:
                a_first_time = a_first_time.replace(tzinfo=timezone.utc)
            age_minutes = (now - a_first_time).total_seconds() / 60
            return age_minutes >= CACHE_PARTITION_WINDOW
        
        return b_rounds_count >= X
    
    return b_rounds_count >= X

# 时间窗口模式下单次请求最大轮转次数（防止一口气压完所有历史）
CACHE_MAX_ROTATIONS = int(os.getenv("CACHE_MAX_ROTATIONS", "2"))


def _apply_breakpoint(msg: dict) -> bool:
    """
    给消息打上 cache_control breakpoint。
    支持 content 为 str 或 list（多模态block数组）两种格式。
    返回 True 表示成功打上，False 表示无法打（比如content为空）。
    """
    content = msg.get('content')
    
    # content 是纯字符串
    if isinstance(content, str) and content.strip():
        msg['content'] = [{"type": "text", "text": content, "cache_control": {"type": "ephemeral"}}]
        return True
    
    # content 是 block 数组（多模态消息）
    if isinstance(content, list):
        # 从后往前找最后一个 text block
        for i in range(len(content) - 1, -1, -1):
            block = content[i]
            if isinstance(block, dict) and block.get("type") == "text" and block.get("text", "").strip():
                block["cache_control"] = {"type": "ephemeral"}
                return True
    
    return False


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


def _prepend_timestamp_to_user_messages(messages: list) -> list:
    """给历史 user 消息加轻量时间戳；assistant/tool 不加。"""
    last_date = None
    stamped = []
    for msg in messages:
        m = dict(msg)
        if m.get('role') == 'user':
            local_dt = _to_local_dt(m.get('created_at'))
            if local_dt:
                show_date = last_date != local_dt.date()
                stamp = f"[{local_dt.strftime('%m-%d %H:%M')}]" if show_date else f"[{local_dt.strftime('%H:%M')}]"
                content = m.get('content')
                if isinstance(content, str):
                    if not re.match(r'^\[[0-9]{2}(?:-[0-9]{2})? [0-9]{2}:[0-9]{2}\]|^\[[0-9]{2}:[0-9]{2}\]', content):
                        m['content'] = f"{stamp}{content}"
                elif isinstance(content, list):
                    for block in content:
                        if isinstance(block, dict) and block.get('type') == 'text':
                            text = block.get('text', '')
                            if not re.match(r'^\[[0-9]{2}(?:-[0-9]{2})? [0-9]{2}:[0-9]{2}\]|^\[[0-9]{2}:[0-9]{2}\]', text):
                                block['text'] = f"{stamp}{text}"
                            break
                last_date = local_dt.date()
        m.pop('created_at', None)
        stamped.append(m)
    return stamped


async def build_partitioned_messages(
    session_id: str,
    all_messages: list,
    base_prompt: str,
    user_message: str,
) -> list:
    """
    分区缓存模式：构建带breakpoint的messages数组。
    
    结构：
    system: [{人设, BP1}]                        ← 永远命中
    messages:
      [摘要blocks（每段一个block）, 最后BP]       ← 尾部追加，前面命中
      [摘要assistant]
      [A区消息... 最后一条BP2]                    ← 正常轮次不变
      [B区消息... 最后一条BP3]                    ← lookback命中
      [当前user: 时间+记忆+消息]                  ← 不缓存
    """
    X = CACHE_PARTITION_X
    
    non_system = [m for m in all_messages if m.get('role') not in ('system', 'developer')]
    
    current_user_msg = None
    history = non_system[:]
    if history and history[-1].get('role') == 'user':
        current_user_msg = history.pop()
    
    # 不在分区构造阶段按“相邻顺序”删除 tool。历史工具链可能乱序，
    # 进入本函数前已按 tool_call_id 尽量归组；剩余非法链统一交给最终 sanitizer 处理，
    # 避免本来可恢复的 tool 在 _normalize_tool_chains_by_id 之前被提前丢弃。
    

    # 按逻辑轮分组（解决tool消息导致的轮计数错乱）
    rounds = group_by_rounds(history)
    total_rounds = len(rounds)
    
    state = await get_session_cache_state(session_id)
    summary_parts = state['summary_parts']
    a_start_round = state['a_start_round']
    
    if total_rounds < X:
        return await _build_basic_cached(history, base_prompt, user_message, current_user_msg)
    
    # 计算A/B区（按逻辑轮切片）
    a_end_round = a_start_round + X
    a_round_groups = rounds[a_start_round : a_end_round]
    b_round_groups = rounds[a_end_round :]
    a_msgs = [msg for rnd in a_round_groups for msg in rnd]
    b_msgs = [msg for rnd in b_round_groups for msg in rnd]
    b_rounds_count = len(b_round_groups)
    
    rotation_count = 0
    max_rotations = CACHE_MAX_ROTATIONS if CACHE_PARTITION_TRIGGER == "time" else 999
    while _should_rotate(b_rounds_count, X, a_msgs) and rotation_count < max_rotations:
        rotation_count += 1
        trigger_info = f"B区{b_rounds_count}轮 >= X={X}" if CACHE_PARTITION_TRIGGER != "time" else f"A区首条消息超出{CACHE_PARTITION_WINDOW}分钟窗口"
        print(f"🔄 轮转#{rotation_count}: session={session_id}, {trigger_info}")
        
        new_summary = await generate_summary(a_msgs, session_id)
        if new_summary:
            summary_parts.append(new_summary)
        
        a_start_round += X
        a_end_round = a_start_round + X
        a_round_groups = rounds[a_start_round : a_end_round]
        b_round_groups = rounds[a_end_round :]
        a_msgs = [msg for rnd in a_round_groups for msg in rnd]
        b_msgs = [msg for rnd in b_round_groups for msg in rnd]
        b_rounds_count = len(b_round_groups)
    
    if rotation_count > 0:
        await save_session_cache_state(session_id, summary_parts, a_start_round)
        summary_total = sum(len(p) for p in summary_parts)
        print(f"🔄 轮转完成(共{rotation_count}次): 摘要{len(summary_parts)}段/{summary_total}字, A区{len(a_msgs)}条, B区{len(b_msgs)}条")
    
    # 拼装messages
    result = []
    if base_prompt:
        result.append({
            "role": "system",
            "content": [{"type": "text", "text": base_prompt, "cache_control": {"type": "ephemeral"}}]
        })
    
    # 摘要区（多block，尾部追加模式）
    if summary_parts:
        blocks = [{"type": "text", "text": "[以下是之前对话的摘要，帮助你回忆上下文]"}]
        for i, part in enumerate(summary_parts):
            item = {"type": "text", "text": part}
            if i == len(summary_parts) - 1:
                item["cache_control"] = {"type": "ephemeral"}
            blocks.append(item)
        result.append({"role": "user", "content": blocks})
        result.append({"role": "assistant", "content": "好的，我已了解之前的对话内容。"})
    
    # A区：剥离tool消息和tool_calls，只保留有文本的user/assistant（节省上下文）
    cleaned_a = []
    for msg in a_msgs:
        if msg.get('role') == 'tool':
            continue
        m = {k: v for k, v in msg.items() if k not in ('created_at', 'tool_calls')}
        if m.get('role') == 'assistant' and not (m.get('content') or '').strip():
            continue
        cleaned_a.append(m)
    
    # A区：从末尾往前找第一条非tool消息打BP
    for j in range(len(cleaned_a) - 1, -1, -1):
        if cleaned_a[j].get('role') != 'tool' and _apply_breakpoint(cleaned_a[j]):
            break
    
    cleaned_a = _prepend_timestamp_to_user_messages(cleaned_a)
    for m in cleaned_a:
        result.append(m)
    
    # B区：先构建去掉created_at的副本，再从末尾往前打BP
    b_cleaned = _prepend_timestamp_to_user_messages(b_msgs)
    
    for j in range(len(b_cleaned) - 1, -1, -1):
        if b_cleaned[j].get('role') != 'tool' and _apply_breakpoint(b_cleaned[j]):
            break
    
    for m in b_cleaned:
        result.append(m)
    
    if current_user_msg:
        current_content, env_text, operit_memory_text = _clean_current_user_content_preserve_multimodal(
            current_user_msg.get('content', ''),
            history=history,
            shorten_time=True,
        )
        result.append({"role": "user", "content": current_content})

        # 环境/插件上下文后置为轻量 system 消息，避免原始注入污染用户正文。
        if env_text:
            result.append({"role": "system", "content": env_text})

        # 相关记忆后置：先让模型看到用户本轮原话，再参考检索记忆，降低“记忆抢注意力”的概率。
        if MEMORY_ENABLED and MEMORY_EXTRACT_ENABLED and user_message:
            mem_text = await build_memory_text(user_message)
            if mem_text:
                result.append({"role": "system", "content": mem_text})

        # Operit 原生记忆附件放在最底部，按用户手动检索结果使用。
        if operit_memory_text:
            result.append({"role": "system", "content": operit_memory_text})
    
    bp_count = 1 + (1 if summary_parts else 0) + (1 if cleaned_a else 0) + (1 if b_msgs else 0)
    summary_total = sum(len(p) for p in summary_parts)
    tool_stripped = len(a_msgs) - len(cleaned_a)
    a_info = f"A区{len(cleaned_a)}条({len(a_round_groups)}轮)" + (f"[剥离{tool_stripped}条tool]" if tool_stripped else "")
    print(f"🔒 分区缓存: BP×{bp_count} | 摘要{'有' if summary_parts else '无'}({len(summary_parts)}段/{summary_total}字) | {a_info} | B区{len(b_msgs)}条({b_rounds_count}轮) | 总{len(result)}条messages")
    return result


async def _build_basic_cached(
    history: list,
    base_prompt: str,
    user_message: str,
    current_user_msg: dict,
) -> list:
    """基础版prompt caching（历史不够分区时的降级模式）"""
    result = []
    if base_prompt:
        result.append({
            "role": "system",
            "content": [{"type": "text", "text": base_prompt, "cache_control": {"type": "ephemeral"}}]
        })
    
    h_cleaned = _prepend_timestamp_to_user_messages(history)
    
    # 从末尾往前找第一条非tool消息打BP
    for j in range(len(h_cleaned) - 1, -1, -1):
        if h_cleaned[j].get('role') != 'tool' and _apply_breakpoint(h_cleaned[j]):
            break
    
    for m in h_cleaned:
        result.append(m)
    
    if current_user_msg:
        current_content, env_text, operit_memory_text = _clean_current_user_content_preserve_multimodal(
            current_user_msg.get('content', ''),
            history=history,
            shorten_time=False,
        )
        result.append({"role": "user", "content": current_content})

        # 环境/插件上下文后置为轻量 system 消息，避免原始注入污染用户正文。
        if env_text:
            result.append({"role": "system", "content": env_text})

        # 相关记忆后置：先让模型看到用户本轮原话，再参考检索记忆，降低“记忆抢注意力”的概率。
        if MEMORY_ENABLED and MEMORY_EXTRACT_ENABLED and user_message:
            mem_text = await build_memory_text(user_message)
            if mem_text:
                result.append({"role": "system", "content": mem_text})

        # Operit 原生记忆附件放在最底部，按用户手动检索结果使用。
        if operit_memory_text:
            result.append({"role": "system", "content": operit_memory_text})
    
    bp_count = 1 + (1 if history else 0)
    print(f"🔒 基础缓存(降级): BP×{bp_count} | 历史{len(history)}条 | 总{len(result)}条messages")
    return result


async def build_memory_text(user_message: str) -> str:
    """搜索记忆并格式化为注入文本（分区缓存模式用）"""
    if MAX_MEMORIES_INJECT <= 0:
        return ""
    try:
        memories = await search_memories(user_message, limit=MAX_MEMORIES_INJECT)
        if not memories:
            return ""
        
        memory_lines = []
        for mem in memories:
            date_str = ""
            if mem.get("created_at"):
                try:
                    utc_str = str(mem['created_at'])[:19]
                    utc_dt = datetime.strptime(utc_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                    local_dt = utc_dt + timedelta(hours=TIMEZONE_HOURS)
                    date_str = f"[{local_dt.strftime('%Y-%m-%d')}] "
                except:
                    date_str = f"[{str(mem['created_at'])[:10]}] "
            memory_lines.append(f"- {date_str}{mem['content']}")
        
        print(f"📚 注入了 {len(memories)} 条相关记忆")
        return "【此刻，你脑海中混入一滩噪音】\n" + "\n".join(memory_lines) + "\n\n注:请假装什么都没发生，继续当前对话。"
    except Exception as e:
        print(f"⚠️ 记忆检索失败: {e}")
        return ""


# ============================================================
# 后台记忆处理
# ============================================================

def clean_user_message_for_log(user_msg: str, history: list = None) -> str:
    """保存到对话记录前，清理附件/proxy注入，避免Dashboard显示大段原始上下文。"""
    if not isinstance(user_msg, str):
        return user_msg

    cleaned = user_msg
    time_text = ""

    cleaned, _env_text, attachment_time = extract_environment_bundle_from_text(cleaned)
    if attachment_time:
        time_text = attachment_time

    cleaned, _operit_memory_text = extract_operit_memory_attachment_from_text(cleaned)

    cleaned, _proxy_env_text, proxy_time = extract_proxy_sender_context_from_text(cleaned)
    if proxy_time and not time_text:
        time_text = proxy_time

    cleaned = (cleaned or "").strip()
    if time_text and cleaned and not re.match(r'^\[[0-9]{2}(?:-[0-9]{2})? [0-9]{2}:[0-9]{2}\]|^\[[0-9]{2}:[0-9]{2}\]', cleaned):
        time_text = _shorten_client_timestamp(time_text, history)
        cleaned = f"{time_text}{cleaned}"
    return cleaned or user_msg


async def persist_assistant_tool_calls_sync(session_id: str, user_msg: str, assistant_msg: str, model: str, assistant_tool_calls: list = None, assistant_reasoning: str = None) -> bool:
    """同步保存首次工具调用的 user + assistant(tool_calls)，避免下一轮 tool 结果先到而 DB 还没写完。"""
    if not assistant_tool_calls:
        return False
    tool_call_ids = [tc.get("id") for tc in assistant_tool_calls if tc.get("id")]
    if not tool_call_ids:
        return False
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            exists = await conn.fetchval(
                """
                SELECT 1
                FROM conversations
                WHERE session_id = $1
                  AND role = 'assistant'
                  AND metadata IS NOT NULL
                  AND EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(metadata::jsonb -> 'tool_calls') AS elem
                    WHERE elem ->> 'id' = ANY($2::text[])
                  )
                LIMIT 1
                """,
                session_id, tool_call_ids
            )
        if exists:
            print(f"🔧 同步存储: assistant(tool_calls)已存在，跳过 ids={tool_call_ids}")
            return False

        recent_log_history = []
        try:
            recent_log_history = await get_conversation_messages(session_id, limit=20)
        except Exception as e:
            print(f"⚠️ 同步存储: 读取最近对话失败，直接保存原始user: {e}")
        clean_user_msg = clean_user_message_for_log(user_msg, recent_log_history) if user_msg else user_msg
        ast_meta_dict = {"tool_calls": assistant_tool_calls}
        if assistant_reasoning:
            ast_meta_dict["reasoning_content"] = assistant_reasoning
        await save_message(session_id, "user", clean_user_msg or "", model)
        await save_message(session_id, "assistant", assistant_msg or "", model, metadata=json.dumps(ast_meta_dict))
        print(f"🔧 同步存储: user + assistant(tool_calls) 已写入DB ids={tool_call_ids}")
        return True
    except Exception as e:
        print(f"⚠️ 同步存储 assistant(tool_calls) 失败，将回退后台异步保存: {e}")
        return False



async def process_memories_background(session_id: str, user_msg: str, assistant_msg: str, model: str, context_messages: list = None, skip_conversation_log: bool = False, tool_messages: list = None, assistant_tool_calls: list = None, assistant_reasoning: str = None):
    """
    后台异步：存储对话 + 提取记忆（不阻塞主流程）
    
    记忆提取受 MEMORY_EXTRACT_INTERVAL 控制：
    - 0: 禁用自动提取
    - 1: 每轮提取（默认）
    - N: 每 N 轮提取一次
    对话记录始终保存，不受间隔影响（除非 skip_conversation_log=True）。
    
    context_messages: 客户端发来的原始对话上下文（不含system prompt），
                      用于让提取模型从完整上下文中提取记忆。
    skip_conversation_log: 跳过对话存储（标题生成等辅助请求时使用）
    tool_messages: 客户端发来的工具结果消息列表
    assistant_tool_calls: response中assistant的工具调用列表（如果有）
    assistant_reasoning: response中assistant的reasoning_content（deepseek thinking mode）
    """
    global _round_counter
    
    try:
        # Debug: 打印存储分支判断依据
        print(f"💾 process_memories_background: user_msg={bool(user_msg)}, tool_messages={len(tool_messages) if tool_messages else 0}, "
              f"assistant_tool_calls={len(assistant_tool_calls) if assistant_tool_calls else 0}, skip={skip_conversation_log}")
        if tool_messages:
            print(f"💾 tool详情: {[{'role': m.get('role'), 'tool_call_id': m.get('tool_call_id', '?')} for m in tool_messages]}")
        
        # 1. 存储对话记录（除非明确跳过）
        recent_log_history = []
        if user_msg:
            try:
                recent_log_history = await get_conversation_messages(session_id, limit=20)
            except Exception as e:
                print(f"⚠️ 读取最近对话用于日志时间戳缩短失败: {e}")
        clean_user_msg = clean_user_message_for_log(user_msg, recent_log_history) if user_msg else user_msg
        if skip_conversation_log:
            print(f"⏭️  跳过对话存储（辅助请求）")
        elif tool_messages:
            # 工具结果轮次：存tool消息 + assistant回复（user消息在之前的轮次已存过）
            # 构建客户端短id→DB原始长id映射：按最近未满足的 assistant(tool_calls) 顺序配对
            _bg_id_map = {}
            try:
                _bg_recent_rows = await get_conversation_messages(session_id, limit=50)
                _bg_recent_msgs = []
                for _row in (_bg_recent_rows or []):
                    _msg = db_row_to_message(_row)
                    _msg["created_at"] = _row.get("created_at")
                    _bg_recent_msgs.append(_msg)
                _bg_id_map = _map_tool_ids_to_db_pending(_bg_recent_msgs, tool_messages)
                _bg_mapped_diff = {k: v for k, v in _bg_id_map.items() if k != v}
                if _bg_mapped_diff:
                    add_dashboard_log("info", f"🔧 tool_call_id映射(后台保存): {_bg_mapped_diff}", category="chat", session_id=session_id)
            except Exception as _e:
                print(f"⚠️后台存储: id映射构建失败: {_e}")

            for tm in tool_messages:
                meta_dict = {}
                tool_call_id = tm.get("tool_call_id")
                db_tool_call_id = _bg_id_map.get(tool_call_id, tool_call_id) if tool_call_id else tool_call_id
                if db_tool_call_id:
                    meta_dict["tool_call_id"] = db_tool_call_id
                if tm.get("name"):
                    meta_dict["name"] = tm["name"]
                meta = json.dumps(meta_dict) if meta_dict else None

                if db_tool_call_id:
                    try:
                        pool = await get_pool()
                        async with pool.acquire() as conn:
                            exists = await conn.fetchval(
                                """
                                SELECT 1
                                FROM conversations
                                WHERE session_id = $1
                                  AND role = 'tool'
                                  AND metadata::jsonb ->> 'tool_call_id' = $2
                                LIMIT 1
                                """,
                                session_id, db_tool_call_id
                            )
                        if exists:
                            print(f"🔧 存储: 跳过重复tool结果 id={db_tool_call_id}")
                            continue
                    except Exception as e:
                        print(f"⚠️ tool结果查重失败，继续保存: {e}")

                await save_message(session_id, "tool", tm.get("content", ""), model, metadata=meta)
            
            if assistant_msg or assistant_tool_calls:
                ast_meta_dict = {}
                if assistant_tool_calls:
                    ast_meta_dict["tool_calls"] = assistant_tool_calls
                if assistant_reasoning:
                    ast_meta_dict["reasoning_content"] = assistant_reasoning
                ast_meta = json.dumps(ast_meta_dict) if ast_meta_dict else None
                await save_message(session_id, "assistant", assistant_msg or "", model, metadata=ast_meta)
                print(f"🔧 存储: {len(tool_messages)}条tool + 1条assistant" + (" (含tool_calls)" if assistant_tool_calls else "") + (" (含reasoning)" if assistant_reasoning else ""))
        else:
            # 普通对话或首次工具调用
            ast_meta_dict = {}
            if assistant_tool_calls:
                ast_meta_dict["tool_calls"] = assistant_tool_calls
            if assistant_reasoning:
                ast_meta_dict["reasoning_content"] = assistant_reasoning
            assistant_meta = json.dumps(ast_meta_dict) if ast_meta_dict else None
            
            if assistant_tool_calls:
                # 首次工具调用：assistant回复包含tool_calls，存user + assistant(tool_calls)
                await save_message(session_id, "user", clean_user_msg, model)
                await save_message(session_id, "assistant", assistant_msg or "", model, metadata=assistant_meta)
                print(f"🔧 存储: user + assistant (含{len(assistant_tool_calls)}个tool_calls)" + (" (含reasoning)" if assistant_reasoning else ""))
            else:
                # 纯文字对话：re-roll检测 + 存user + assistant
                last_user = await get_last_user_content(session_id)
                if last_user and last_user.strip() == clean_user_msg.strip():
                    updated = await update_last_assistant_message(session_id, assistant_msg, model)
                    if updated:
                        print(f"🔄 检测到re-roll，已覆盖最后一条assistant回复")
                    else:
                        await save_message(session_id, "user", clean_user_msg, model)
                        await save_message(session_id, "assistant", assistant_msg, model, metadata=assistant_meta)
                else:
                    await save_message(session_id, "user", clean_user_msg, model)
                    await save_message(session_id, "assistant", assistant_msg, model, metadata=assistant_meta)
        
        # 2. 检查是否需要提取记忆
        if not MEMORY_EXTRACT_ENABLED:
            add_dashboard_log("skip", "⏭️  记忆提取已关闭（MEMORY_EXTRACT_ENABLED=false）", session_id=session_id)
            return
        
        if MEMORY_EXTRACT_INTERVAL == 0:
            add_dashboard_log("skip", "⏭️  记忆自动提取已禁用，跳过", session_id=session_id)
            return
        
        # 使用“书签”判断提取范围：上次提到第几轮，本次从下一轮继续。
        # 书签存在 gateway_config，不改 conversations 表，也不影响分区压缩。
        cursor_key = f"memory_extract_cursor:{session_id}"
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                user_round_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM conversations WHERE session_id = $1 AND role = 'user'",
                    session_id
                )
                session_memory_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM memories WHERE source_session = $1",
                    session_id
                )
            last_extracted_round = int(await get_gateway_config(cursor_key, "0") or 0)
        except Exception as e:
            add_dashboard_log("warn", f"⚠️ 读取记忆提取书签失败，回退到进程计数: {e}", session_id=session_id)
            _round_counter += 1
            user_round_count = _round_counter
            session_memory_count = 0
            last_extracted_round = max(0, user_round_count - 1)

        pending_rounds = max(0, user_round_count - last_extracted_round)
        if pending_rounds < MEMORY_EXTRACT_INTERVAL:
            add_dashboard_log("skip", f"⏭️ 书签在第 {last_extracted_round} 轮，当前第 {user_round_count} 轮，攒了 {pending_rounds}/{MEMORY_EXTRACT_INTERVAL} 轮，跳过", session_id=session_id)
            return

        extract_start_round = last_extracted_round + 1
        extract_end_round = user_round_count
        add_dashboard_log("run", f"📝 提取范围：第 {extract_start_round}-{extract_end_round} 轮（书签原在第 {last_extracted_round} 轮，已有本会话记忆 {session_memory_count} 条）", session_id=session_id)
        
        # 3. 获取已有记忆，传给提取模型做对比去重
        existing = await get_recent_memories(limit=80)
        existing_contents = [r["content"] for r in existing]
        
        # 4. 构建用于提取的消息列表
        #    使用 conversations 表中已经保存的清洗后消息，和 Dashboard 对话记录同源。
        #    注意：这里单独按轮次范围读取，不复用 get_conversation_messages()，避免影响分区压缩。
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    WITH ordered AS (
                        SELECT role, content, created_at, id,
                               SUM(CASE WHEN role = 'user' THEN 1 ELSE 0 END)
                               OVER (ORDER BY created_at ASC, id ASC) AS user_round
                        FROM conversations
                        WHERE session_id = $1 AND role IN ('user', 'assistant')
                    )
                    SELECT role, content
                    FROM ordered
                    WHERE user_round >= $2 AND user_round <= $3
                    ORDER BY created_at ASC, id ASC
                    """,
                    session_id, extract_start_round, extract_end_round
                )
            messages_for_extraction = [
                {"role": r["role"], "content": r["content"]}
                for r in rows
                if r["content"]
            ]
            add_dashboard_log("run", f"🧾 从数据库对话记录读取第 {extract_start_round}-{extract_end_round} 轮，共 {len(messages_for_extraction)} 条清洗后消息", session_id=session_id)
        except Exception as e:
            add_dashboard_log("warn", f"⚠️ 读取数据库对话记录失败，回退到当前轮提取: {e}", session_id=session_id)
            messages_for_extraction = [
                {"role": "user", "content": user_msg},
                {"role": "assistant", "content": assistant_msg},
            ]

        if not messages_for_extraction:
            add_dashboard_log("empty", "🫧 没有可用于记忆提取的对话记录", session_id=session_id)
            return
        
        import memory_extractor as _me_mod
        extractor_base = getattr(_me_mod, "MEMORY_API_BASE_URL", "")
        extractor_model = getattr(_me_mod, "MEMORY_MODEL", "")
        if not extractor_base:
            add_dashboard_log("error", "⚠️ MEMORY_API_BASE_URL 为空，记忆提取没有发出请求", session_id=session_id)
            return
        add_dashboard_log("run", f"📡 请求记忆模型：{extractor_model} @ {extractor_base}", session_id=session_id)
        new_memories = await extract_memories(messages_for_extraction, existing_memories=existing_contents)
        
        # 过滤垃圾记忆（不靠模型自觉，硬过滤）
        META_BLACKLIST = [
            "记忆库", "记忆系统", "检索", "没有被记录", "没有被提取",
            "记忆遗漏", "尚未被记录", "写入不完整", "检索功能",
            "系统没有返回", "关键词匹配", "语义匹配", "语义检索",
            "阈值", "数据库", "seed", "导入", "部署",
            "bug", "debug", "端口", "网关",
        ]
        
        filtered_memories = []
        for mem in new_memories:
            content = mem["content"]
            if any(kw in content for kw in META_BLACKLIST):
                print(f"🚫 过滤掉meta记忆: {content[:60]}...")
                continue
            filtered_memories.append(mem)
        
        for mem in filtered_memories:
            await save_memory(
                content=mem["content"],
                importance=mem["importance"],
                source_session=session_id,
            )
        
        extract_debug = getattr(_me_mod, "LAST_EXTRACTION_DEBUG", {}) or {}
        extract_status = extract_debug.get("status")
        if extract_status == "parsed":
            await set_gateway_config(cursor_key, str(extract_end_round))
            add_dashboard_log("success", f"🔖 记忆提取书签已移动到第 {extract_end_round} 轮", session_id=session_id)
        else:
            detail = extract_debug.get("message") or ""
            raw_preview = extract_debug.get("raw_preview") or ""
            suffix = f"：{detail}" if detail else ""
            if raw_preview:
                suffix += f"；返回片段：{raw_preview[:180]}"
            add_dashboard_log("warn", f"⚠️ 记忆提取未确认成功，书签停在第 {last_extracted_round} 轮（状态: {extract_status}）{suffix}", session_id=session_id)

        if filtered_memories:
            total = await get_all_memories_count()
            add_dashboard_log("success", f"💾 已保存 {len(filtered_memories)} 条新记忆（过滤了 {len(new_memories) - len(filtered_memories)} 条），总计 {total} 条", session_id=session_id)
        else:
            add_dashboard_log("empty", f"🫧 本轮未提取到新记忆（模型返回 {len(new_memories)} 条，过滤后 0 条）", session_id=session_id)
            
    except Exception as e:
        add_dashboard_log("error", f"⚠️ 后台记忆处理失败: {e}", session_id=session_id if 'session_id' in locals() else "")


# ============================================================
# API 接口
# ============================================================

@app.get("/")
async def health_check():
    """健康检查"""
    memory_count = 0
    if MEMORY_ENABLED:
        try:
            memory_count = await get_all_memories_count()
        except:
            pass
    
    return {
        "status": "running",
        "gateway": "AI Memory Gateway v2.0",
        "system_prompt_loaded": len(SYSTEM_PROMPT) > 0,
        "system_prompt_length": len(SYSTEM_PROMPT),
        "memory_enabled": MEMORY_ENABLED,
        "memory_count": memory_count,
        "memory_extract_interval": MEMORY_EXTRACT_INTERVAL,
    }


@app.get("/v1/models")
async def list_models():
    """模型列表（让客户端不报错）"""
    return {
        "object": "list",
        "data": [
            {
                "id": DEFAULT_MODEL,
                "object": "model",
                "created": 1700000000,
                "owned_by": "ai-memory-gateway",
            }
        ],
    }


@app.post("/v1/chat/completions")
async def chat_completions(request: Request):
    """核心转发接口"""
    if not API_KEY:
        return JSONResponse(
            status_code=500,
            content={"error": "API_KEY 未设置，请在环境变量中配置"},
        )
    
    body = await request.json()
    messages = body.get("messages", [])

    # ---------- 入口诊断日志（无条件打印，定位请求是否真的进入网关） ----------
    try:
        _entry_msg_count = len(messages)
        _entry_body_chars = len(json.dumps(body, ensure_ascii=False))
        _entry_has_summary = any(
            ("摘要" in str(m.get("content", ""))) or ("summary" in str(m.get("content", "")).lower())
            for m in messages
        )
        add_dashboard_log(
            "info",
            f"入口收到主对话请求：messages={_entry_msg_count}，body≈{_entry_body_chars}字，含摘要关键词={_entry_has_summary}",
            category="chat",
        )
    except Exception as _e:
        print(f"⚠️ 入口诊断日志失败: {_e}", flush=True)

    # ---------- 检测是否应跳过对话存储 ----------
    # 客户端通过header显式声明（如标题生成等辅助请求）
    skip_conversation_log = request.headers.get("X-Skip-Conversation-Log", "").lower() == "true"
    
    # ---------- 提取用户最新消息 ----------
    user_message = ""
    for msg in reversed(messages):
        if msg.get("role") == "user":
            content = msg.get("content", "")
            if isinstance(content, str):
                user_message = content
            elif isinstance(content, list):
                user_message = " ".join(
                    item.get("text", "") for item in content
                    if isinstance(item, dict) and item.get("type") == "text"
                )
            break
    
    # ---------- 构建 system prompt ----------
    # 先保存原始对话消息（不含 system prompt），用于记忆提取
    original_messages = [msg for msg in messages if msg.get("role") != "system"]
    
    # ---------- 检测工具调用消息 ----------
    tool_messages = [m for m in messages if m.get("role") == "tool"]
    if tool_messages:
        # 只把“当前轮”的工具消息算进去：如果最后一条 assistant(tool_calls) 后面已经有最终 assistant，
        # 说明这是历史消息，不要把旧 tool 重新当成本轮工具结果。
        last_tool_idx = max((i for i, m in enumerate(messages) if m.get("role") == "tool"), default=-1)
        last_assistant_idx = max((i for i, m in enumerate(messages) if m.get("role") == "assistant"), default=-1)
        last_tool_call_idx = max((i for i, m in enumerate(messages) if m.get("role") == "assistant" and m.get("tool_calls")), default=-1)
        if last_tool_idx < 0 or last_assistant_idx < 0 or last_tool_idx < last_assistant_idx:
            tool_messages = []
        elif last_tool_call_idx >= 0 and last_tool_call_idx < last_tool_idx:
            tool_messages = [m for m in messages[last_tool_call_idx + 1:] if m.get("role") == "tool"]
        else:
            tool_messages = []
        if tool_messages:
            print(f"🔧 检测到 {len(tool_messages)} 条工具结果消息")
    
    # ---------- 生成 session ID ----------
    # OpenAI 兼容请求本身通常不带会话 ID。之前这里每次随机生成 uuid，
    # 会导致每聊一句 Dashboard 就出现一条新对话。
    # 现在优先读取客户端/请求头提供的会话 ID；如果没有，则使用固定 default。
    session_id = (
        body.get("session_id") or
        body.get("conversation_id") or
        body.get("sessionId") or
        request.headers.get("X-Session-ID") or
        request.headers.get("X-Conversation-ID") or
        get_active_session_id() or
        "default"
    )
    
    # ---------- 分区缓存模式 ----------
    if CACHE_PARTITION_ENABLED:
        active_sid = get_active_session_id()
        if active_sid:
            session_id = active_sid
        
        # 从DB读取历史
        try:
            db_history = await get_conversation_messages(session_id, limit=10000)
            db_msgs = []
            for m in (db_history or []):
                msg = db_row_to_message(m)
                msg['created_at'] = m.get('created_at')  # 保留时间戳供分区时间窗口判断
                db_msgs.append(msg)
        except Exception as e:
            print(f"[warning] 分区模式读取历史失败: {e}")
            db_msgs = []
        
        # 提取客户端 system prompt。分区缓存会重组 messages，不能直接保留原 system 消息，
        # 但必须把客户端传入的 system 内容作为 base_prompt 传入，避免系统消息被吞。
        client_system_parts = []
        system_like_roles = {"system", "developer"}
        for m in messages:
            if m.get("role") in system_like_roles:
                c = m.get("content", "")
                if isinstance(c, str):
                    client_system_parts.append(c)
                elif isinstance(c, list):
                    client_system_parts.append(" ".join(
                        item.get("text", "") for item in c
                        if isinstance(item, dict) and item.get("type") == "text"
                    ))
                else:
                    client_system_parts.append(str(c))
        client_system_prompt = "\n\n".join(p for p in client_system_parts if p).strip()
        partition_base_prompt = client_system_prompt or SYSTEM_PROMPT

        # 提取客户端新消息（非系统级消息），可能是user、tool、或带tool_calls的assistant
        client_new_msgs = [m for m in messages if m.get("role") not in system_like_roles]
        # 如果客户端最后一条非系统消息是 user，通常是普通用户新一轮。
        # 但 Operit 的工具结果请求也可能在 tool 后追加重复 user，
        # 所以不能在确认“没有本轮工具结果”之前就丢掉 assistant(tool_calls)+tool。
        last_client_msg = client_new_msgs[-1] if client_new_msgs else None
        client_ends_with_user = bool(last_client_msg and last_client_msg.get("role") == "user")
        active_tool_result_in_client = False
        for idx in range(len(client_new_msgs) - 1, -1, -1):
            probe = client_new_msgs[idx]
            if probe.get("role") == "assistant" and probe.get("tool_calls"):
                probe_ids = {tc.get("id") for tc in probe.get("tool_calls", []) if tc.get("id")}
                saw_matching_tool = False
                completed_by_assistant = False
                for tail in client_new_msgs[idx + 1:]:
                    if tail.get("role") == "assistant" and not tail.get("tool_calls"):
                        completed_by_assistant = True
                        break
                    if tail.get("role") == "tool" and tail.get("tool_call_id") in probe_ids:
                        saw_matching_tool = True
                active_tool_result_in_client = saw_matching_tool and not completed_by_assistant
                break
        if client_ends_with_user and not active_tool_result_in_client:
            client_new_msgs = [last_client_msg]
            print("🔧 分区模式: 客户端最后一条是user且无本轮tool结果，忽略随请求携带的旧tool历史")
        # 分区模式下，普通assistant消息来自上一轮response（DB里已存），过滤掉避免重复
        # 但带tool_calls的assistant必须保留最后一条——它是当前工具轮的一部分，需要和tool配对
        # （历史里的旧assistant(tool_calls)已在DB中，不需要重复带入）
        # 找到客户端带的最后一条 assistant(tool_calls)（当前轮工具调用）
        last_tc_ast = None
        last_tc_idx = -1
        for i in range(len(client_new_msgs) - 1, -1, -1):
            m = client_new_msgs[i]
            if m.get("role") == "assistant" and m.get("tool_calls"):
                last_tc_ast = m
                last_tc_idx = i
                break
        # 如果这是工具结果轮，只允许最后一条 assistant(tool_calls) 后面的 tool 进入本轮。
        # Operit 可能把更早历史里的 tool 也一起带来，不能把它们夹到最新 tool_call 前面。
        if last_tc_ast and last_tc_idx >= 0:
            current_tc_ids = {tc.get("id") for tc in last_tc_ast.get("tool_calls", []) if tc.get("id")}
            trailing_tools = [
                m for m in client_new_msgs[last_tc_idx + 1:]
                if m.get("role") == "tool" and m.get("tool_call_id") in current_tc_ids
            ]
            if trailing_tools:
                dropped_tools = [m for m in client_new_msgs if m.get("role") == "tool" and m not in trailing_tools]
                if dropped_tools:
                    print(f"🔧 分区模式: 丢弃{len(dropped_tools)}条最新tool_call之前的旧tool")
                client_new_msgs = [last_tc_ast] + trailing_tools
            else:
                client_new_msgs = [m for m in client_new_msgs if m.get("role") != "assistant"]
        else:
            # 过滤掉所有 assistant（DB里已有历史）
            client_new_msgs = [m for m in client_new_msgs if m.get("role") != "assistant"]
        # 分区模式下DB已有完整历史，客户端发来的旧user是冗余的，只保留最后一条
        user_msgs = [m for m in client_new_msgs if m.get("role") == "user"]
        if len(user_msgs) > 1:
            last_user = user_msgs[-1]
            client_new_msgs = [m for m in client_new_msgs if m.get("role") != "user"]
            client_new_msgs.append(last_user)
            print(f"🔧 去重: 过滤{len(user_msgs)-1}条冗余user，保留最后1条")
        # 工具结果轮次处理：基于DB状态 + 当前轮次tool_call_id精确判断
        # 只取匹配当前轮 assistant(tool_calls) 的 tool，历史轮的 tool 已在 DB 中
        if last_tc_ast:
            current_tc_ids = {tc.get("id") for tc in last_tc_ast.get("tool_calls", []) if tc.get("id")}
            client_tools = [m for m in client_new_msgs if m.get("role") == "tool" and m.get("tool_call_id") in current_tc_ids]
        else:
            client_tools = [m for m in client_new_msgs if m.get("role") == "tool"]
        if not client_tools:
            # 本轮没有工具结果时，不能把DB里末尾悬空的 assistant(tool_calls) 当历史发给上游。
            # 这通常来自上一次工具轮失败/中断；继续发送会触发 upstream 400/429：
            # assistant(tool_calls) must be followed by tool messages.
            dangling_count = 0
            while db_msgs and db_msgs[-1].get("role") == "assistant" and db_msgs[-1].get("tool_calls"):
                db_msgs.pop()
                dangling_count += 1
            if dangling_count:
                print(f"🔧 分区模式: 清理{dangling_count}条末尾悬空assistant(tool_calls)")

        if client_tools:
            # 判断DB是否处于"等待tool结果"状态（最后一条是assistant(tool_calls)）
            db_last = db_msgs[-1] if db_msgs else None
            db_expecting_tool = (db_last and db_last.get("role") == "assistant" and db_last.get("tool_calls"))
            
            if not db_expecting_tool:
                # DB不在等待tool结果，但可能是异步存储延迟（process_memories_background还没写完）
                # 先检查客户端原始messages里是否有匹配的assistant(tool_calls)
                client_tool_ids = {m.get('tool_call_id') for m in client_tools if m.get('tool_call_id')}
                db_matching_ast_ids = []
                for hist_msg in db_msgs:
                    if hist_msg.get("role") == "assistant" and hist_msg.get("tool_calls"):
                        hist_ids = [tc.get("id") for tc in hist_msg.get("tool_calls", []) if tc.get("id")]
                        if client_tool_ids & set(hist_ids):
                            db_matching_ast_ids.extend([i for i in hist_ids if i in client_tool_ids])
                print(f"🔎 工具结果race诊断: client_tool_ids={list(client_tool_ids)}, db_has_matching_ast={bool(db_matching_ast_ids)}, matched_ids={db_matching_ast_ids}")
                matching_ast = None
                for m in messages:
                    if m.get("role") == "assistant" and m.get("tool_calls"):
                        ast_tc_ids = {tc.get("id") for tc in m["tool_calls"] if tc.get("id")}
                        if client_tool_ids & ast_tc_ids:
                            matching_ast = m
                            break
                if matching_ast:
                    # 客户端有匹配的assistant(tool_calls)，说明是DB延迟，保留tool结果并补充assistant
                    matched_ids = {tc.get("id") for tc in matching_ast["tool_calls"] if tc.get("id")}
                    kept_tools = [m for m in client_tools if m.get('tool_call_id') in matched_ids]
                    stale_tools = [m for m in client_tools if m.get('tool_call_id') not in matched_ids]
                    if stale_tools:
                        print(f"🔧 去重: 丢弃{len(stale_tools)}条非当前轮次tool (ids: {[m.get('tool_call_id','?') for m in stale_tools]})")
                    # 从客户端原始messages里找到matching_ast前面最近的user，一起补进来
                    # 否则DB为空时all_msgs里没有user，模型不知道用户问了什么
                    preceding_user = None
                    for idx_m, orig_m in enumerate(messages):
                        if orig_m is matching_ast:
                            # 往前找最近的user
                            for back in range(idx_m - 1, -1, -1):
                                if messages[back].get("role") == "user":
                                    preceding_user = messages[back]
                                    break
                            break

                    # 重建client_new_msgs: [user] + assistant(tool_calls) + tool results
                    # 让tool结果作为真正的末尾，不追加重复user。
                    client_new_msgs = []
                    if preceding_user and not db_msgs:
                        client_new_msgs.append(preceding_user)
                    client_new_msgs.append(matching_ast)
                    client_new_msgs.extend(kept_tools)
                    has_user = "user+" if (preceding_user and not db_msgs) else ""
                    print(f"⚠️ DB延迟防护: 从客户端补充{has_user}assistant(tool_calls) + {len(kept_tools)}条tool")
                else:
                    # 客户端也没有匹配的assistant(tool_calls)，确实是历史残留
                    stale_ids = [m.get('tool_call_id', '?') for m in client_tools]
                    # 诊断：打印两边ID，看为什么匹配失败
                    all_ast_in_messages = []
                    for m in messages:
                        if m.get("role") == "assistant" and m.get("tool_calls"):
                            all_ast_in_messages.append([tc.get("id") for tc in m["tool_calls"]])
                    print(f"❌ 工具配对失败诊断: client_tool_ids={stale_ids}, 原始messages中的assistant tool_calls ids={all_ast_in_messages}, db_msgs末尾role={db_msgs[-1].get('role') if db_msgs else 'empty'}")
                    print(f"🔧 去重: DB未在等待tool结果且客户端无匹配assistant，丢弃{len(client_tools)}条客户端tool (ids: {stale_ids})")
                    client_new_msgs = [m for m in client_new_msgs if m.get("role") != "tool"]
            else:
                # DB在等待tool → 只保留匹配当前轮次assistant(tool_calls)的tool
                expected_tool_ids = {tc.get("id") for tc in db_last.get("tool_calls", []) if tc.get("id")}
                client_tool_ids_set = {m.get("tool_call_id") for m in client_tools if m.get("tool_call_id")}
                new_tools = [m for m in client_tools if m.get("tool_call_id") in expected_tool_ids]
                stale_tools = [m for m in client_tools if m.get("tool_call_id") not in expected_tool_ids]
                
                if not new_tools and client_tool_ids_set and not (expected_tool_ids & client_tool_ids_set):
                    # DB末尾的assistant(tool_calls)是旧的残留，ID完全不匹配当前工具结果
                    # 把它移除，然后走延迟防护分支从客户端原始messages里补正确的
                    db_msgs.pop()
                    print(f"🔧 分区模式: DB末尾assistant(tool_calls)是旧残留(ids={expected_tool_ids})，与当前tool(ids={client_tool_ids_set})不匹配，移除并回退到客户端补充")
                    # 重新走延迟防护逻辑
                    matching_ast = None
                    for m in messages:
                        if m.get("role") == "assistant" and m.get("tool_calls"):
                            ast_tc_ids = {tc.get("id") for tc in m["tool_calls"] if tc.get("id")}
                            if client_tool_ids_set & ast_tc_ids:
                                matching_ast = m
                                break
                    if matching_ast:
                        matched_ids = {tc.get("id") for tc in matching_ast["tool_calls"] if tc.get("id")}
                        kept_tools = [m for m in client_tools if m.get('tool_call_id') in matched_ids]
                        preceding_user = None
                        for idx_m, orig_m in enumerate(messages):
                            if orig_m is matching_ast:
                                for back in range(idx_m - 1, -1, -1):
                                    if messages[back].get("role") == "user":
                                        preceding_user = messages[back]
                                        break
                                break

                        client_new_msgs = []
                        if preceding_user and not db_msgs:
                            client_new_msgs.append(preceding_user)
                        client_new_msgs.append(matching_ast)
                        client_new_msgs.extend(kept_tools)
                        print(f"⚠️ 旧残留修复: 从客户端补充assistant(tool_calls) + {len(kept_tools)}条tool")
                    else:
                        # 客户端也找不到匹配，丢弃tool
                        print(f"🔧 去重: DB旧残留+客户端无匹配assistant，丢弃{len(client_tools)}条tool")
                        client_new_msgs = [m for m in client_new_msgs if m.get("role") != "tool"]
                else:
                    if stale_tools:
                        print(f"🔧 去重: 丢弃{len(stale_tools)}条非当前轮次tool (ids: {[m.get('tool_call_id','?') for m in stale_tools]})")
                    if new_tools:
                        print(f"🔧 保留{len(new_tools)}条当前轮次tool (ids: {[m.get('tool_call_id','?') for m in new_tools]})")
                
                    # 重建 client_new_msgs：只保留tool结果
                    # 注意：工具结果轮次不能再追加末尾的重复user（Operit会把原始问题贴在末尾），
                    # 否则它会被build_partitioned_messages当成current_user_msg，
                    # 导致assistant(tool_calls)+tool链失去末尾锚点、被甩进A区剥离掉。
                    client_new_msgs = new_tools[:]
                
                if new_tools:
                    # Race condition 防护：DB的assistant(tool_calls)已确认存在（db_expecting_tool=True），
                    # 但仍需检查是否被其他并发请求意外清除
                    new_tool_ids = {m.get("tool_call_id") for m in new_tools if m.get("tool_call_id")}
                    db_has_matching_ast = False
                    for m in db_msgs:
                        if m.get("role") == "assistant" and m.get("tool_calls"):
                            ast_tc_ids = {tc.get("id") for tc in m["tool_calls"] if tc.get("id")}
                            if new_tool_ids & ast_tc_ids:
                                db_has_matching_ast = True
                                break
                    if not db_has_matching_ast and new_tool_ids:
                        for m in messages:
                            if m.get("role") == "assistant" and m.get("tool_calls"):
                                ast_tc_ids = {tc.get("id") for tc in m["tool_calls"] if tc.get("id")}
                                if new_tool_ids & ast_tc_ids:
                                    client_new_msgs.insert(0, m)
                                    print(f"⚠️ Race防护: 从客户端补充assistant(tool_calls)")
                                    break
        # 分区模式以 DB 历史为准：如果本次是工具结果轮，先把当前 tool 结果写入历史，
        # 再重新读取 DB 构造 A/B 分区。这样后续请求不再依赖客户端携带完整历史。
        tool_messages = [m for m in client_new_msgs if m.get("role") == "tool"]
        if tool_messages:
            # 构建客户端短id→DB原始长id映射：按最近未满足的 assistant(tool_calls) 顺序配对
            _id_map = _map_tool_ids_to_db_pending(db_msgs, tool_messages)
            _mapped_diff = {k: v for k, v in _id_map.items() if k != v}
            if _mapped_diff:
                add_dashboard_log("info", f"🔧 tool_call_id映射(分区保存): {_mapped_diff}", category="chat", session_id=session_id)

            persisted_tools = 0
            for tm in tool_messages:
                tool_call_id = tm.get("tool_call_id")
                if not tool_call_id:
                    continue
                db_tool_call_id = _id_map.get(tool_call_id, tool_call_id)
                try:
                    pool = await get_pool()
                    async with pool.acquire() as conn:
                        exists = await conn.fetchval(
                            """
                            SELECT 1
                            FROM conversations
                            WHERE session_id = $1
                              AND role = 'tool'
                              AND metadata::jsonb ->> 'tool_call_id' = $2
                            LIMIT 1
                            """,
                            session_id, db_tool_call_id
                        )
                    if exists:
                        continue
                    meta_dict = {"tool_call_id": db_tool_call_id}
                    if tm.get("name"):
                        meta_dict["name"] = tm["name"]
                    await save_message(session_id, "tool", tm.get("content", ""), model, metadata=json.dumps(meta_dict))
                    persisted_tools += 1
                except Exception as e:
                    print(f"⚠️ 分区模式: 同步保存tool结果失败 id={db_tool_call_id}: {e}")
            if persisted_tools:
                print(f"🔧 分区模式: 已先写入{persisted_tools}条tool结果到DB，再重建历史")
                try:
                    db_history = await get_conversation_messages(session_id, limit=10000)
                    db_msgs = []
                    for m in (db_history or []):
                        msg = db_row_to_message(m)
                        msg['created_at'] = m.get('created_at')
                        db_msgs.append(msg)
                    client_new_msgs = [m for m in client_new_msgs if m.get("role") != "tool"]
                except Exception as e:
                    print(f"⚠️ 分区模式: tool写入后重读历史失败: {e}")

        # 最终归一化：分区请求 = DB历史 + 本轮增量。
        # - 工具结果轮：如果当前DB快照还没有本轮tool，就把Operit刚返回的tool作为增量发给上游
        # - 普通用户轮：只追加客户端最后一条user，避免旧tool历史替换用户消息
        if tool_messages:
            db_tool_ids = {m.get("tool_call_id") for m in db_msgs if m.get("role") == "tool" and m.get("tool_call_id")}
            increment_tools = [m for m in tool_messages if m.get("tool_call_id") and m.get("tool_call_id") not in db_tool_ids]
            client_increment = []
            if increment_tools:
                increment_tool_ids = {m.get("tool_call_id") for m in increment_tools if m.get("tool_call_id")}
                db_has_matching_ast = False
                for m in reversed(db_msgs):
                    if m.get("role") == "assistant" and m.get("tool_calls"):
                        ast_ids = {tc.get("id") for tc in m.get("tool_calls", []) if tc.get("id")}
                        if increment_tool_ids & ast_ids:
                            db_has_matching_ast = True
                        break
                if not db_has_matching_ast:
                    matching_ast = None
                    for m in reversed(messages):
                        if m.get("role") == "assistant" and m.get("tool_calls"):
                            ast_ids = {tc.get("id") for tc in m.get("tool_calls", []) if tc.get("id")}
                            if increment_tool_ids & ast_ids:
                                matching_ast = m
                                break
                    if matching_ast:
                        client_increment.append(matching_ast)
                        print("⚠️ 分区模式: DB当前快照缺少assistant(tool_calls)，从客户端补当前工具请求")
                client_increment.extend(increment_tools)
        else:
            last_user_msg = None
            for m in reversed([m for m in messages if m.get("role") not in system_like_roles]):
                if m.get("role") == "user":
                    last_user_msg = m
                    break
            client_increment = [last_user_msg] if last_user_msg else []

        all_msgs = db_msgs + client_increment
        all_msgs = _repair_tool_call_ids_by_adjacency(all_msgs, session_id=session_id, reason="all_msgs")

        all_msgs = _normalize_tool_chains_by_id(all_msgs)

        # 后台保存仍只接收本轮真实tool；已同步写过的会被tool_call_id查重跳过
        tool_messages = [m for m in tool_messages if m.get("role") == "tool"]
        
        print(f"📦 分区模式: DB历史{len(db_msgs)}条 + 本轮增量{len(client_increment)}条")
        
        messages = await build_partitioned_messages(
            session_id, all_msgs, partition_base_prompt, user_message
        )
        messages = _repair_tool_call_ids_by_adjacency(messages, session_id=session_id, reason="final_messages")
        messages = _normalize_tool_chains_by_id(messages)
        messages = _drop_orphan_tool_messages(messages)

        body["messages"] = messages
    
    else:
        # ---------- 原有逻辑：system prompt + 记忆注入 ----------
        if False and (SYSTEM_PROMPT or (MEMORY_ENABLED and MEMORY_EXTRACT_ENABLED and user_message)):
            if MEMORY_ENABLED and MEMORY_EXTRACT_ENABLED and user_message:
                enhanced_prompt = await build_system_prompt_with_memories(user_message)
            else:
                enhanced_prompt = SYSTEM_PROMPT
            
            if enhanced_prompt:
                has_system = any(msg.get("role") == "system" for msg in messages)
                if has_system:
                    for i, msg in enumerate(messages):
                        if msg.get("role") == "system":
                            messages[i]["content"] = enhanced_prompt + "\n\n" + msg["content"]
                            break
                else:
                    messages.insert(0, {"role": "system", "content": enhanced_prompt})

        # 非分区模式下也要兜一下工具轮次：
        # Operit 有时会把原始 user 又贴到末尾，导致上游把它当成新问题，
        # 进而让本轮 tool 结果看起来“没接上”。这里只给上游请求生成裁剪副本，
        # 不原地修改原始 messages，避免影响对话存储/下一轮用户消息。
        upstream_messages = messages
        if tool_messages:
            upstream_messages = list(messages)
            last_tool_idx = -1
            for i in range(len(upstream_messages) - 1, -1, -1):
                if upstream_messages[i].get("role") == "tool":
                    last_tool_idx = i
                    break
            if last_tool_idx >= 0:
                old_len = len(upstream_messages)
                while len(upstream_messages) > last_tool_idx + 1 and upstream_messages[-1].get("role") == "user":
                    upstream_messages.pop()
                if len(upstream_messages) != old_len:
                    print(f"🔧 非分区模式: 去掉上游请求末尾重复user，messages {old_len}->{len(upstream_messages)}")
        
        body["messages"] = upstream_messages
    
    # ---------- 模型处理 ----------
    model = body.get("model", DEFAULT_MODEL)
    if not model:
        model = DEFAULT_MODEL
    body["model"] = model
    
    # ---------- 温度参数注入 ----------
    if str(CHAT_TEMPERATURE).strip() != "":
        try:
            body["temperature"] = float(CHAT_TEMPERATURE)
        except Exception:
            print(f"⚠️ CHAT_TEMPERATURE 无效，跳过注入: {CHAT_TEMPERATURE}")

    # ---------- cache_control 兼容性处理 ----------
    if CACHE_PARTITION_ENABLED and not _is_anthropic_model(model):
        _strip_cache_control(body.get("messages", []))
    
    # ---------- 记录最近一次实际发送给上游的请求体（Dashboard 手动查看） ----------
    global _last_upstream_request_body, _last_upstream_request_meta
    try:
        _last_upstream_request_body = json.loads(json.dumps(body, ensure_ascii=False))
        _last_upstream_request_meta = {
            "time": (datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_HOURS)).strftime("%m-%d %H:%M:%S"),
            "session_id": session_id,
            "model": body.get("model", ""),
            "message_count": len(body.get("messages", []) or []),
            "cache_partition_enabled": CACHE_PARTITION_ENABLED,
        }
    except Exception as e:
        print(f"⚠️ 记录上次请求体失败: {e}")

    # ---------- 转发请求 ----------
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
    }
    # OpenRouter 需要的额外头
    if "openrouter" in API_BASE_URL:
        headers["HTTP-Referer"] = EXTRA_REFERER
        headers["X-Title"] = EXTRA_TITLE
    
    is_stream = body.get("stream", False)
    
    # 强制流式传输（解决部分客户端不发stream=true的问题）
    if FORCE_STREAM and not is_stream:
        is_stream = True
        body["stream"] = True
        print(f"⚡ 强制开启流式传输（FORCE_STREAM=true）")
    
    # 注入推理参数（解决客户端走网关时不带reasoning参数的问题）
    if REASONING_EFFORT:
        # 统一用 reasoning_effort（Claude/OpenAI/Google Gemini OpenAI兼容端点都支持）
        # 先删除客户端可能已带的值，确保用我们配置的
        body.pop("reasoning_effort", None)
        body.pop("google", None)
        body["reasoning_effort"] = REASONING_EFFORT
        print(f"🧠 注入推理参数: reasoning_effort={REASONING_EFFORT}")
    
    print(f"📡 请求: model={model}, stream={is_stream}, memory={'on' if MEMORY_ENABLED else 'off'}", flush=True)
    
    # 调试：打印请求体中的推理相关字段
    debug_keys = {k: v for k, v in body.items() if k in ('reasoning_effort', 'google', 'reasoning')}
    if debug_keys:
        print(f"📡 推理字段: {debug_keys}", flush=True)
    
    if is_stream:
        return StreamingResponse(
            stream_and_capture(headers, body, session_id, user_message, model, original_messages, skip_conversation_log, tool_messages),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
        )
    else:
        async with httpx.AsyncClient(timeout=300) as client:
            response = await client.post(API_BASE_URL, headers=headers, json=body)
            
            if response.status_code == 200:
                resp_data = response.json()
                assistant_msg = ""
                assistant_tool_calls = None
                assistant_reasoning = None
                try:
                    msg_obj = resp_data["choices"][0]["message"]
                    # raw_assistant_msg 用于 DB 历史/记忆提取；assistant_msg_for_client 仅用于返回客户端
                    raw_assistant_msg = msg_obj.get("content") or ""
                    assistant_msg = raw_assistant_msg
                    if raw_assistant_msg:
                        transformed_msg = apply_response_transform_rules(raw_assistant_msg)
                        if transformed_msg != raw_assistant_msg:
                            msg_obj["content"] = transformed_msg
                            print("🔁 Response transform 已应用：客户端返回转换后，DB保存转换前")
                    if msg_obj.get("tool_calls"):
                        assistant_tool_calls = msg_obj["tool_calls"]
                        print(f"🔧 Response 包含 {len(assistant_tool_calls)} 个工具调用")
                    if msg_obj.get("reasoning_content"):
                        assistant_reasoning = msg_obj["reasoning_content"]
                        print(f"🧠 Response 包含 reasoning_content ({len(assistant_reasoning)}字符)")
                except (KeyError, IndexError):
                    pass
                
                if MEMORY_ENABLED and (user_message or tool_messages):
                    sync_saved_tool_call = False
                    if assistant_tool_calls and not tool_messages and not skip_conversation_log:
                        sync_saved_tool_call = await persist_assistant_tool_calls_sync(
                            session_id, user_message, assistant_msg, model, assistant_tool_calls, assistant_reasoning
                        )
                    asyncio.create_task(
                        process_memories_background(session_id, user_message, assistant_msg, model, 
                                                    context_messages=original_messages, skip_conversation_log=(skip_conversation_log or sync_saved_tool_call),
                                                    tool_messages=tool_messages, assistant_tool_calls=assistant_tool_calls,
                                                    assistant_reasoning=assistant_reasoning)
                    )
                
                return JSONResponse(status_code=200, content=resp_data)
            else:
                try:
                    error_content = response.json()
                except Exception:
                    error_content = {
                        "error": {
                            "message": response.text[:1000],
                            "type": "upstream_error",
                            "status": response.status_code,
                        }
                    }
                msg_count = len(body.get("messages", []) or [])
                body_chars = len(json.dumps(body, ensure_ascii=False))
                preview = response.text[:180]
                add_dashboard_log(
                    "error",
                    f"主对话上游失败 HTTP {response.status_code}，对话线={session_id}，messages={msg_count}，body≈{body_chars}字，返回片段={preview}",
                    category="chat",
                    session_id=session_id,
                )
                return JSONResponse(status_code=response.status_code, content=error_content)


async def stream_and_capture(headers: dict, body: dict, session_id: str, user_message: str, model: str, original_messages: list = None, skip_conversation_log: bool = False, tool_messages: list = None):
    """流式响应 + 捕获完整回复（原始字节透传，确保SSE格式和thinking数据完整）"""
    full_response = []
    full_reasoning = []
    stream_usage = {}
    line_buffer = ""
    accumulated_tool_calls = {}  # index -> {id, type, function: {name, arguments}}
    
    async with httpx.AsyncClient(timeout=300) as client:
        async with client.stream("POST", API_BASE_URL, headers=headers, json=body) as response:
            # 打印上游响应头（排查thinking问题用）
            upstream_ct = response.headers.get("content-type", "")
            print(f"📨 上游响应: status={response.status_code}, content-type={upstream_ct}", flush=True)
            
            # 上游非200时，提前打印messages结构方便debug
            if response.status_code != 200:
                msg_summary = [{"role": m.get("role"), "tool_calls": bool(m.get("tool_calls")), "tool_call_id": m.get("tool_call_id", ""), "content_type": type(m.get("content")).__name__} for m in body.get("messages", [])]
                print(f"❌ 发送的messages结构({len(msg_summary)}条): {msg_summary}", flush=True)
            
            error_body_parts = []
            is_error = response.status_code != 200

            if is_error:
                raw_error = await response.aread()
                error_text = raw_error.decode("utf-8", errors="ignore")[:1000]
                msg_count = len(body.get("messages", []) or [])
                body_chars = len(json.dumps(body, ensure_ascii=False))
                print(f"❌ 上游错误内容: {error_text[:500]}", flush=True)
                add_dashboard_log(
                    "error",
                    f"主对话上游失败 HTTP {response.status_code}，对话线={session_id}，messages={msg_count}，body≈{body_chars}字，返回片段={error_text[:180]}",
                    category="chat",
                    session_id=session_id,
                )
                safe_error = {
                    "id": f"chatcmpl-error-{uuid.uuid4().hex[:12]}",
                    "object": "chat.completion.chunk",
                    "created": int(datetime.now(timezone.utc).timestamp()),
                    "model": body.get("model", model),
                    "choices": [{
                        "index": 0,
                        "delta": {"content": f"上游接口请求失败：HTTP {response.status_code}。请在网关后台日志查看详情。"},
                        "finish_reason": "stop",
                    }],
                }
                yield f"data: {json.dumps(safe_error, ensure_ascii=False)}\n\n".encode("utf-8")
                yield b"data: [DONE]\n\n"
                return
            
            async for chunk in response.aiter_bytes():
                # 原始字节直接透传给客户端
                yield chunk
                
                # 旁路解析：从字节流中提取assistant回复内容，用于后续记忆提取
                text = chunk.decode("utf-8", errors="ignore")
                line_buffer += text
                while "\n" in line_buffer:
                    line, line_buffer = line_buffer.split("\n", 1)
                    line = line.strip()
                    if line.startswith("data: ") and line != "data: [DONE]":
                        try:
                            data = json.loads(line[6:])
                            
                            if "usage" in data:
                                stream_usage = data["usage"]
                            
                            delta = data.get("choices", [{}])[0].get("delta", {})
                            content = delta.get("content", "")
                            if content:
                                full_response.append(content)
                            
                            # 收集reasoning_content（deepseek thinking mode）
                            reasoning = delta.get("reasoning_content", "")
                            if reasoning:
                                full_reasoning.append(reasoning)
                            
                            # 累积tool_calls
                            if "tool_calls" in delta:
                                for tc in delta["tool_calls"]:
                                    idx = tc.get("index", 0)
                                    if idx not in accumulated_tool_calls:
                                        accumulated_tool_calls[idx] = {
                                            "index": idx,
                                            "id": tc.get("id", ""),
                                            "type": tc.get("type", "function"),
                                            "function": {"name": "", "arguments": ""}
                                        }
                                    if tc.get("id"):
                                        accumulated_tool_calls[idx]["id"] = tc["id"]
                                    if "function" in tc:
                                        fn = tc["function"]
                                        if fn.get("name"):
                                            accumulated_tool_calls[idx]["function"]["name"] = fn["name"]
                                        if "arguments" in fn:
                                            accumulated_tool_calls[idx]["function"]["arguments"] += fn["arguments"]
                        except (json.JSONDecodeError, KeyError, IndexError):
                            pass
    
    assistant_msg = "".join(full_response)
    assistant_reasoning = "".join(full_reasoning) if full_reasoning else None
    assistant_tool_calls = list(accumulated_tool_calls.values()) if accumulated_tool_calls else None
    
    if assistant_reasoning:
        print(f"🧠 Stream response 包含 reasoning_content ({len(assistant_reasoning)}字符)")
    
    # 上游非 200 已在流开始时转成 OpenAI 兼容 SSE 错误，不再透传 HTML/JSON 错误页。
    if assistant_tool_calls:
        print(f"🔧 Stream response 包含 {len(assistant_tool_calls)} 个工具调用")
    
    if stream_usage:
        pt = stream_usage.get("prompt_tokens", 0)
        ct = stream_usage.get("completion_tokens", 0)
        tt = stream_usage.get("total_tokens", 0)
        if tt > 0:
            asyncio.create_task(save_token_usage(session_id, model, pt, ct, tt))
            print(f"📊 Stream Token: {pt} + {ct} = {tt}")
    
    if MEMORY_ENABLED and (user_message or tool_messages):
        sync_saved_tool_call = False
        if assistant_tool_calls and not tool_messages and not skip_conversation_log:
            sync_saved_tool_call = await persist_assistant_tool_calls_sync(
                session_id, user_message, assistant_msg, model, assistant_tool_calls, assistant_reasoning
            )
        asyncio.create_task(
            process_memories_background(session_id, user_message, assistant_msg, model, 
                                        context_messages=original_messages, skip_conversation_log=(skip_conversation_log or sync_saved_tool_call),
                                        tool_messages=tool_messages, assistant_tool_calls=assistant_tool_calls,
                                        assistant_reasoning=assistant_reasoning)
        )


# ============================================================
# 记忆管理接口
# ============================================================


@app.get("/import/seed-memories")
async def import_seed_memories():
    """一次性导入预置记忆（从 seed_memories.py）"""
    try:
        from seed_memories import run_seed_import
        result = await run_seed_import()
        return result
    except ImportError:
        return {"error": "未找到 seed_memories.py，请参考 seed_memories_example.py 创建"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/export/memories")
async def export_memories():
    """
    导出所有记忆为 JSON（用于备份或迁移）
    浏览器访问这个地址就会返回所有记忆数据
    """
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用（设置 MEMORY_ENABLED=true 开启）"}
    
    try:
        memories = await get_all_memories()
        # 把 datetime 转成字符串
        for mem in memories:
            if mem.get("created_at"):
                mem["created_at"] = str(mem["created_at"])
        
        return {
            "total": len(memories),
            "exported_at": str(__import__("datetime").datetime.now()),
            "memories": memories,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard_page(request: Request):
    """Dashboard - 整合的记忆管理界面"""
    if not MEMORY_ENABLED:
        return HTMLResponse("<h3>记忆系统未启用（设置 MEMORY_ENABLED=true 开启）</h3>")
    
    return templates.TemplateResponse(request, "dashboard.html")



# ============================================================
# 管理 API
# ============================================================

@app.get("/api/dashboard/logs")
async def api_dashboard_logs(limit: int = 80):
    """Dashboard 查看最近后台任务/记忆提取日志。"""
    limit = max(1, min(limit, 200))
    return {"logs": list(_dashboard_logs)[:limit]}


@app.get("/api/dashboard/last-request")
async def api_dashboard_last_request():
    """Dashboard 手动查看最近一次实际转发给上游模型的请求体。"""
    if _last_upstream_request_body is None:
        return {"available": False, "message": "还没有记录到已转发的请求体"}
    return {
        "available": True,
        "meta": _last_upstream_request_meta,
        "body": _last_upstream_request_body,
    }


@app.post("/api/dashboard/logs/clear")
async def api_clear_dashboard_logs():
    _dashboard_logs.clear()
    return {"status": "ok"}


@app.get("/api/memories")
async def api_get_memories(layer: int = None, active_only: bool = None):
    """获取所有记忆（管理页面用）
    
    Query params:
        layer: 筛选层级（1=碎片, 2=事件, 3=核心）
        active_only: 是否只返回活跃记忆
    """
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    memories = await get_all_memories_detail(layer=layer, active_only=active_only)
    tz_offset = timezone(timedelta(hours=TIMEZONE_HOURS))
    for m in memories:
        if m.get("created_at"):
            dt = m["created_at"]
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            m["created_at"] = dt.astimezone(tz_offset).strftime("%Y-%m-%d %H:%M:%S")
    # 获取层级统计
    try:
        layer_stats = await get_layer_statistics()
    except Exception:
        layer_stats = None
    
    result = {"memories": memories}
    if layer_stats:
        result["layer_stats"] = layer_stats
    return result


@app.get("/api/memories/search")
async def api_search_memories(q: str = "", limit: int = 20):
    """语义搜索记忆（Dashboard用，走后端 search_memories）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    if not q.strip():
        return {"error": "搜索关键词不能为空", "results": []}
    try:
        results = await search_memories(q.strip(), limit)
        tz_offset = timezone(timedelta(hours=TIMEZONE_HOURS))
        out = []
        for r in results:
            item = dict(r)
            if item.get("created_at"):
                dt = item["created_at"]
                if hasattr(dt, 'tzinfo'):
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    item["created_at"] = dt.astimezone(tz_offset).strftime("%Y-%m-%d %H:%M:%S")
            out.append(item)
        return {"results": out, "total": len(out)}
    except Exception as e:
        return {"error": str(e), "results": []}


@app.put("/api/memories/{memory_id}")
async def api_update_memory(memory_id: int, request: Request):
    """更新单条记忆（支持 content / importance / title / layer）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    # 解析 event_date（支持 YYYY-MM-DD 字符串）
    event_date = None
    if data.get("event_date"):
        try:
            event_date = datetime.strptime(data["event_date"], "%Y-%m-%d").date()
        except (ValueError, TypeError):
            event_date = None
    
    # 解析 created_at（支持 YYYY-MM-DD 或 YYYY-MM-DD HH:MM:SS）
    created_at = None
    if data.get("created_at"):
        try:
            raw = data["created_at"]
            if len(raw) <= 10:
                created_at = datetime.strptime(raw, "%Y-%m-%d")
            else:
                created_at = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
        except (ValueError, TypeError):
            created_at = None
    
    await update_memory_with_layer(
        memory_id,
        content=data.get("content"),
        importance=data.get("importance"),
        title=data.get("title"),
        layer=data.get("layer"),
        event_date=event_date,
        created_at=created_at,
    )
    return {"status": "ok", "id": memory_id}


@app.delete("/api/memories/{memory_id}")
async def api_delete_memory(memory_id: int, soft: bool = False):
    """删除单条记忆
    
    Query params:
        soft: true=归档（is_active=false），false=永久删除
    """
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    if soft:
        await update_memory_with_layer(memory_id, is_active=False)
    else:
        await delete_memory(memory_id)
    return {"status": "ok", "id": memory_id}


@app.post("/api/memories/extract-from-chat")
async def api_extract_from_chat(request: Request):
    """从聊天记录文本中提取记忆（调用记忆提取 API）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    chat_text = data.get("text", "").strip()
    if not chat_text:
        return {"error": "聊天记录为空"}
    
    # 智能解析聊天记录，支持多种格式：
    # 1. Operit md 导出: "## N. 用户/Operit · 时间" 标题 + 正文
    # 2. "用户: xxx" / "助手: xxx" 前缀格式
    # 3. JSON 数组 [{role, content}, ...]
    # 4. 无前缀纯文本 → 全当 user
    
    import re as _re
    messages = []
    
    # 尝试 JSON 格式
    try:
        parsed = json.loads(chat_text)
        if isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], dict) and "content" in parsed[0]:
            messages = [{"role": m.get("role", "user"), "content": m.get("content", "")} for m in parsed if m.get("content")]
    except (json.JSONDecodeError, TypeError, KeyError):
        pass
    
    # 尝试 Operit md 格式: "## N. 用户/Operit · 时间"
    if not messages:
        md_pattern = _re.compile(r"^#{1,3}\s*\d+\.\s*(用户|Operit|User|Assistant|助手|AI|澈)\s*[·•\-]", _re.MULTILINE)
        md_matches = list(md_pattern.finditer(chat_text))
        if md_matches:
            for idx, match in enumerate(md_matches):
                role_name = match.group(1).strip()
                role = "user" if role_name in ("用户", "User") else "assistant"
                # 内容从标题行结束到下一个标题
                start = chat_text.index("\n", match.start()) + 1 if "\n" in chat_text[match.start():] else match.end()
                end = md_matches[idx + 1].start() if idx + 1 < len(md_matches) else len(chat_text)
                body = chat_text[start:end].strip()
                # 去掉 markdown 分隔线
                body = _re.sub(r"^---+\s*$", "", body, flags=_re.MULTILINE).strip()
                if body:
                    messages.append({"role": role, "content": body})
    
    # 尝试前缀格式
    if not messages:
        current_role = "user"
        current_content = []
        
        for line in chat_text.split("\n"):
            stripped = line.strip()
            if not stripped:
                continue
            new_role = None
            msg_content = stripped
            for prefix, role in [("用户:", "user"), ("user:", "user"), ("User:", "user"),
                                 ("助手:", "assistant"), ("assistant:", "assistant"), ("Assistant:", "assistant"),
                                 ("AI:", "assistant"), ("澈:", "assistant"), ("Operit:", "assistant"),
                                 ("bot:", "assistant"), ("Bot:", "assistant")]:
                if stripped.startswith(prefix):
                    new_role = role
                    msg_content = stripped[len(prefix):].strip()
                    break
            
            if new_role and new_role != current_role:
                if current_content:
                    messages.append({"role": current_role, "content": "\n".join(current_content)})
                    current_content = []
                current_role = new_role
            
            if msg_content:
                current_content.append(msg_content)
        
        if current_content:
            messages.append({"role": current_role, "content": "\n".join(current_content)})
    
    if not messages:
        return {"error": "无法从文本中解析出对话内容"}
    
    # 获取已有记忆用于去重
    existing = await get_all_memories()
    existing_contents = [m["content"] for m in existing] if existing else []
    
    # 调用提取
    try:
        extracted = await extract_memories(messages, existing_memories=existing_contents)
    except Exception as e:
        return {"error": f"提取失败: {str(e)}"}
    
    if not extracted:
        return {"status": "ok", "memories": [], "message": "未提取到新记忆"}
    
    return {"status": "ok", "memories": extracted, "message": f"提取到 {len(extracted)} 条记忆"}


@app.post("/api/memories/batch-update")
async def api_batch_update(request: Request):
    """批量更新记忆"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    updates = data.get("updates", [])
    if not updates:
        return {"error": "没有要更新的记忆"}
    for item in updates:
        event_date = None
        if item.get("event_date"):
            try:
                event_date = datetime.strptime(item["event_date"], "%Y-%m-%d").date()
            except (ValueError, TypeError):
                pass
        created_at = None
        if item.get("created_at"):
            try:
                raw = item["created_at"]
                created_at = datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S") if len(raw) > 10 else datetime.strptime(raw, "%Y-%m-%d")
            except (ValueError, TypeError):
                pass
        await update_memory_with_layer(
            item["id"],
            content=item.get("content"),
            importance=item.get("importance"),
            title=item.get("title"),
            layer=item.get("layer"),
            event_date=event_date,
            created_at=created_at,
        )
    return {"status": "ok", "updated": len(updates)}


@app.post("/api/memories/batch-delete")
async def api_batch_delete(request: Request):
    """批量删除记忆"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    ids = data.get("ids", [])
    if not ids:
        return {"error": "未选择记忆"}
    await delete_memories_batch(ids)
    return {"status": "ok", "deleted": len(ids)}


# ============================================================
# 三层记忆架构：整理 / 合并 / 升级 / 统计
# ============================================================

_DEFAULT_CONSOLIDATION_PROMPT = """
你是记忆整理助手。请将以下对话碎片整理成完整的事件记录。

要求：
1. 按主题/事件分组，相关的碎片合并到一起
2. 每个事件一条记录，不要太细碎也不要太笼统
3. 每条记录包含：标题（10字内）+ 完整描述
4. 合并重复内容，保留重要细节
5. 保留原文中的主观感受、情绪表达和个人化用语，不要改写为客观陈述或第三方总结
6. content字段中不要使用双引号，用单引号或书名号代替

碎片记忆：
{fragments}

请用 JSON 格式输出：
[
  {{
    "title": "事件标题（10字内）",
    "content": "完整的事件描述",
    "importance": 5,
    "merged_ids": [1, 2, 3]
  }}
]

只输出 JSON，不要其他内容。确保 JSON 语法正确。
"""


# 碎片整理提示词（支持面板热更新）
_cached_consolidation_prompt = None
_cached_consolidation_prompt_loaded = False

async def get_consolidation_prompt() -> str:
    global _cached_consolidation_prompt, _cached_consolidation_prompt_loaded
    if _cached_consolidation_prompt_loaded:
        return _cached_consolidation_prompt or _DEFAULT_CONSOLIDATION_PROMPT
    try:
        db_prompt = await get_gateway_config("consolidationPrompt", "")
        if db_prompt:
            _cached_consolidation_prompt = db_prompt
        else:
            _cached_consolidation_prompt = _DEFAULT_CONSOLIDATION_PROMPT
        _cached_consolidation_prompt_loaded = True
        return _cached_consolidation_prompt
    except Exception:
        _cached_consolidation_prompt = _DEFAULT_CONSOLIDATION_PROMPT
        _cached_consolidation_prompt_loaded = True
        return _cached_consolidation_prompt

def set_consolidation_prompt(prompt: str):
    global _cached_consolidation_prompt, _cached_consolidation_prompt_loaded
    _cached_consolidation_prompt = prompt
    _cached_consolidation_prompt_loaded = True

def invalidate_consolidation_prompt_cache():
    global _cached_consolidation_prompt, _cached_consolidation_prompt_loaded
    _cached_consolidation_prompt = None
    _cached_consolidation_prompt_loaded = False

# 整理状态（异步执行，防重入）
_consolidate_status = {
    "running": False,
    "started_at": None,
    "result": None,
    "error": None,
}



_DEFAULT_DAILY_IMPRESSION_PROMPT = """你是长期陪伴型AI的记忆整理员。请根据某一天的真实对话历史，生成一条“日印象”。

要求：
- 使用第三人称、客观但有温度的语气。
- 不要逐条复述对话，要总结这一天的主题、状态、重要进展和关系氛围。
- 如果有承诺、待办、偏好变化、情绪波动，可以自然写入。
- 可以保留对用户表达习惯、互动模式的观察，但不要编造对话中没有的信息。
- 输出 JSON 对象，不要代码块，不要额外文字。

输出格式：
{
  "summary": "200-600字的日印象正文",
  "topics": ["主题1", "主题2"],
  "mood": "当天整体氛围/情绪，简短描述"
}

当天对话历史：
{conversation}
"""


_cached_daily_impression_prompt = None
_cached_daily_impression_prompt_loaded = False


async def get_daily_impression_prompt() -> str:
    global _cached_daily_impression_prompt, _cached_daily_impression_prompt_loaded
    if _cached_daily_impression_prompt_loaded:
        return _cached_daily_impression_prompt or _DEFAULT_DAILY_IMPRESSION_PROMPT
    try:
        db_prompt = await get_gateway_config("dailyImpressionPrompt", "")
        _cached_daily_impression_prompt = db_prompt or _DEFAULT_DAILY_IMPRESSION_PROMPT
    except Exception:
        _cached_daily_impression_prompt = _DEFAULT_DAILY_IMPRESSION_PROMPT
    _cached_daily_impression_prompt_loaded = True
    return _cached_daily_impression_prompt


def set_daily_impression_prompt(prompt: str):
    global _cached_daily_impression_prompt, _cached_daily_impression_prompt_loaded
    _cached_daily_impression_prompt = prompt or _DEFAULT_DAILY_IMPRESSION_PROMPT
    _cached_daily_impression_prompt_loaded = True


async def generate_daily_impression_for_date(impression_date):
    """从指定日期的对话历史生成/更新日印象，不改动碎片状态。"""
    messages = await get_conversation_messages_by_date(impression_date)
    if not messages:
        return {"status": "no_conversations", "date": str(impression_date)}

    role_map = {"user": "用户", "assistant": "助手", "system": "系统", "tool": "工具"}
    conversation_text = "\n".join([
        f"[{m['created_at'].strftime('%H:%M') if hasattr(m['created_at'], 'strftime') else ''}] {role_map.get(m['role'], m['role'])}: {m['content']}"
        for m in messages
    ])
    prompt = (await get_daily_impression_prompt()).replace("{conversation}", conversation_text).replace("{fragments}", conversation_text)

    memory_api_base_url = get_memory_api_base_url()
    if not memory_api_base_url:
        return {"status": "error", "error": "MEMORY_API_BASE_URL 未设置，无法生成日印象"}

    impression_model = os.getenv("MEMORY_MODEL", "anthropic/claude-haiku-4")
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(
                memory_api_base_url,
                headers={
                    "Authorization": f"Bearer {get_memory_api_key()}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": impression_model,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 2500,
                    "temperature": 0,
                },
            )
        if response.status_code != 200:
            return {"status": "error", "error": f"HTTP {response.status_code}: {response.text[:300]}"}

        raw = response.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        import re as _re
        match = _re.search(r"\{[\s\S]*\}", raw)
        if not match:
            return {"status": "error", "error": "AI 未返回 JSON 对象", "raw": raw[:500]}
        try:
            obj = json.loads(match.group(), strict=False)
        except json.JSONDecodeError as e:
            return {"status": "error", "error": f"JSON 解析失败: {e}", "raw": raw[:500]}

        topics = obj.get("topics", [])
        if isinstance(topics, list):
            topics_text = "、".join(str(t).strip() for t in topics if str(t).strip())
        else:
            topics_text = str(topics or "")

        saved = await upsert_daily_impression(
            impression_date,
            str(obj.get("summary", "")).strip(),
            topics=topics_text,
            mood=str(obj.get("mood", "")).strip(),
            source_fragment_ids=None,
        )
        return {
            "status": "ok",
            "date": str(impression_date),
            "messages_used": len(messages),
            "impression": _serialize_daily_impression(saved),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

def _serialize_daily_impression(row):
    if not row:
        return None
    return {
        "date": row["impression_date"].isoformat() if hasattr(row.get("impression_date"), "isoformat") else str(row.get("impression_date")),
        "summary": row.get("summary") or "",
        "topics": row.get("topics") or "",
        "mood": row.get("mood") or "",
        "source_fragment_ids": row.get("source_fragment_ids") or [],
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


async def consolidate_memories_for_date(event_date):
    """整理指定日期的碎片记忆"""
    return await consolidate_memories_for_date_range(event_date, event_date)


async def consolidate_memories_for_date_range(start_date, end_date):
    """整理指定时间段的碎片记忆"""
    from datetime import date
    import re
    
    # 获取该时间段的碎片
    fragments = await get_fragments_by_date_range(start_date, end_date)
    
    if not fragments:
        return {"status": "no_fragments", "start_date": str(start_date), "end_date": str(end_date)}
    
    # 构建碎片文本
    fragments_text = "\n".join([
        f"[ID={f['id']}] ({f['created_at'].strftime('%m-%d') if hasattr(f['created_at'], 'strftime') else str(f['created_at'])[:10]}) {f['content']}"
        for f in fragments
    ])
    
    # 调用 AI 进行整理
    prompt = (await get_consolidation_prompt()).format(fragments=fragments_text)
    
    # 使用记忆模型进行整理，和记忆提取/分区摘要共用同一套 MEMORY_* 配置
    consolidation_model = os.getenv("MEMORY_MODEL", "anthropic/claude-haiku-4")
    memory_api_base_url = get_memory_api_base_url()
    if not memory_api_base_url:
        return {"status": "error", "error": "MEMORY_API_BASE_URL 未设置，无法整理记忆（不会回退到主 API_BASE_URL）"}
    
    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            # 最多重试2次（应对429限流）
            last_error = None
            for attempt in range(3):
                response = await client.post(
                    memory_api_base_url,
                    headers={
                        "Authorization": f"Bearer {get_memory_api_key()}",
                        "Content-Type": "application/json"
                    },
                    json={
                        "model": consolidation_model,
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 9000
                    }
                )

                if response.status_code == 429:
                    wait_time = (attempt + 1) * 10
                    print(f"⚠️ 整理API 429限流，{wait_time}秒后重试（第{attempt+1}次）")
                    last_error = f"429 Too Many Requests (重试{attempt+1}次)"
                    await asyncio.sleep(wait_time)
                    continue

                if response.status_code != 200:
                    last_error = f"HTTP {response.status_code}: {response.text[:200]}"
                    print(f"⚠️ 整理API返回 {response.status_code}: {response.text[:200]}")
                    break

                last_error = None
                break

            if last_error:
                return {"status": "error", "error": f"API调用失败: {last_error}"}

            data = response.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            
            # 解析 JSON（三层容错）
            json_match = re.search(r'\[[\s\S]*\]', content)
            if json_match:
                json_str = json_match.group()
                try:
                    events = json.loads(json_str)
                except json.JSONDecodeError:
                    # 方案1：用 strict=False
                    try:
                        events = json.loads(json_str, strict=False)
                    except json.JSONDecodeError:
                        # 方案2：去掉控制字符后重试
                        cleaned = re.sub(r'[\x00-\x1f\x7f]', ' ', json_str)
                        try:
                            events = json.loads(cleaned)
                        except json.JSONDecodeError as e:
                            # 方案3：让 AI 重新格式化
                            print(f"⚠️ JSON解析失败，尝试让AI修复: {e}")
                            fix_resp = await client.post(
                                memory_api_base_url,
                                headers={
                                    "Authorization": f"Bearer {get_memory_api_key()}",
                                    "Content-Type": "application/json"
                                },
                                json={
                                    "model": consolidation_model,
                                    "messages": [{"role": "user", "content": f"请修复以下JSON的语法错误，只输出修复后的JSON数组，不要其他内容：\n{json_str[:2000]}"}],
                                    "max_tokens": 9000
                                }
                            )
                            if fix_resp.status_code == 200:
                                fix_content = fix_resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")
                                fix_match = re.search(r'\[[\s\S]*\]', fix_content)
                                if fix_match:
                                    try:
                                        events = json.loads(fix_match.group())
                                        print(f"✅ AI修复JSON成功")
                                    except json.JSONDecodeError:
                                        return {"status": "error", "error": f"JSON解析失败（AI修复也失败）", "raw": content[:500]}
                                else:
                                    return {"status": "error", "error": "AI修复未返回有效JSON", "raw": content[:500]}
                            else:
                                return {"status": "error", "error": f"JSON解析失败，AI修复请求失败: HTTP {fix_resp.status_code}", "raw": content[:500]}
            else:
                return {"status": "error", "error": "无法解析 AI 返回的 JSON", "raw": content}
            
            # 创建事件记忆并停用碎片
            created_count = 0
            for event in events:
                merged_ids = event.get("merged_ids", [])
                if merged_ids:
                    await create_event_memory(
                        title=event.get("title", ""),
                        content=event.get("content", ""),
                        importance=event.get("importance", 5),
                        event_date=start_date,
                        merged_from=merged_ids
                    )
                    created_count += 1
            
            # 停用所有已处理的碎片
            all_fragment_ids = [f['id'] for f in fragments]
            await deactivate_memories(all_fragment_ids)
            
            return {
                "status": "ok",
                "start_date": str(start_date),
                "end_date": str(end_date),
                "fragments_processed": len(fragments),
                "events_created": created_count
            }
            
    except Exception as e:
        return {"status": "error", "error": str(e)}



@app.get("/api/daily-impressions")
async def api_list_daily_impressions(limit: int = 30):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    rows = await list_daily_impressions(limit)
    return {"status": "ok", "impressions": [_serialize_daily_impression(r) for r in rows]}


@app.get("/api/daily-impressions/{date_str}")
async def api_get_daily_impression(date_str: str):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    impression_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    row = await get_daily_impression(impression_date)
    if not row:
        return {"status": "not_found", "date": date_str}
    return {"status": "ok", "impression": _serialize_daily_impression(row)}


@app.post("/api/daily-impressions/generate")
async def api_generate_daily_impression(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    date_str = data.get("date")
    if not date_str:
        return {"error": "请提供日期"}
    impression_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    return await generate_daily_impression_for_date(impression_date)


@app.post("/api/memories/consolidate")
async def api_manual_consolidate(request: Request):
    """手动触发整理（异步，立即返回）
    
    Body:
        start_date: 开始日期（YYYY-MM-DD 格式）
        end_date: 结束日期（YYYY-MM-DD 格式）
        或
        date: 单个日期（兼容旧版）
    """
    from datetime import date as date_type
    
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    if _consolidate_status.get("running"):
        return {"status": "already_running", "started_at": _consolidate_status.get("started_at")}
    
    data = await request.json()
    
    # 解析日期参数
    if "date" in data and "start_date" not in data:
        start_date = datetime.strptime(data["date"], "%Y-%m-%d").date()
        end_date = start_date
    else:
        start_date_str = data.get("start_date")
        end_date_str = data.get("end_date")
        
        if not start_date_str or not end_date_str:
            return {"error": "请提供开始和结束日期"}
        
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        
        if start_date > end_date:
            return {"error": "开始日期不能晚于结束日期"}
    
    async def _run():
        _consolidate_status.update({"running": True, "started_at": f"{start_date}~{end_date}", "result": None, "error": None})
        try:
            result = await consolidate_memories_for_date_range(start_date, end_date)
            _consolidate_status["result"] = result
            print(f"[manual/consolidate] 整理 {start_date}~{end_date}: {result}")
        except Exception as e:
            _consolidate_status["error"] = str(e)
            print(f"[manual/consolidate] 整理 {start_date}~{end_date} 失败: {e}")
        finally:
            _consolidate_status["running"] = False
    
    asyncio.create_task(_run())
    return {"status": "started", "start_date": str(start_date), "end_date": str(end_date)}


@app.get("/api/memories/consolidate/status")
async def api_consolidate_status():
    """查询整理任务状态"""
    return _consolidate_status


@app.post("/api/memories/{memory_id}/promote")
async def api_promote_to_core(memory_id: int, request: Request):
    """将记忆升级为核心记忆"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    data = await request.json()
    title = data.get("title")
    
    await promote_to_core(memory_id, title=title)
    return {"status": "ok", "memory_id": memory_id, "layer": 3}


@app.post("/api/memories/merge")
async def api_merge_memories(request: Request):
    """手动合并多条记忆"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    data = await request.json()
    memory_ids = data.get("ids", [])
    new_title = data.get("title", "")
    new_content = data.get("content", "")
    importance = data.get("importance", 5)
    layer = data.get("layer", 2)
    
    if not memory_ids or not new_content:
        return {"error": "请提供记忆ID列表和合并后内容"}
    
    new_id = await merge_memories(memory_ids, new_title, new_content, importance, layer)
    return {"status": "ok", "new_id": new_id, "merged": len(memory_ids)}


@app.post("/api/memories/check-duplicate")
async def api_check_duplicate(request: Request):
    """检查记忆是否重复"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    data = await request.json()
    content = data.get("content", "")
    threshold = data.get("threshold", 0.7)
    
    if not content:
        return {"error": "请提供记忆内容"}
    
    result = await check_duplicate_memory(content, threshold)
    return result


@app.post("/api/memories/cleanup-fragments")
async def api_cleanup_fragments(request: Request):
    """清理指定天数前的归档碎片
    
    Body:
        days: 清理多少天前的归档碎片（默认30天）
    """
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    data = await request.json()
    days = data.get("days", 30)
    
    try:
        deleted = await cleanup_old_fragments(days)
        return {"status": "ok", "deleted": deleted, "days": days}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/memories/{memory_id}/revert-merge")
async def api_revert_merge(memory_id: int):
    """撤回合并操作：恢复原始碎片，删除合并后的事件记忆"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    try:
        result = await revert_merge(memory_id)
        return result
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/memories/{memory_id}/restore")
async def api_restore_memory(memory_id: int):
    """恢复已归档的记忆（将 is_active 设为 TRUE）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    try:
        await update_memory_with_layer(memory_id, is_active=True)
        return {"status": "ok", "id": memory_id}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/memories/layer-stats")
async def api_layer_statistics():
    """获取各层记忆统计数据"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    try:
        stats = await get_layer_statistics()
        return stats
    except Exception as e:
        return {"error": str(e)}


@app.post("/import/text")
async def import_text_memories(request: Request):
    """从纯文本导入记忆（每行一条），可选自动评分"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用（设置 MEMORY_ENABLED=true 开启）"}
    
    try:
        data = await request.json()
        lines = data.get("lines", [])
        skip_scoring = data.get("skip_scoring", False)
        
        if not lines:
            return {"error": "没有找到记忆条目"}
        
        if skip_scoring:
            scored = [{"content": t, "importance": 5} for t in lines]
        else:
            scored = await score_memories(lines)
        
        imported = 0
        skipped = 0
        
        for mem in scored:
            content = mem.get("content", "")
            if not content:
                continue
            
            pool = await get_pool()
            async with pool.acquire() as conn:
                existing = await conn.fetchval(
                    "SELECT COUNT(*) FROM memories WHERE content = $1", content
                )
            
            if existing > 0:
                skipped += 1
                continue
            
            await save_memory(
                content=content,
                importance=mem.get("importance", 5),
                source_session="text-import",
            )
            imported += 1
        
        total = await get_all_memories_count()
        return {
            "status": "done",
            "imported": imported,
            "skipped": skipped,
            "total": total,
        }
    except Exception as e:
        return {"error": str(e)}


@app.post("/import/memories")
async def import_memories(request: Request):
    """从 JSON 导入记忆（用于迁移或恢复备份）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用（设置 MEMORY_ENABLED=true 开启）"}
    
    try:
        data = await request.json()
        memories = data.get("memories", [])
        
        if not memories:
            return {"error": "没有找到记忆数据，请确认 JSON 格式正确"}
        
        imported = 0
        skipped = 0
        
        for mem in memories:
            content = mem.get("content", "")
            if not content:
                continue
            
            pool = await get_pool()
            async with pool.acquire() as conn:
                existing = await conn.fetchval(
                    "SELECT COUNT(*) FROM memories WHERE content = $1", content
                )
            
            if existing > 0:
                skipped += 1
                continue
            
            await save_memory(
                content=content,
                importance=mem.get("importance", 5),
                source_session=mem.get("source_session", "json-import"),
            )
            imported += 1
        
        total = await get_all_memories_count()
        return {
            "status": "done",
            "imported": imported,
            "skipped": skipped,
            "total": total,
        }
    except Exception as e:
        return {"error": str(e)}


# ============================================================
# 对话记录管理 API
# ============================================================

@app.get("/api/conversations")
async def api_conversations(page: int = 1, per_page: int = 20):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        results, total = await get_conversations_paginated(page, per_page)
        total_pages = max(1, -(-total // per_page))  # 向上取整
        return {"conversations": results, "total": total, "page": page, "per_page": per_page, "total_pages": total_pages}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/conversations/{session_id}/messages")
async def api_conversation_messages(session_id: str, limit: int = 50, offset: int = 0):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM conversations WHERE session_id = $1", session_id
            )
            rows = await conn.fetch("""
                SELECT id, role, content, metadata, created_at
                FROM conversations WHERE session_id = $1
                ORDER BY created_at DESC
                LIMIT $2 OFFSET $3
            """, session_id, limit, offset)
        msgs = []
        for r in rows:
            metadata = None
            if r.get("metadata"):
                try:
                    metadata = json.loads(r["metadata"])
                except Exception:
                    metadata = r["metadata"]
            content = r["content"]
            if (
                r.get("role") == "assistant"
                and not metadata
                and isinstance(content, str)
                and content.startswith("工具调用:")
            ):
                content = " "
            msgs.append({
                "id": r["id"],
                "role": r["role"],
                "content": content,
                "metadata": metadata,
                "created_at": r["created_at"].isoformat() if r.get("created_at") else None,
            })
        return {"messages": msgs, "total": total}
    except Exception as e:
        return {"error": str(e)}


@app.delete("/api/conversations/{session_id}")
async def api_delete_conversation(session_id: str):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        await delete_conversation(session_id)
        return {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/conversations/batch-delete")
async def api_batch_delete(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        body = await request.json()
        ids = body.get("session_ids", [])
        if ids:
            await batch_delete_conversations(ids)
        return {"status": "ok", "deleted": len(ids)}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/admin/merge-sessions")
async def api_merge_sessions(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        body = await request.json()
        source_ids = [s for s in body.get("source_ids", []) if s != body.get("target_id", "")]
        target_id = body.get("target_id", "")
        if not source_ids or not target_id:
            return {"error": "source_ids 和 target_id 不能为空"}
        result = await merge_sessions_to_target(source_ids, target_id)
        return {"status": "ok", **result}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/chat/search")
async def api_search_conversations(q: str = "", limit: int = 20, offset: int = 0):
    """搜索对话内容"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    if not q.strip():
        return {"error": "搜索关键词不能为空", "results": [], "total": 0}
    try:
        results, total = await search_conversations(q.strip(), limit, offset)
        return {"results": results, "total": total}
    except Exception as e:
        return {"error": str(e), "results": [], "total": 0}


@app.patch("/api/chat/messages/{message_id}")
async def api_update_message(message_id: int, request: Request):
    """编辑单条消息内容"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        body = await request.json()
        content = body.get("content", "").strip()
        if not content:
            return {"error": "内容不能为空"}
        updated = await update_message_content(message_id, content)
        if updated == 0:
            return {"error": "消息不存在"}
        return {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}


async def _delete_message_by_id(message_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM conversations WHERE id = $1", message_id)
    deleted = int(result.split()[-1]) if result else 0
    if deleted == 0:
        return {"error": "消息不存在"}
    return {"status": "ok", "deleted": deleted}





@app.delete("/api/messages/{message_id}")
async def api_delete_message_legacy(message_id: int):
    """删除单条对话消息（兼容 Dashboard 旧接口）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        return await _delete_message_by_id(message_id)
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/conversations/export")
async def api_export_conversations():
    """导出所有对话记录"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await export_all_conversations()
        return JSONResponse(content=data)
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/conversations/import")
async def api_import_conversations(request: Request):
    """导入对话记录（JSON格式，自动去重）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        records = await request.json()
        if not isinstance(records, list):
            return {"error": "格式错误：需要 JSON 数组"}
        imported, skipped = await import_conversations(records)
        return {"status": "ok", "imported": imported, "skipped": skipped, "total": imported + skipped}
    except Exception as e:
        return {"error": str(e)}


# ============================================================
# 对话线管理 API（分区缓存）
# ============================================================

@app.get("/api/partition/status")
async def api_partition_status():
    active_sid = get_active_session_id()
    state = await get_session_cache_state(active_sid) if active_sid else {}
    return {
        "enabled": CACHE_PARTITION_ENABLED,
        "active_session_id": active_sid,
        "partition_x": CACHE_PARTITION_X,
        "summary_model": os.getenv("MEMORY_MODEL", "anthropic/claude-haiku-4"),
        "summary": '\n\n'.join(state.get('summary_parts', [])),
        "summary_parts": state.get('summary_parts', []),
        "summary_count": len(state.get('summary_parts', [])),
        "summary_length": sum(len(p) for p in state.get('summary_parts', [])),
        "a_start_round": state.get('a_start_round', 0),
        "updated_at": state.get('updated_at').isoformat() if state.get('updated_at') else None,
    }


@app.get("/api/partition/threads")
async def api_partition_threads():
    threads = await list_all_session_cache_states()
    active_sid = get_active_session_id()
    for t in threads:
        t['is_active'] = (t['session_id'] == active_sid)
    if active_sid and not any(t['session_id'] == active_sid for t in threads):
        threads.insert(0, {'session_id': active_sid, 'summary': '', 'summary_length': 0, 'summary_count': 0, 'a_start_round': 0, 'updated_at': None, 'message_count': 0, 'chat_tokens': 0, 'is_active': True})
    return {"threads": threads, "active_session_id": active_sid}


@app.put("/api/partition/summary")
async def api_update_summary(request: Request):
    try:
        body = await request.json()
        sid = body.get("session_id", "")
        summary = body.get("summary", "")
        if not sid:
            return {"error": "session_id 不能为空"}
        state = await get_session_cache_state(sid)
        summary_parts = [summary] if isinstance(summary, str) and summary else summary if isinstance(summary, list) else []
        # 摘要清空时 a_start_round 也归零，否则历史会被跳过
        a_start = state.get('a_start_round', 0) if summary_parts else 0
        await save_session_cache_state(sid, summary_parts, a_start)
        total_len = sum(len(p) for p in summary_parts)
        return {"status": "ok", "summary_parts": len(summary_parts), "summary_length": total_len}
    except Exception as e:
        return {"error": str(e)}


@app.delete("/api/partition/summary")
async def api_clear_summary(request: Request):
    try:
        body = await request.json()
        sid = body.get("session_id", "")
        if not sid:
            return {"error": "session_id 不能为空"}
        # 摘要和 a_start_round 一起归零
        await save_session_cache_state(sid, [], 0)
        return {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/partition/thread")
async def api_create_thread(request: Request):
    try:
        body = await request.json()
        new_id = body.get("session_id", "").strip()
        copy_from = body.get("copy_summary_from", "")
        if not new_id:
            return {"error": "session_id 不能为空"}
        existing = await get_session_cache_state(new_id)
        if existing.get('updated_at'):
            return {"error": f"对话线 '{new_id}' 已存在"}
        summary_parts = []
        if copy_from:
            source = await get_session_cache_state(copy_from)
            summary_parts = source.get('summary_parts', [])
        await save_session_cache_state(new_id, summary_parts, 0)
        total_len = sum(len(p) for p in summary_parts)
        return {"status": "ok", "session_id": new_id, "summary_length": total_len}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/partition/switch")
async def api_switch_thread(request: Request):
    global PARTITION_SESSION_ID
    try:
        body = await request.json()
        new_id = body.get("session_id", "").strip()
        if not new_id:
            return {"error": "session_id 不能为空"}
        old_id = PARTITION_SESSION_ID
        PARTITION_SESSION_ID = new_id
        await set_gateway_config("partition_session_id", new_id)
        return {"status": "ok", "old_session_id": old_id, "new_session_id": new_id}
    except Exception as e:
        return {"error": str(e)}


@app.put("/api/partition/thread/rename")
async def api_rename_thread(request: Request):
    global PARTITION_SESSION_ID
    try:
        body = await request.json()
        old_id = body.get("old_id", "").strip()
        new_id = body.get("new_id", "").strip()
        if not old_id or not new_id:
            return {"error": "old_id 和 new_id 不能为空"}
        if old_id == new_id:
            return {"error": "新旧ID相同"}
        success = await rename_session_id(old_id, new_id)
        if not success:
            return {"error": f"对话线 '{new_id}' 已存在"}
        # 如果重命名的是活跃线，同步更新
        if PARTITION_SESSION_ID == old_id:
            PARTITION_SESSION_ID = new_id
            await set_gateway_config("partition_session_id", new_id)
        return {"status": "ok", "old_id": old_id, "new_id": new_id}
    except Exception as e:
        return {"error": str(e)}


@app.delete("/api/partition/thread/{session_id:path}")
async def api_delete_thread(session_id: str):
    """删除对话线（不允许删除当前活跃线）"""
    try:
        active_sid = get_active_session_id()
        if session_id == active_sid:
            return {"error": "不能删除当前活跃的对话线"}
        await delete_session_cache_state(session_id)
        print(f"🗑️ 删除对话线: {session_id}")
        return {"status": "ok", "session_id": session_id}
    except Exception as e:
        return {"error": str(e)}


# ============================================================
# 记忆向量补算（带进度追踪）
# ============================================================

_backfill_mem_status = {
    "running": False,
    "total": 0,
    "done": 0,
    "error": None,
    "finished_at": None,
}

@app.post("/api/admin/backfill-memory-embeddings")
async def api_backfill_memory_embeddings():
    """给已有记忆补算embedding（后台异步执行，前端轮询进度）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    
    if _backfill_mem_status["running"]:
        return {"error": "补算任务正在运行中，请等待完成"}
    
    try:
        total = await get_pending_memory_embedding_count()
    except Exception as e:
        return {"error": f"查询待处理数量失败: {e}"}
    
    if total == 0:
        return {"status": "done", "message": "所有记忆已有embedding，无需补算", "total": 0, "done": 0}
    
    _backfill_mem_status["running"] = True
    _backfill_mem_status["total"] = total
    _backfill_mem_status["done"] = 0
    _backfill_mem_status["error"] = None
    _backfill_mem_status["finished_at"] = None
    
    async def run_backfill():
        try:
            while _backfill_mem_status["running"]:
                updated = await backfill_memory_embeddings(batch_size=20)
                _backfill_mem_status["done"] += updated
                
                if updated == 0:
                    break
                
                await asyncio.sleep(1)
            
            _backfill_mem_status["finished_at"] = datetime.now(timezone.utc).isoformat()
            print(f"✅ 记忆embedding补算完成：{_backfill_mem_status['done']}/{_backfill_mem_status['total']}")
        except Exception as e:
            _backfill_mem_status["error"] = str(e)
            print(f"❌ 记忆embedding补算异常: {e}")
        finally:
            _backfill_mem_status["running"] = False
    
    asyncio.create_task(run_backfill())
    return {"status": "started", "total": total}

@app.get("/api/admin/backfill-memory-embeddings/status")
async def api_backfill_memory_embeddings_status():
    """查询记忆embedding补算进度"""
    return {
        "running": _backfill_mem_status["running"],
        "total": _backfill_mem_status["total"],
        "done": _backfill_mem_status["done"],
        "error": _backfill_mem_status["error"],
        "finished_at": _backfill_mem_status["finished_at"],
    }


# ============================================================
# 模型列表 API（/api/models）
# 设置面板的 combo-box 用，根据 API_BASE_URL 自动适配
# ============================================================

@app.get("/api/models")
async def get_models():
    """获取可用模型列表（根据 API_BASE_URL 自动适配）"""
    is_openrouter = "openrouter.ai" in API_BASE_URL
    is_google = "googleapis.com" in API_BASE_URL or "generativelanguage" in API_BASE_URL
    is_openai = "api.openai.com" in API_BASE_URL

    try:
        if is_openrouter:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    "https://openrouter.ai/api/v1/models",
                    headers={"Authorization": f"Bearer {API_KEY}"}
                )
                if response.status_code == 200:
                    data = response.json()
                    models = data.get("data", [])
                    simplified = [{"id": m.get("id"), "name": m.get("name"), "context_length": m.get("context_length")} for m in models]
                    simplified.sort(key=lambda x: x.get("name", ""))
                    return {"models": simplified, "total": len(simplified), "provider": "openrouter"}

        elif is_google:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"https://generativelanguage.googleapis.com/v1beta/models?key={API_KEY}"
                )
                if response.status_code == 200:
                    data = response.json()
                    models = data.get("models", [])
                    simplified = []
                    for m in models:
                        full_name = m.get("name", "")
                        model_id = full_name.replace("models/", "") if full_name.startswith("models/") else full_name
                        display_name = m.get("displayName", model_id)
                        supported_methods = m.get("supportedGenerationMethods", [])
                        if "generateContent" in supported_methods:
                            simplified.append({"id": model_id, "name": display_name, "context_length": m.get("inputTokenLimit"), "output_limit": m.get("outputTokenLimit")})
                    def sort_key(x):
                        name = x.get("id", "")
                        if "gemini-3" in name: return "0" + name
                        elif "gemini-2.5" in name: return "1" + name
                        elif "gemini-2.0" in name: return "2" + name
                        else: return "9" + name
                    simplified.sort(key=sort_key)
                    return {"models": simplified, "total": len(simplified), "provider": "google"}
                else:
                    print(f"[get_models] Google API 返回 {response.status_code}: {response.text}")
                    return {"error": f"Google API 返回 {response.status_code}", "models": [], "provider": "google"}

        elif is_openai:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    "https://api.openai.com/v1/models",
                    headers={"Authorization": f"Bearer {API_KEY}"}
                )
                if response.status_code == 200:
                    data = response.json()
                    models = data.get("data", [])
                    simplified = [{"id": m.get("id", ""), "name": m.get("id", "")} for m in models if m.get("id", "").startswith(("gpt-", "o1", "o3", "o4"))]
                    simplified.sort(key=lambda x: x.get("id", ""))
                    return {"models": simplified, "total": len(simplified), "provider": "openai"}
            openai_models = [
                {"id": "gpt-4.1", "name": "GPT-4.1"},
                {"id": "gpt-4o", "name": "GPT-4o"},
                {"id": "gpt-4o-mini", "name": "GPT-4o Mini"},
                {"id": "o3-mini", "name": "o3-mini"},
            ]
            return {"models": openai_models, "total": len(openai_models), "provider": "openai"}

        else:
            return {"models": [], "total": 0, "provider": "unknown", "note": "未识别的 API，请手动输入模型名"}

    except Exception as e:
        print(f"[get_models] 错误: {e}")
        return {"error": str(e), "models": []}


# ============================================================
# 高级设置面板 API（/api/settings）
# Dashboard 前端设置面板用，管理所有运行时可调配置
# ============================================================

def _mask_key(key_value: str) -> str:
    """API Key 打码：只露前5位和后4位"""
    if not key_value:
        return ""
    if len(key_value) < 10:
        return "****"
    return key_value[:5] + "****" + key_value[-4:]


def _is_masked(value: str) -> bool:
    """判断值是否是打码值（用户没改过）"""
    return "****" in str(value)


def _parse_bool(val, fallback=False) -> bool:
    """解析布尔值（兼容字符串/布尔/None）"""
    if val is None:
        return fallback
    if isinstance(val, bool):
        return val
    return str(val).lower() in ("true", "1", "yes")


@app.get("/api/settings")
async def get_settings():
    """获取高级设置（数据库优先，fallback 到环境变量/运行时默认值）"""
    try:
        db = await get_all_gateway_config()

        # --- 基础连接 ---
        api_key_raw = db.get("API_KEY") or API_KEY
        embedding_key_raw = db.get("EMBEDDING_API_KEY") or _db_module.EMBEDDING_API_KEY

        memory_key_raw = db.get("MEMORY_API_KEY") or MEMORY_API_KEY

        settings = {
            # 基础连接
            "API_BASE_URL":     db.get("API_BASE_URL") or str(API_BASE_URL),
            "API_KEY":          _mask_key(api_key_raw),
            "DEFAULT_MODEL":    db.get("DEFAULT_MODEL") or str(DEFAULT_MODEL),
        "CHAT_TEMPERATURE": db.get("CHAT_TEMPERATURE") or str(CHAT_TEMPERATURE),

            # 记忆系统
            "MEMORY_ENABLED":          _parse_bool(db.get("MEMORY_ENABLED"), MEMORY_ENABLED),
            "MEMORY_API_KEY":          _mask_key(memory_key_raw),
            "MEMORY_API_BASE_URL":     db.get("MEMORY_API_BASE_URL") or str(MEMORY_API_BASE_URL),
            "MEMORY_MODEL":            db.get("MEMORY_MODEL") or os.environ.get("MEMORY_MODEL", ""),
            "MAX_MEMORIES_INJECT":     int(db.get("MAX_MEMORIES_INJECT") or MAX_MEMORIES_INJECT),
            "MIN_SCORE_THRESHOLD":     float(db.get("MIN_SCORE_THRESHOLD") or _db_module.MIN_SCORE_THRESHOLD),
            "MEMORY_EXTRACT_INTERVAL": int(db.get("MEMORY_EXTRACT_INTERVAL") or MEMORY_EXTRACT_INTERVAL),

            # 缓存分区
            "CACHE_PARTITION_ENABLED": _parse_bool(db.get("CACHE_PARTITION_ENABLED"), CACHE_PARTITION_ENABLED),
            "CACHE_PARTITION_X":       int(db.get("CACHE_PARTITION_X") or CACHE_PARTITION_X),
            "CACHE_PARTITION_TRIGGER": db.get("CACHE_PARTITION_TRIGGER") or CACHE_PARTITION_TRIGGER,
            "CACHE_PARTITION_WINDOW":  int(db.get("CACHE_PARTITION_WINDOW") or CACHE_PARTITION_WINDOW),
            "CACHE_SUMMARY_MODEL":     db.get("CACHE_SUMMARY_MODEL") or str(CACHE_SUMMARY_MODEL),

            # 向量搜索（开源版用 EMBEDDING_API_KEY + EMBEDDING_BASE_URL）
            "MEMORY_VECTOR_ENABLED":   _parse_bool(db.get("MEMORY_VECTOR_ENABLED"), _db_module.MEMORY_VECTOR_ENABLED),
            "EMBEDDING_API_KEY":       _mask_key(embedding_key_raw),
            "EMBEDDING_BASE_URL":      db.get("EMBEDDING_BASE_URL") or str(_db_module.EMBEDDING_BASE_URL),
            "EMBEDDING_MODEL":         db.get("EMBEDDING_MODEL") or str(_db_module.EMBEDDING_MODEL),
            "EMBEDDING_DIM":           int(db.get("EMBEDDING_DIM") or _db_module.EMBEDDING_DIM),

            # 搜索权重
            "MEMORY_HW_KEYWORD":        float(db.get("MEMORY_HW_KEYWORD") or _db_module.MEMORY_HW_KEYWORD),
            "MEMORY_HW_SEMANTIC":       float(db.get("MEMORY_HW_SEMANTIC") or _db_module.MEMORY_HW_SEMANTIC),
            "MEMORY_HW_IMPORTANCE":     float(db.get("MEMORY_HW_IMPORTANCE") or _db_module.MEMORY_HW_IMPORTANCE),
            "MEMORY_HW_RECENCY":        float(db.get("MEMORY_HW_RECENCY") or _db_module.MEMORY_HW_RECENCY),
            "MEMORY_SEMANTIC_THRESHOLD": float(db.get("MEMORY_SEMANTIC_THRESHOLD") or _db_module.MEMORY_SEMANTIC_THRESHOLD),

            # 其他
            "FORCE_STREAM":       _parse_bool(db.get("FORCE_STREAM"), FORCE_STREAM),
            "RESPONSE_TRANSFORM_ENABLED": _parse_bool(db.get("RESPONSE_TRANSFORM_ENABLED"), RESPONSE_TRANSFORM_ENABLED),
            "RESPONSE_TRANSFORM_RULES": db.get("RESPONSE_TRANSFORM_RULES") or str(RESPONSE_TRANSFORM_RULES),
            "REASONING_EFFORT":   db.get("REASONING_EFFORT") or str(REASONING_EFFORT),

            # System Prompt
            "systemPrompt": db.get("systemPrompt") or _DEFAULT_SYSTEM_PROMPT or "",

            # 记忆提取提示词
            "extractionPrompt": db.get("extractionPrompt") or _DEFAULT_EXTRACTION_PROMPT or "",
            "consolidationPrompt": db.get("consolidationPrompt") or _DEFAULT_CONSOLIDATION_PROMPT or "",
            "dailyImpressionPrompt": db.get("dailyImpressionPrompt") or _DEFAULT_DAILY_IMPRESSION_PROMPT or "",
            "modelPresets": json.loads(db.get("modelPresets") or "[]"),
        }

        return {"status": "ok", "settings": settings}
    except Exception as e:
        print(f"[get_settings] 错误: {e}")
        return {"error": str(e)}


@app.post("/api/settings/test-memory-model")
async def test_memory_model(request: Request):
    """测试记忆模型接口是否可用（OpenAI chat/completions 兼容格式）"""
    try:
        data = await request.json()

        memory_api_base_url = str(data.get("MEMORY_API_BASE_URL") or MEMORY_API_BASE_URL or "").strip()
        memory_model = str(data.get("MEMORY_MODEL") or os.getenv("MEMORY_MODEL", "") or "anthropic/claude-haiku-4").strip()
        memory_api_key_raw = str(data.get("MEMORY_API_KEY") or "").strip()
        memory_api_key = get_memory_api_key() if (not memory_api_key_raw or _is_masked(memory_api_key_raw)) else memory_api_key_raw

        if not memory_api_base_url:
            return {"ok": False, "error": "MEMORY_API_BASE_URL 为空，记忆模型不会回退到主 API_BASE_URL"}
        if not memory_api_key:
            return {"ok": False, "error": "MEMORY_API_KEY / API_KEY 为空"}
        if not memory_model:
            return {"ok": False, "error": "MEMORY_MODEL 为空"}

        headers = {
            "Authorization": f"Bearer {memory_api_key}",
            "Content-Type": "application/json",
        }
        if "openrouter" in memory_api_base_url:
            headers["HTTP-Referer"] = EXTRA_REFERER
            headers["X-Title"] = EXTRA_TITLE

        payload = {
            "model": memory_model,
            "messages": [
                {"role": "user", "content": "请只回复 OK，用于测试记忆模型接口连通性。"}
            ],
            "max_tokens": 20,
            "temperature": 0,
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(memory_api_base_url, headers=headers, json=payload)

        if resp.status_code != 200:
            return {
                "ok": False,
                "status_code": resp.status_code,
                "error": f"HTTP {resp.status_code}: {resp.text[:500]}",
            }

        try:
            resp_data = resp.json()
        except Exception:
            return {"ok": False, "error": f"响应不是 JSON: {resp.text[:500]}"}

        reply = resp_data.get("choices", [{}])[0].get("message", {}).get("content", "")
        if reply is None:
            reply = ""
        if "choices" not in resp_data:
            return {"ok": False, "error": f"接口返回成功，但不是 OpenAI chat/completions 格式: {str(resp_data)[:500]}"}

        return {"ok": True, "status_code": resp.status_code, "model": memory_model, "reply": str(reply)[:200]}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.put("/api/settings")
async def save_settings(request: Request):
    """保存高级设置（写入数据库 + 热更新运行时变量，立即生效无需重启）"""
    try:
        data = await request.json()
        updated = []
        skipped = []

        # main.py 全局变量映射（key → 类型转换函数）
        _MAIN_VARS = {
            "API_BASE_URL":          str,
            "API_KEY":               str,
            "DEFAULT_MODEL":         str,
            "CHAT_TEMPERATURE":      str,
            "MEMORY_API_KEY":        str,
            "MEMORY_API_BASE_URL":   str,
            "MEMORY_ENABLED":        lambda v: _parse_bool(v),
            "MAX_MEMORIES_INJECT":   int,
            "MEMORY_EXTRACT_INTERVAL": int,
            "CACHE_PARTITION_ENABLED": lambda v: _parse_bool(v),
            "CACHE_PARTITION_X":     int,
            "CACHE_PARTITION_TRIGGER": str,
            "CACHE_PARTITION_WINDOW": int,
            "CACHE_SUMMARY_MODEL":   str,
            "FORCE_STREAM":          lambda v: _parse_bool(v),
            "RESPONSE_TRANSFORM_ENABLED": lambda v: _parse_bool(v),
            "RESPONSE_TRANSFORM_RULES": str,
            "REASONING_EFFORT":      str,
        }

        # database.py 全局变量映射（开源版用 EMBEDDING_API_KEY + EMBEDDING_BASE_URL）
        _DB_VARS = {
            "EMBEDDING_API_KEY":       str,
            "EMBEDDING_BASE_URL":      str,
            "EMBEDDING_MODEL":         str,
            "EMBEDDING_DIM":           int,
            "MIN_SCORE_THRESHOLD":     float,
            "MEMORY_VECTOR_ENABLED":   lambda v: _parse_bool(v),
            "MEMORY_HW_KEYWORD":       float,
            "MEMORY_HW_SEMANTIC":      float,
            "MEMORY_HW_IMPORTANCE":    float,
            "MEMORY_HW_RECENCY":       float,
            "MEMORY_SEMANTIC_THRESHOLD": float,
        }

        # 只存 os.environ 的变量
        _ENV_ONLY = {"MEMORY_MODEL": str}

        # 打码字段
        _MASKED_KEYS = {"API_KEY", "EMBEDDING_API_KEY", "MEMORY_API_KEY"}

        for key, value in data.items():
            # --- 打码字段特殊处理 ---
            if key in _MASKED_KEYS:
                str_val = str(value).strip()
                if _is_masked(str_val):
                    skipped.append(key)
                    continue
                if not str_val:
                    await set_gateway_config(key, "")
                    if key in _MAIN_VARS:
                        globals()[key] = ""
                    elif key in _DB_VARS:
                        setattr(_db_module, key, "")
                    if key == "MEMORY_API_KEY":
                        import memory_extractor as _me_mod
                        _me_mod.MEMORY_API_KEY = ""
                    os.environ[key] = ""
                    updated.append(key)
                    continue

            # --- systemPrompt 特殊处理 ---
            if key == "systemPrompt":
                await set_gateway_config("systemPrompt", str(value))
                invalidate_system_prompt_cache()
                updated.append("systemPrompt")
                print(f"[settings] systemPrompt 已更新（{len(str(value))} 字）")
                continue

            # --- extractionPrompt 特殊处理 ---
            if key == "extractionPrompt":
                await set_gateway_config("extractionPrompt", str(value))
                set_extraction_prompt(str(value))
                updated.append("extractionPrompt")
                continue

            # --- consolidationPrompt 特殊处理 ---
            if key == "consolidationPrompt":
                await set_gateway_config("consolidationPrompt", str(value))
                set_consolidation_prompt(str(value))
                updated.append("consolidationPrompt")
                continue

            # --- dailyImpressionPrompt 特殊处理 ---
            if key == "dailyImpressionPrompt":
                await set_gateway_config("dailyImpressionPrompt", str(value))
                set_daily_impression_prompt(str(value))
                updated.append("dailyImpressionPrompt")
                continue

            # --- modelPresets 特殊处理 ---
            if key == "modelPresets":
                presets_json = json.dumps(value) if isinstance(value, list) else str(value)
                await set_gateway_config("modelPresets", presets_json)
                updated.append("modelPresets")
                continue

            # --- activatePreset 特殊处理（激活某个预设 → 切换 DEFAULT_MODEL / URL / Key）---
            if key == "activatePreset":
                new_model = str(value)
                globals()["DEFAULT_MODEL"] = new_model
                await set_gateway_config("DEFAULT_MODEL", new_model)
                updated.append(f"DEFAULT_MODEL→{new_model}")
                continue

            if key == "activatePresetUrl":
                if value:
                    globals()["API_BASE_URL"] = str(value)
                    await set_gateway_config("API_BASE_URL", str(value))
                    updated.append(f"API_BASE_URL→{value}")
                continue

            if key == "activatePresetKey":
                if value and not _is_masked(str(value)):
                    globals()["API_KEY"] = str(value)
                    await set_gateway_config("API_KEY", str(value))
                    updated.append(f"API_KEY→***")
                continue


            # --- 常规字段 ---
            await set_gateway_config(key, str(value))

            if key in _MAIN_VARS:
                typed_value = _MAIN_VARS[key](value)
                globals()[key] = typed_value
                os.environ[key] = str(value)
                if key == "MEMORY_API_KEY":
                    import memory_extractor as _me_mod
                    _me_mod.MEMORY_API_KEY = str(value)
                if key == "MEMORY_API_BASE_URL":
                    import memory_extractor as _me_mod
                    _me_mod.MEMORY_API_BASE_URL = str(value)
                updated.append(key)
                print(f"[settings] {key} = {typed_value}")

            elif key in _DB_VARS:
                typed_value = _DB_VARS[key](value)
                setattr(_db_module, key, typed_value)
                os.environ[key] = str(value)
                updated.append(key)
                print(f"[settings] {key} = {typed_value} (database)")

            elif key in _ENV_ONLY:
                typed_value = _ENV_ONLY[key](value)
                os.environ[key] = str(typed_value)
                if key == "MEMORY_MODEL":
                    import memory_extractor as _me_mod
                    _me_mod.MEMORY_MODEL = str(typed_value)
                updated.append(key)
                print(f"[settings] {key} = {typed_value} (env)")

            else:
                skipped.append(key)

        return {
            "status": "ok",
            "updated": updated,
            "skipped": skipped,
            "message": f"已更新 {len(updated)} 项配置，立即生效"
        }
    except Exception as e:
        print(f"[save_settings] 错误: {e}")
        return {"error": str(e)}


# ============================================================

if __name__ == "__main__":
    import uvicorn
    print(f"🚀 AI Memory Gateway 启动中... 端口 {PORT}")
    print(f"📝 人设长度：{len(SYSTEM_PROMPT)} 字符")
    print(f"🤖 默认模型：{DEFAULT_MODEL}")
    print(f"🔗 API 地址：{API_BASE_URL}")
    print(f"🧠 记忆系统：{'开启' if MEMORY_ENABLED else '关闭'}")
    if MEMORY_ENABLED:
        print(f"📝 记忆提取+注入：{'开启' if MEMORY_EXTRACT_ENABLED else '关闭'}")
    print(f"🔄 记忆提取间隔：{'禁用' if MEMORY_EXTRACT_INTERVAL == 0 else '每轮提取' if MEMORY_EXTRACT_INTERVAL == 1 else f'每 {MEMORY_EXTRACT_INTERVAL} 轮提取一次'}")
    if CACHE_PARTITION_ENABLED:
        print(f"🔒 分区缓存：开启 (X={CACHE_PARTITION_X}, session={PARTITION_SESSION_ID or '未设置'})")
    if FORCE_STREAM:
        print(f"⚡ 强制流式传输：开启")
    if REASONING_EFFORT:
        print(f"🧠 推理参数注入：{REASONING_EFFORT}")
    if str(CHAT_TEMPERATURE).strip() != "":
        print(f"🌡️ 聊天温度参数：{CHAT_TEMPERATURE}")
    if RESPONSE_TRANSFORM_ENABLED:
        print("🔁 非流式响应转换：开启")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
