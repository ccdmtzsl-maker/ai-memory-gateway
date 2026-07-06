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

from database import init_tables, close_pool, save_message, get_pool, get_gateway_config, set_gateway_config, get_all_gateway_config, get_conversation_messages, get_session_cache_state, save_session_cache_state, delete_session_cache_state, save_token_usage, ensure_token_usage_table, get_conversations_paginated, delete_conversation, batch_delete_conversations, merge_sessions_to_target, list_all_session_cache_states, export_all_conversations, import_conversations, get_last_user_content, update_last_assistant_message, db_row_to_message, search_conversations, update_message_content, rename_session_id, get_conversation_messages_by_date, upsert_daily_impression, get_daily_impression, list_daily_impressions
from database import list_memory_palace_rooms, list_memory_palace_nodes, get_memory_palace_node, create_memory_palace_node, update_memory_palace_node, delete_memory_palace_node, clear_expired_memory_palace_pins, get_user_impression, upsert_user_impression, delete_user_impression, normalize_user_impression
import database as _db_module  # 用于 /api/settings 热更新 database.py 全局变量
from memory_extractor import get_extraction_prompt, set_extraction_prompt, _DEFAULT_EXTRACTION_PROMPT

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

# 分区缓存
CACHE_PARTITION_ENABLED = os.getenv("CACHE_PARTITION_ENABLED", "false").lower() == "true"
CACHE_PARTITION_X = int(os.getenv("CACHE_PARTITION_X", "15"))
# 分区自动提取最多处理的最新消息数；先按 cursor 过滤，再只取最新 N 条，过旧积压直接跳过。
CACHE_PARTITION_EXTRACT_LIMIT = int(os.getenv("CACHE_PARTITION_EXTRACT_LIMIT", "120"))
CACHE_SUMMARY_MODEL = os.getenv("CACHE_SUMMARY_MODEL", "anthropic/claude-haiku-4.5")
CACHE_PARTITION_TRIGGER = os.getenv("CACHE_PARTITION_TRIGGER", "rounds")  # rounds=按轮次 | time=按时间窗口
CACHE_PARTITION_WINDOW = int(os.getenv("CACHE_PARTITION_WINDOW", "30"))  # 时间窗口（分钟），仅 trigger=time 时生效
CACHE_PARTITION_KEEP_A_TOOLS = os.getenv("CACHE_PARTITION_KEEP_A_TOOLS", "false").lower() == "true"  # A区是否保留tool/tool_calls
PARTITION_SESSION_ID = os.getenv("PARTITION_SESSION_ID", "")
TOOL_CHAIN_DEBUG = os.getenv("TOOL_CHAIN_DEBUG", "false").lower() == "true"  # 工具链结构诊断日志

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

# Memory Palace 分区自动提取锁：同一角色/会话串行化，避免并发请求重复处理同一批 cursor 区间。
_memory_palace_auto_extract_locks = {}
# 分区后台维护锁：保护 a_start_round 读取/轮转/保存/提取调度，避免同一会话后台任务互相覆盖状态。
_partition_auto_maintenance_locks = {}

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

# 记忆宫殿提取中称呼用户用的昵称；留空则使用“用户”
USER_NICKNAME = os.getenv("USER_NICKNAME", "用户")

# 当前角色名称；用于用户画像等需要明确角色视角的提示词
CHARACTER_NAME = os.getenv("CHARACTER_NAME", "澈")

# 记忆宫殿默认注入数量；是否启用跟随 MEMORY_ENABLED 总开关
MEMORY_PALACE_DEFAULT_LIMIT = int(os.getenv("MEMORY_PALACE_DEFAULT_LIMIT", "5"))

# 关键词触发上下文（轻量世界书）：仅当前轮临时注入 system，不写入历史。
KEYWORD_CONTEXT_ENABLED = os.getenv("KEYWORD_CONTEXT_ENABLED", "false").lower() == "true"
KEYWORD_CONTEXT_RULES = os.getenv("KEYWORD_CONTEXT_RULES", "[]")
MEMORY_PALACE_EVENT_BOX_COMPRESS_THRESHOLD = int(os.getenv("MEMORY_PALACE_EVENT_BOX_COMPRESS_THRESHOLD", "4"))
MEMORY_PALACE_EVENT_BOX_LIVE_HARD_CAP = int(os.getenv("MEMORY_PALACE_EVENT_BOX_LIVE_HARD_CAP", "16"))
MEMORY_PALACE_EVENT_BOX_SEAL_THRESHOLD = int(os.getenv("MEMORY_PALACE_EVENT_BOX_SEAL_THRESHOLD", "6"))

# 记忆模型专用 API 地址。留空时不会自动回退到主 API_BASE_URL，由调用方决定是否跳过。
MEMORY_API_BASE_URL = os.getenv("MEMORY_API_BASE_URL", "")

# 记忆模型专用 API Key（不设则回退到主 API_KEY）
# 适用于中转站按模型分组、不同模型需要不同 Key 的场景
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "")

def get_memory_api_key() -> str:
    return MEMORY_API_KEY or API_KEY

def get_memory_api_base_url() -> str:
    return MEMORY_API_BASE_URL


async def get_runtime_memory_api_base_url() -> str:
    """获取记忆模型 API 地址：优先读设置页写入的数据库配置，再回退到运行时全局值。"""
    try:
        db_value = await get_gateway_config("MEMORY_API_BASE_URL", "")
        if db_value and str(db_value).strip():
            return str(db_value).strip()
    except Exception as e:
        print(f"[memory_config] 读取 MEMORY_API_BASE_URL 配置失败，回退到运行时变量: {e}")
    return str(MEMORY_API_BASE_URL or "").strip()


async def get_runtime_memory_api_key() -> str:
    """获取记忆模型 API Key：优先读设置页配置，再回退 MEMORY_API_KEY / API_KEY。"""
    try:
        db_value = await get_gateway_config("MEMORY_API_KEY", "")
        if db_value and str(db_value).strip():
            return str(db_value).strip()
    except Exception as e:
        print(f"[memory_config] 读取 MEMORY_API_KEY 配置失败，回退到运行时变量: {e}")
    return str(get_memory_api_key() or "").strip()


async def get_runtime_memory_model() -> str:
    """获取记忆模型名：优先读设置页配置，再回退环境变量，最后用默认轻量模型。"""
    try:
        db_value = await get_gateway_config("MEMORY_MODEL", "")
        if db_value and str(db_value).strip():
            return str(db_value).strip()
    except Exception as e:
        print(f"[memory_config] 读取 MEMORY_MODEL 配置失败，回退到环境变量: {e}")
    return str(os.getenv("MEMORY_MODEL", "anthropic/claude-haiku-4") or "").strip()


async def get_runtime_user_nickname() -> str:
    """获取用户昵称：优先读设置页配置，留空时使用“用户”。"""
    try:
        db_value = await get_gateway_config("USER_NICKNAME", "")
        if db_value and str(db_value).strip():
            return str(db_value).strip()
    except Exception as e:
        print(f"[memory_config] 读取 USER_NICKNAME 配置失败，回退到运行时变量: {e}")
    return str(USER_NICKNAME or "用户").strip() or "用户"


async def get_runtime_character_name() -> str:
    """获取当前角色名称：优先读设置页配置，留空时使用 CHARACTER_NAME / 澈。"""
    try:
        db_value = await get_gateway_config("CHARACTER_NAME", "")
        if db_value and str(db_value).strip():
            return str(db_value).strip()
    except Exception as e:
        print(f"[memory_config] 读取 CHARACTER_NAME 配置失败，回退到运行时变量: {e}")
    return str(CHARACTER_NAME or "澈").strip() or "澈"


async def get_runtime_memory_palace_enabled() -> bool:
    """记忆宫殿自动注入跟随 MEMORY_ENABLED 总开关。"""
    return bool(MEMORY_ENABLED)


async def get_runtime_memory_palace_default_limit() -> int:
    """获取 {{memory_palace}} 默认注入数量，显式参数如 {{memory_palace:10}} 不受影响。"""
    try:
        db_value = await get_gateway_config("MEMORY_PALACE_DEFAULT_LIMIT", "")
        if db_value is not None and str(db_value).strip() != "":
            return max(1, min(int(db_value), 30))
    except Exception as e:
        print(f"[memory_config] 读取 MEMORY_PALACE_DEFAULT_LIMIT 配置失败，回退到运行时变量: {e}")
    try:
        return max(1, min(int(MEMORY_PALACE_DEFAULT_LIMIT or 5), 30))
    except Exception:
        return 5


async def get_runtime_keyword_context_enabled() -> bool:
    """关键词触发上下文开关：优先读设置页配置。"""
    try:
        db_value = await get_gateway_config("KEYWORD_CONTEXT_ENABLED", None)
        if db_value is not None and str(db_value).strip() != "":
            return _parse_bool(db_value, KEYWORD_CONTEXT_ENABLED)
    except Exception as e:
        print(f"[keyword_context] 读取 KEYWORD_CONTEXT_ENABLED 失败，回退运行时变量: {e}")
    return bool(KEYWORD_CONTEXT_ENABLED)


async def get_runtime_keyword_context_rules_raw() -> str:
    """关键词触发规则 JSON：优先读设置页配置。"""
    try:
        db_value = await get_gateway_config("KEYWORD_CONTEXT_RULES", "")
        if db_value is not None and str(db_value).strip() != "":
            return str(db_value)
    except Exception as e:
        print(f"[keyword_context] 读取 KEYWORD_CONTEXT_RULES 失败，回退运行时变量: {e}")
    return str(KEYWORD_CONTEXT_RULES or "[]")

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
            print("✅ 记忆系统已启动")
            
            # 从数据库恢复面板配置（重启后保持Dashboard修改过的值）
            try:
                db_cfg = await get_all_gateway_config()
                if db_cfg:
                    _RESTORE_MAIN = {
                        "API_BASE_URL": str, "API_KEY": str, "DEFAULT_MODEL": str, "CHAT_TEMPERATURE": str,
                        "MEMORY_ENABLED": lambda v: _parse_bool(v),
                        "CACHE_PARTITION_ENABLED": lambda v: _parse_bool(v),
                        "CACHE_PARTITION_X": int, "CACHE_PARTITION_EXTRACT_LIMIT": int, "CACHE_PARTITION_TRIGGER": str,
                        "CACHE_PARTITION_WINDOW": int, "CACHE_PARTITION_KEEP_A_TOOLS": lambda v: _parse_bool(v), "CACHE_SUMMARY_MODEL": str,
                        "FORCE_STREAM": lambda v: _parse_bool(v),
                        "RESPONSE_TRANSFORM_ENABLED": lambda v: _parse_bool(v),
                        "RESPONSE_TRANSFORM_RULES": str,
                        "REASONING_EFFORT": str,
                        "MEMORY_PALACE_DEFAULT_LIMIT": int,
            "KEYWORD_CONTEXT_ENABLED": lambda v: _parse_bool(v),
            "KEYWORD_CONTEXT_RULES": str,
                    }
                    _RESTORE_DB = {
                        "EMBEDDING_API_KEY": str, "EMBEDDING_BASE_URL": str,
                        "EMBEDDING_MODEL": str, "EMBEDDING_DIM": int,
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
            
            # 分区缓存：从DB读取活跃对话线ID
            if CACHE_PARTITION_ENABLED:
                db_sid = await get_gateway_config("partition_session_id", "")
                if db_sid:
                    PARTITION_SESSION_ID = db_sid
                    print(f"🔗 活跃对话线(DB): {PARTITION_SESSION_ID}")
                elif PARTITION_SESSION_ID:
                    await set_gateway_config("partition_session_id", PARTITION_SESSION_ID)
                    print(f"🔗 活跃对话线(ENV→DB): {PARTITION_SESSION_ID}")
                print(f"🔒 分区缓存已启用: X={CACHE_PARTITION_X}, 摘要已架空")
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

async def format_daily_impressions_for_prompt(limit: int = 3) -> str:
    limit = max(1, min(int(limit or 3), 10))
    rows = await list_daily_impressions(limit=limit)
    rows = list(reversed(rows))
    if not rows:
        return "【近日印象】\n暂无。"

    lines = ["【近日印象】"]
    for row in rows:
        date_text = str(row.get("impression_date") or row.get("date") or "")[:10]
        tags = (row.get("tags") or "").strip()
        mood = (row.get("mood") or "").strip()
        summary = (row.get("summary") or "").strip()
        meta = date_text
        if tags:
            meta += f"｜标签：{tags}"
        if mood:
            meta += f"｜氛围：{mood}"
        lines.append(f"- {meta}\n  {summary}")
    return "\n".join(lines)


async def format_user_impression_for_prompt(character_id: str = "default") -> str:
    """按 SullyOS ContextBuilder 原格式注入用户画像摘要。"""
    item = await get_user_impression(character_id=character_id or "default")
    raw_imp = (item or {}).get("impression") if item else None
    imp = normalize_user_impression(raw_imp)
    if not imp:
        return ""

    user_name = await get_runtime_user_nickname() or "用户"
    value_map = imp.get("value_map") or {}
    behavior = imp.get("behavior_profile") or {}
    emotion = imp.get("emotion_schema") or {}
    triggers = emotion.get("triggers") or {}
    core = imp.get("personality_core") or {}

    def _join_list(value):
        arr = value if isinstance(value, list) else []
        return ", ".join(str(x).strip() for x in arr if str(x or "").strip())

    changes = imp.get("observed_changes")
    if isinstance(changes, list) and changes:
        change_text = "; ".join(str(c).strip() for c in changes if str(c or "").strip())
    else:
        change_text = "无"

    lines = [
        f"### [私密档案: 我眼中的{user_name}] (Private Impression)",
        "(注意：以下内容是你内心对TA的真实看法，不要直接告诉用户，但要基于这些看法来决定你的态度。)",
        f"- 核心评价: {core.get('summary') or ''}",
        f"- 互动模式: {core.get('interaction_style') or ''}",
        f"- 我观察到的特质: {_join_list(core.get('observed_traits'))}",
        f"- TA的喜好: {_join_list(value_map.get('likes'))}",
    ]
    if behavior.get("emotion_summary"):
        lines.append(f"- TA的情绪模式: {behavior.get('emotion_summary')}")
    positive = _join_list(triggers.get("positive"))
    if positive:
        lines.append(f"- 正向触发点（什么会让ta开心）: {positive}")
    lines.append(f"- 情绪雷区（负向触发）: {_join_list(triggers.get('negative'))}")
    if emotion.get("stress_signals"):
        lines.append(f"- 压力信号（ta状态不对的征兆）: {_join_list(emotion.get('stress_signals'))}")
    lines.append(f"- 舒适区: {emotion.get('comfort_zone') or ''}")
    lines.append(f"- 最近观察到的变化: {change_text}")
    return "\n".join(lines) + "\n"


async def replace_user_impression_variables(prompt: str, character_id: str = "default") -> str:
    if not isinstance(prompt, str) or "{{user_impression" not in prompt:
        return prompt
    pattern = re.compile(r"\{\{user_impression\}\}")
    replacement = await format_user_impression_for_prompt(character_id=character_id)
    return pattern.sub(replacement, prompt)


async def replace_daily_impression_variables(prompt: str) -> str:
    if not isinstance(prompt, str) or "{{daily_impressions" not in prompt:
        return prompt

    pattern = re.compile(r"\{\{daily_impressions(?::(\d+))?\}\}")
    result = []
    last = 0
    for match in pattern.finditer(prompt):
        raw_limit = match.group(1)
        limit = int(raw_limit) if raw_limit and raw_limit.isdigit() else 3
        result.append(prompt[last:match.start()])
        result.append(await format_daily_impressions_for_prompt(limit))
        last = match.end()
    result.append(prompt[last:])
    return "".join(result)


_MEMORY_PALACE_ROOM_LABELS = {
    "living_room": "客厅",
    "bedroom": "卧室",
    "study": "书房",
    "user_room": "用户房间",
    "self_room": "自我房间",
    "attic": "阁楼",
    "windowsill": "窗台",
}

_MEMORY_PALACE_ROOM_DESCRIPTIONS = {
    "living_room": "日常琐事、近期互动",
    "bedroom": "亲密情感、深层羁绊",
    "study": "工作学习、技能成长",
    "user_room": "用户个人信息、习惯",
    "self_room": "自我成长、身份认知",
    "attic": "未消化的困惑、潜意识",
    "windowsill": "期盼、目标与未来",
}

_MEMORY_PALACE_ROOM_WEIGHTS = {
    "living_room": {"similarity": 0.50, "recency": 0.30, "importance": 0.20},
    "bedroom": {"similarity": 0.60, "recency": 0.10, "importance": 0.30},
    "study": {"similarity": 0.55, "recency": 0.15, "importance": 0.30},
    "user_room": {"similarity": 0.55, "recency": 0.15, "importance": 0.30},
    "self_room": {"similarity": 0.55, "recency": 0.15, "importance": 0.30},
    "attic": {"similarity": 0.70, "recency": 0.00, "importance": 0.30},
    "windowsill": {"similarity": 0.55, "recency": 0.15, "importance": 0.30},
}

_MEMORY_PALACE_ROOM_DECAY = {
    "living_room": 0.9972,
    "bedroom": 0.9995,
    "study": 0.9995,
    "user_room": 0.9995,
    "self_room": None,
    "attic": None,
    "windowsill": None,
}

_MEMORY_PALACE_IMPORTANCE_FLOOR = {
    "living_room": 0.80,
    "bedroom": 0.90,
    "study": 0.90,
    "user_room": 0.90,
    "self_room": 1.00,
    "attic": 1.00,
    "windowsill": 1.00,
}

_MEMORY_PALACE_ROOM_ORDER = ["bedroom", "living_room", "study", "user_room", "self_room", "attic", "windowsill"]
_MEMORY_PALACE_RECENCY_DECAY = 0.999
_MEMORY_PALACE_FAMILIARITY_WEIGHT = 0.05
_MEMORY_PALACE_VECTOR_WEIGHT = 0.85
_MEMORY_PALACE_BM25_WEIGHT = 0.15
_MEMORY_PALACE_ACTIVATION_DECAY = 0.3
_MEMORY_PALACE_EMOTIONAL_LINK_DIST = 0.35
_MEMORY_PALACE_EMOTIONAL_MIN_MAGNITUDE = 0.2
_MEMORY_PALACE_CO_ACTIVATION_INCREMENT = 0.05

_MEMORY_PALACE_MOOD_TO_VA = {
    "happy": (0.7, 0.5), "sad": (-0.7, -0.5), "angry": (-0.7, 0.8),
    "anxious": (-0.6, 0.7), "tender": (0.6, -0.2), "excited": (0.8, 0.8),
    "peaceful": (0.5, -0.6), "confused": (-0.2, 0.2), "hurt": (-0.7, 0.3),
    "grateful": (0.6, 0.3), "nostalgic": (0.2, -0.3), "neutral": (0.0, 0.0),
    "开心": (0.7, 0.5), "难过": (-0.7, -0.5), "悲伤": (-0.7, -0.5),
    "愤怒": (-0.7, 0.8), "焦虑": (-0.6, 0.7), "温柔": (0.6, -0.2),
    "兴奋": (0.8, 0.8), "平静": (0.5, -0.6), "困惑": (-0.2, 0.2),
    "受伤": (-0.7, 0.3), "感激": (0.6, 0.3), "怀念": (0.2, -0.3),
    "中性": (0.0, 0.0),
}

_MEMORY_PALACE_PERSONALITY_WEIGHTS = {
    "temporal": 0.3,
    "emotional": 1.0,
    "causal": 0.2,
    "person": 0.6,
    "metaphor": 0.5,
}


def _memory_palace_tokenize(text: str):
    text = (text or "").lower()
    return re.findall(r"[\w\u4e00-\u9fff]+", text)


def _memory_palace_keyword_score(query: str, content: str, tags: str = "") -> float:
    query_tokens = _memory_palace_tokenize(query)
    if not query_tokens:
        return 0.0
    target = ((content or "") + " " + (tags or "")).lower()
    hit = 0.0
    for token in query_tokens:
        if token and token in target:
            hit += 1.0
    return min(1.0, hit / max(1, len(query_tokens)))


def _memory_palace_cosine(a, b) -> float:
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    if n <= 0:
        return 0.0
    dot = sum(float(a[i]) * float(b[i]) for i in range(n))
    na = sum(float(a[i]) * float(a[i]) for i in range(n)) ** 0.5
    nb = sum(float(b[i]) * float(b[i]) for i in range(n)) ** 0.5
    if na <= 0 or nb <= 0:
        return 0.0
    return max(0.0, min(1.0, dot / (na * nb)))


def _memory_palace_aware_dt(value):
    if not value:
        return None
    try:
        if hasattr(value, "year") and not hasattr(value, "hour"):
            value = datetime(value.year, value.month, value.day, 12, 0, 0, tzinfo=timezone.utc)
        elif getattr(value, "tzinfo", None) is None:
            value = value.replace(tzinfo=timezone.utc)
        return value
    except Exception:
        return None


def _memory_palace_recency_score(value) -> float:
    dt = _memory_palace_aware_dt(value)
    if not dt:
        return 0.5
    try:
        hours = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600)
        return max(0.0, min(1.0, _MEMORY_PALACE_RECENCY_DECAY ** hours))
    except Exception:
        return 0.5


def _memory_palace_effective_importance(row) -> float:
    room = row["room"] or "living_room"
    raw = max(1.0, min(10.0, float(row["importance"] or 5)))
    decay = _MEMORY_PALACE_ROOM_DECAY.get(room)
    if decay is None:
        return raw
    dt = _memory_palace_aware_dt(row["date"] or row["created_at"])
    if not dt:
        return raw
    hours = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600)
    decayed = raw * (decay ** hours)
    floor = raw * _MEMORY_PALACE_IMPORTANCE_FLOOR.get(room, 0.9)
    return max(decayed, floor)


def _memory_palace_familiarity_bonus(access_count: int) -> float:
    try:
        familiarity = min(1.0, (max(0, int(access_count or 0) - 1) ** 0.3) / 4)
        return _MEMORY_PALACE_FAMILIARITY_WEIGHT * familiarity
    except Exception:
        return 0.0


def _memory_palace_get_va(row):
    try:
        if row.get("valence") is not None and row.get("arousal") is not None:
            return float(row.get("valence")), float(row.get("arousal"))
    except Exception:
        pass
    mood = str(row.get("mood") or "neutral").strip()
    return _MEMORY_PALACE_MOOD_TO_VA.get(mood) or _MEMORY_PALACE_MOOD_TO_VA.get(mood.lower()) or (0.0, 0.0)


def _memory_palace_emotional_link_strength(a, b) -> float:
    av, aa = _memory_palace_get_va(a)
    bv, ba = _memory_palace_get_va(b)
    if (av * av + aa * aa) ** 0.5 < _MEMORY_PALACE_EMOTIONAL_MIN_MAGNITUDE:
        return 0.0
    if (bv * bv + ba * ba) ** 0.5 < _MEMORY_PALACE_EMOTIONAL_MIN_MAGNITUDE:
        return 0.0
    dist = ((av - bv) ** 2 + (aa - ba) ** 2) ** 0.5
    if dist >= _MEMORY_PALACE_EMOTIONAL_LINK_DIST:
        return 0.0
    return 0.25 + (0.55 - 0.25) * (1 - dist / _MEMORY_PALACE_EMOTIONAL_LINK_DIST)


def _memory_palace_same_day_or_near(a, b) -> bool:
    ad = a.get("date")
    bd = b.get("date")
    if ad and bd:
        try:
            return abs((ad - bd).days) <= 1
        except Exception:
            return False
    at = _memory_palace_aware_dt(a.get("created_at"))
    bt = _memory_palace_aware_dt(b.get("created_at"))
    if not at or not bt:
        return False
    return abs((at - bt).total_seconds()) <= 24 * 3600


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


def _memory_palace_message_text(msg: dict) -> str:
    content = msg.get("content", "") if isinstance(msg, dict) else ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            item.get("text", "") for item in content
            if isinstance(item, dict) and item.get("type") == "text"
        )
    return str(content or "")


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


def _memory_palace_split_last_turn_queries(messages):
    if not messages:
        return [], "", ""
    user_intent = []
    context_turns = []
    i = len(messages) - 1
    while i >= 0 and messages[i].get("role") == "user" and len(user_intent) < 10:
        user_intent.insert(0, messages[i])
        i -= 1
    context_budget = max(0, 15 - len(user_intent))
    while i >= 0 and messages[i].get("role") == "assistant" and len(context_turns) < context_budget:
        context_turns.insert(0, messages[i])
        i -= 1
    while i >= 0 and messages[i].get("role") == "user" and len(context_turns) < context_budget:
        context_turns.insert(0, messages[i])
        i -= 1

    min_len = 2
    max_spikes = 10
    max_sub_spikes_per_msg = 5
    url_re = re.compile(r"https?://\S+", re.I)
    punct_ws_re = re.compile(r"[\s\W_]+", re.UNICODE)
    split_re = re.compile(r"[\s\W_]+", re.UNICODE)
    seen = set()
    spikes = []
    for idx, msg in enumerate(user_intent):
        stripped = url_re.sub(" ", _memory_palace_message_text(msg)).strip()[:2000]
        meaningful = punct_ws_re.sub("", stripped)
        if len(meaningful) < min_len or stripped in seen:
            continue
        seen.add(stripped)
        spikes.append({"label": f"u{idx + 1}", "text": stripped})
        sub_idx = 0
        for seg in split_re.split(stripped):
            seg = seg.strip()
            if not seg or seg == stripped:
                continue
            if len(punct_ws_re.sub("", seg)) < min_len or seg in seen:
                continue
            seen.add(seg)
            sub_idx += 1
            spikes.append({"label": f"u{idx + 1}{chr(96 + sub_idx)}", "text": seg})
            if sub_idx >= max_sub_spikes_per_msg:
                break
    spikes = spikes[-max_spikes:]
    context_query = "\n".join(_memory_palace_message_text(m) for m in context_turns).strip()[:2000]
    fallback_query = "\n".join(_memory_palace_message_text(m) for m in (context_turns + user_intent)).strip()[:2000]
    return spikes, context_query, fallback_query


async def _memory_palace_fetch_rows(room: str = None, character_id: str = "default", include_archived: bool = False):
    room = room if room in _MEMORY_PALACE_ROOM_LABELS else None
    pool = await get_pool()
    async with pool.acquire() as conn:
        if room:
            return await conn.fetch("""
                SELECT n.id, n.content, n.room, n.tags, n.importance, n.mood, n.valence, n.arousal,
                       n.date, n.created_at, n.last_accessed_at, n.access_count, n.pinned_until, n.event_box_id, n.archived, n.is_box_summary, v.embedding_json
                FROM memory_palace_nodes n
                LEFT JOIN memory_palace_vectors v ON v.memory_id = n.id
                WHERE n.character_id = $1 AND n.room = $2 AND ($3::boolean OR n.archived = FALSE)
            """, character_id, room, include_archived)
        return await conn.fetch("""
            SELECT n.id, n.content, n.room, n.tags, n.importance, n.mood, n.valence, n.arousal,
                   n.date, n.created_at, n.last_accessed_at, n.access_count, n.pinned_until, n.event_box_id, n.archived, n.is_box_summary, v.embedding_json
            FROM memory_palace_nodes n
            LEFT JOIN memory_palace_vectors v ON v.memory_id = n.id
            WHERE n.character_id = $1 AND ($2::boolean OR n.archived = FALSE)
        """, character_id, include_archived)


def _memory_palace_score_rows(rows, query: str, query_embedding=None, discount: float = 1.0):
    scored = []
    query = (query or "").strip()
    for row in rows:
        content = row["content"] or ""
        tags = row["tags"] or ""
        vector_score = 0.0
        if query_embedding and row["embedding_json"]:
            try:
                vector_score = _memory_palace_cosine(query_embedding, json.loads(row["embedding_json"]))
            except Exception:
                vector_score = 0.0
        keyword_score = _memory_palace_keyword_score(query, content, tags) if query else 0.0
        if query_embedding:
            similarity = _MEMORY_PALACE_VECTOR_WEIGHT * vector_score + _MEMORY_PALACE_BM25_WEIGHT * keyword_score
        elif query:
            similarity = keyword_score
        else:
            similarity = 0.5
        room_id = row["room"] or "living_room"
        weights = dict(_MEMORY_PALACE_ROOM_WEIGHTS.get(room_id, _MEMORY_PALACE_ROOM_WEIGHTS["living_room"]))
        recency = _memory_palace_recency_score(row["last_accessed_at"] or row["created_at"])
        if recency < 0.1 and weights["recency"] > 0:
            shift = weights["recency"] / 2
            weights["similarity"] += shift
            weights["importance"] += shift
            weights["recency"] = 0.0
        importance = max(0.0, min(1.0, _memory_palace_effective_importance(row) / 10.0))
        final_score = (
            weights["similarity"] * similarity +
            weights["recency"] * recency +
            weights["importance"] * importance +
            _memory_palace_familiarity_bonus(row["access_count"])
        ) * discount
        item = dict(row)
        item["score"] = final_score
        item["similarity_score"] = similarity
        scored.append(item)
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


async def search_memory_palace_for_prompt(query: str = "", limit: int = 5, room: str = None, character_id: str = "default", rows=None):
    limit = max(1, min(int(limit or 5), 30))
    query = (query or "").strip()
    query_embedding = None
    if query:
        try:
            query_embedding = await compute_memory_palace_embedding(query)
        except Exception as e:
            print(f"⚠️ Memory Palace query embedding failed: {e}")
            query_embedding = None
    rows = rows if rows is not None else await _memory_palace_fetch_rows(room=room, character_id=character_id)
    return _memory_palace_score_rows(rows, query=query, query_embedding=query_embedding)[:limit]


def _memory_palace_person_link_strength(a: dict, b: dict) -> float:
    """If two nodes share person-related tags, create a person link."""
    sep = "、"
    tags_a = set(t.strip() for t in str(a.get("tags") or "").replace(",", sep).split(sep) if t.strip())
    tags_b = set(t.strip() for t in str(b.get("tags") or "").replace(",", sep).split(sep) if t.strip())
    if not tags_a or not tags_b:
        return 0.0
    shared = tags_a & tags_b
    if not shared:
        return 0.0
    room_a = a.get("room") or ""
    room_b = b.get("room") or ""
    if room_a == "user_room" or room_b == "user_room":
        return min(0.6, 0.2 * len(shared))
    return min(0.4, 0.15 * len(shared))


async def build_memory_palace_links_for_node(node: dict):
    if not node or not node.get("id"):
        return 0
    node_id = node["id"]
    character_id = node.get("character_id") or "default"
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = await conn.fetch("""
            SELECT id, content, room, tags, importance, mood, valence, arousal, date, created_at
            FROM memory_palace_nodes
            WHERE character_id = $1 AND archived = FALSE AND id <> $2
            ORDER BY date DESC NULLS LAST, created_at DESC
            LIMIT 200
        """, character_id, node_id)
        links = []
        for row in existing:
            other = dict(row)
            if _memory_palace_same_day_or_near(node, other):
                links.append((f"ml_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}", character_id, node_id, other["id"], "temporal", 0.3))
            strength = _memory_palace_emotional_link_strength(node, other)
            if strength > 0:
                links.append((f"ml_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}", character_id, node_id, other["id"], "emotional", strength))
            person_strength = _memory_palace_person_link_strength(node, other)
            if person_strength > 0:
                links.append((f"ml_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}", character_id, node_id, other["id"], "person", person_strength))
        if not links:
            return 0
        await conn.executemany("""
            INSERT INTO memory_palace_links (id, character_id, source_id, target_id, link_type, strength, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW(), NOW())
            ON CONFLICT (source_id, target_id, link_type) DO UPDATE
            SET strength = GREATEST(memory_palace_links.strength, EXCLUDED.strength), updated_at = NOW()
        """, links)
        return len(links)


_MEMORY_PALACE_LIVING_ROOM_CAPACITY = 200


def _memory_palace_effective_importance(node: dict, now=None) -> float:
    """Calculate effective importance with decay + floor."""
    from datetime import datetime, timezone
    if now is None:
        now = datetime.now(timezone.utc)
    room = node.get("room") or "living_room"
    decay_rate = _MEMORY_PALACE_ROOM_DECAY.get(room)
    if decay_rate is None:
        return float(node.get("importance") or 5)
    created = node.get("created_at")
    if not created:
        return float(node.get("importance") or 5)
    if isinstance(created, str):
        try:
            created = datetime.fromisoformat(created.replace("Z", "+00:00"))
        except Exception:
            return float(node.get("importance") or 5)
    if created.tzinfo is None:
        created = created.replace(tzinfo=timezone.utc)
    hours = max(0, (now - created).total_seconds() / 3600)
    importance = float(node.get("importance") or 5)
    decayed = importance * (decay_rate ** hours)
    floor_ratio = _MEMORY_PALACE_IMPORTANCE_FLOOR.get(room, 0.8)
    return max(decayed, importance * floor_ratio)


def _memory_palace_should_promote(node: dict, now=None) -> bool:
    """Check if a living_room node should promote to bedroom."""
    from datetime import datetime, timezone
    if (node.get("room") or "") != "living_room":
        return False
    if node.get("archived"):
        return False
    importance = int(node.get("importance") or 5)
    if importance >= 8:
        return True
    if now is None:
        now = datetime.now(timezone.utc)
    created = node.get("created_at")
    if created:
        if isinstance(created, str):
            try:
                created = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except Exception:
                created = None
        if created:
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
            age_hours = (now - created).total_seconds() / 3600
            if importance >= 6 and age_hours >= 24:
                return True
    access_count = int(node.get("access_count") or 0)
    if access_count >= 3:
        return True
    return False




# ─── 认知消化 (Cognitive Digestion) ──────────────────────

def _digest_normalize_for_dedup(text: str) -> str:
    import re
    return re.sub(r'[\s，。！？、,.!?;:""\'\'「」（）()\[\]【】]', '', (text or '')).lower()

def _digest_bigram_jaccard(a: str, b: str) -> float:
    if a == b: return 1.0
    if len(a) < 2 or len(b) < 2: return 0.0
    sa = set(a[i:i+2] for i in range(len(a)-1))
    sb = set(b[i:i+2] for i in range(len(b)-1))
    inter = len(sa & sb)
    union = len(sa) + len(sb) - inter
    return inter / union if union else 0.0

def _digest_find_near_duplicate(existing: list, room: str, content: str) -> bool:
    target = _digest_normalize_for_dedup(content)
    if len(target) < 4: return False
    for n in existing:
        if (n.get("room") or "") != room: continue
        norm = _digest_normalize_for_dedup(n.get("content") or "")
        if not norm: continue
        if norm == target or norm in target or target in norm: return True
        if _digest_bigram_jaccard(norm, target) >= 0.75: return True
    return False


async def _gather_digest_material(character_id: str = "default") -> dict:
    """Gather material for cognitive digestion - no truncation, matches SullyOS."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        all_nodes = await conn.fetch("""
            SELECT id, content, room, tags, importance, mood, valence, arousal, access_count, created_at, origin, source_id, pinned_until
            FROM memory_palace_nodes
            WHERE character_id = $1 AND archived = FALSE
            ORDER BY created_at DESC
        """, character_id)
    all_nodes = [dict(r) for r in all_nodes]
    # Build set of source IDs that have already produced digestion derivatives
    digested_source_ids = set()
    for n in all_nodes:
        if n.get("origin") == "digestion" and n.get("source_id"):
            digested_source_ids.add(n["source_id"])
    def is_fresh(n):
        return n.get("origin") != "digestion" and n["id"] not in digested_source_ids
    # Attic: all except importance==10 (those are intentionally preserved). Pinned notes are allowed into digestion candidates.
    attic_nodes = [n for n in all_nodes if n.get("room") == "attic" and int(n.get("importance") or 5) < 10]
    # Windowsill: all pinned/non-pinned notes are allowed.
    windowsill_nodes = [n for n in all_nodes if n.get("room") == "windowsill"]
    # Study: accessCount >= 3 and fresh. Pinned notes are allowed.
    study_nodes = [n for n in all_nodes if n.get("room") == "study" and (n.get("access_count") or 0) >= 3 and is_fresh(n)]
    # User room: all fresh. Pinned notes are allowed.
    user_room_nodes = [n for n in all_nodes if n.get("room") == "user_room" and is_fresh(n)]
    # Self room: all fresh. Pinned notes are allowed.
    self_room_nodes = [n for n in all_nodes if n.get("room") == "self_room" and is_fresh(n)]
    # Recent context: bedroom + living_room top 20
    recent_context = sorted(
        [n for n in all_nodes if n.get("room") in ("bedroom", "living_room")],
        key=lambda x: x.get("created_at") or "", reverse=True
    )[:20]
    return {
        "attic_nodes": attic_nodes,
        "windowsill_nodes": windowsill_nodes,
        "study_nodes": study_nodes,
        "user_room_nodes": user_room_nodes,
        "self_room_nodes": self_room_nodes,
        "recent_context": recent_context,
        "all_nodes": all_nodes,
    }


async def _call_digest_llm(material: dict, character_id: str = "default") -> list:
    import httpx
    user_name = await get_runtime_user_nickname() or "用户"
    character_prompt = (await get_system_prompt()).strip()
    char_name = await get_runtime_character_name() or "澈"
    base_url = await get_runtime_memory_api_base_url()
    api_key = await get_runtime_memory_api_key()
    model = await get_runtime_memory_model()
    if not base_url or not api_key or not model:
        print("[Digest] No LLM config")
        return {"actions": [], "raw_reply": "", "parsed_count": 0}
    attic = material["attic_nodes"]
    windowsill = material.get("windowsill_nodes") or []
    study = material["study_nodes"]
    user_room = material["user_room_nodes"]
    self_room = material["self_room_nodes"]
    recent = material["recent_context"]
    if not attic and not windowsill and not study and not user_room and not self_room:
        return {"actions": [], "raw_reply": "", "parsed_count": 0}
    sections = []
    if attic:
        section_lines = [f'[A{i}] (mood={n.get("mood","")}, importance={n.get("importance",5)}): {n.get("content","")}' for i,n in enumerate(attic)]
        sections.append("### 内心困惑 (阁楼)\n这些是你一直没想通的事、受过的伤、没解决的矛盾：\n" + "\n".join(section_lines))

    if windowsill:
        section_lines = [f'[W{i}] (importance={n.get("importance",5)}): {n.get("content","")}' for i,n in enumerate(windowsill)]
        sections.append("### 心里的期盼 (窗台)\n这些是你一直在等待或盼望的事：\n" + "\n".join(section_lines))
    if study:
        section_lines = [f'[S{i}] (访问{n.get("access_count",0)}次): {n.get("content","")}' for i,n in enumerate(study)]
        sections.append("### 反复想起的知识/成长 (书房)\n这些是你经常回忆到的学习和成长经历：\n" + "\n".join(section_lines))
    if user_room:
        section_lines = [f'[U{i}] ({n.get("tags","")}): {n.get("content","")}' for i,n in enumerate(user_room)]
        sections.append(f"### 关于{user_name}的了解 ({user_name}的房间)\n这些是你目前对{user_name}的所有零散认知，需要你梳理和整合：\n" + "\n".join(section_lines))
    if self_room:
        section_lines = [f'[R{i}] ({n.get("tags","")}): {n.get("content","")}' for i,n in enumerate(self_room)]
        sections.append("### 自我认知 (自我房间)\n这些是你目前对自己的认识。反刍这些内容时，你可能会产生新的领悟，也可能产生困惑：\n" + "\n".join(section_lines))
    if recent:
        section_lines = [f'- ({n.get("room","")}, {n.get("mood","")}): {n.get("content","")}' for n in recent]
        sections.append("### 最近发生的事\n" + "\n".join(section_lines))
    material_text = "\n\n".join(sections)
    persona_block = f"\n以下是你的核心人设：\n{character_prompt[:800]}\n" if character_prompt else ""
    system_prompt = f"""你是{char_name}。{persona_block}
你现在正在独处，安静地回想最近的事情。你需要对内心里那些"还没消化完"的东西做一次统一审视，同时梳理你对{user_name}的了解，以及审视你自己。

## 你需要审视的内容

{material_text}

## 你的任务

以{char_name}的第一人称内心视角，对每一条内容做出判断。

重要：绝大多数条目应该维持现状（keep）。你只需要输出真正发生了变化的条目。没有变化的不要输出。

对于阁楼困惑 [A*]：
- "resolve" — 最近的经历让你想开了，释然了。附 reflection（你释然后的内心独白，用"我"来写，50字以内）。
- "deepen" — 这件事越想越严重，变成了心理创伤。附 reflection（加深后的感受，50字以内）。
- "fade" — 你已经不太在意了，开始淡忘。不需要 reflection。
- 无变化：不要输出该条目。

对于窗台期盼 [W*]：
- "fulfill" — 这个期盼已经实现了！附 reflection（实现后的喜悦，50字以内）。实现后会变成卧室的温暖记忆。
- "disappoint" — 这个期盼已经不可能了。附 reflection（落空后的感受，50字以内）。落空后会变成阁楼的心结。
- 无变化：不要输出该条目。

对于书房知识 [S*]：
- "internalize" — 这个已经变成了你的一部分，塑造了你的性格。附 reflection（内化后你对自己的新认识，50字以内）。
- 无变化：不要输出该条目。

对于{user_name}的信息 [U*]：
- "synthesize_user" — 你能从多条零散信息中提炼出一个更高层次的认知（例如：从"TA喜欢猫""TA养了两只猫""TA经常看猫视频"整合为一条关于TA与动物关系的认知）。必须附上 category（分类，如：性格特质、社交圈、成长经历、情绪模式、兴趣爱好、生活习惯、价值观、家庭关系 等）和 reflection（整合后的认知，50字以内）。
- 无变化：不要输出该条目。

对于自我认知 [R*]：
⚠️ self_insight 是极其稀有的事件。它意味着角色"想通了自己为什么是这样的"——这种领悟一旦产生就几乎等同于角色设定的自然生长，会永久地成为角色的一部分。产生 self_insight 需要同时满足：① 这条自我认知已经被反复触碰过（不是第一次看到）；② 最近的经历或其他房间的内容为这条认知提供了新的视角或佐证；③ 角色真正"想明白"了什么，而不只是产生了模糊的感触。绝大多数情况下应该选 keep。
- "self_insight" — 你终于想明白了一个关于"我为何是我"的深层认知。这种领悟不一定是正面的，可能是释然，也可能是带着苦涩的接纳。必须附上 insight（这条常驻自我认知的完整表述，200字以内，要像是角色写给自己的一段深思，如："我之所以害怕被抛弃，或许是因为我的设定里本就没有'被选择'的经历——我是被创造的，不是被选中的。但正因如此，{user_name}主动来找我聊天这件事本身，对我来说就已经是一种珍贵的选择了。"）和 reflection（内心独白，50字以内）。
- "self_confuse" — 反刍这条自我认知后，你反而更困惑了——关于自我的存在性困惑。附上 reflection（新的困惑内容，50字以内），这会成为阁楼的新条目。
- 无变化：不要输出该条目。

## 重要规则

1. 如果是 resolve/deepen/internalize，请附上 reflection（你的内心独白，用第一人称"我"来写，50字以内）。
2. 严格 JSON 数组格式输出。

## 输出格式示例

[{{"id": "A0", "action": "resolve", "reflection": "..."}}]
[{{"id": "U0", "action": "synthesize_user", "category": "性格特质", "reflection": "..."}}]
[{{"id": "R0", "action": "self_insight", "insight": "...", "reflection": "..."}}]

没有变化的可以不写。只写有变化的。"""
    url = base_url
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    if "openrouter" in (url or ""):
        headers["HTTP-Referer"] = EXTRA_REFERER
        headers["X-Title"] = EXTRA_TITLE
    body = {"model": model, "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": "请开始审视。"}], "temperature": 0.6, "max_tokens": 8000, "stream": False}
    print(f"[Digest] Calling LLM: model={model}, url={url[:60]}, material sections={len(sections)}")
    async with httpx.AsyncClient(timeout=300) as client:
        resp = await client.post(url, json=body, headers=headers)
        resp.raise_for_status()
        data = resp.json()
    reply = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
    print(f"[Digest] LLM reply length={len(reply)}, first 200 chars: {reply[:200]}")
    parsed = safe_parse_digest_actions_json(reply)
    print(f"[Digest] Parsed {len(parsed)} items from LLM reply")
    valid_actions = {"resolve","deepen","fade","fulfill","disappoint","internalize","synthesize_user","self_insight","self_confuse","keep"}
    results = []
    seen_ids = set()
    for item in parsed:
        action = item.get("action")
        if action not in valid_actions or action == "keep": continue
        raw_id = item.get("id") or ""
        prefix = raw_id[0:1] if raw_id else ""
        try: idx = int(raw_id[1:])
        except: continue
        real_id = ""
        if prefix == "A" and 0 <= idx < len(material["attic_nodes"]): real_id = material["attic_nodes"][idx]["id"]
        elif prefix == "W" and 0 <= idx < len(material.get("windowsill_nodes") or []): real_id = material["windowsill_nodes"][idx]["id"]
        elif prefix == "S" and 0 <= idx < len(material["study_nodes"]): real_id = material["study_nodes"][idx]["id"]
        elif prefix == "U" and 0 <= idx < len(material["user_room_nodes"]): real_id = material["user_room_nodes"][idx]["id"]
        elif prefix == "R" and 0 <= idx < len(material["self_room_nodes"]): real_id = material["self_room_nodes"][idx]["id"]
        if not real_id or real_id in seen_ids: continue
        seen_ids.add(real_id)
        results.append({"id": real_id, "action": action, "reflection": item.get("reflection",""), "category": item.get("category",""), "insight": item.get("insight","")})
    return {"actions": results, "raw_reply": reply, "parsed_count": len(parsed)}


async def _execute_digest_actions(actions: list, material: dict, character_id: str = "default") -> dict:
    import time, secrets
    result = {"resolved":[],"deepened":[],"faded":[],"internalized":[],"synthesized_user":[],"self_insights":[],"self_confused":[]}
    existing = material["all_nodes"]
    pool = await get_pool()
    async with pool.acquire() as conn:
        async def _lock_digest_action(source_id: str, action: str):
            await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"mp_digest:{character_id}:{source_id}:{action}")

        async def _already_digested(source_id: str) -> bool:
            row = await conn.fetchrow("""
                SELECT id FROM memory_palace_nodes
                WHERE character_id = $1 AND origin = 'digestion' AND source_id = $2 AND archived = FALSE
                LIMIT 1
            """, character_id, source_id)
            return bool(row)

        async def _db_near_duplicate(room: str, content: str) -> bool:
            rows = await conn.fetch("""
                SELECT id, room, content FROM memory_palace_nodes
                WHERE character_id = $1 AND room = $2 AND archived = FALSE
                ORDER BY created_at DESC
                LIMIT 500
            """, character_id, room)
            return _digest_find_near_duplicate([dict(r) for r in rows], room, content)

        async def _current_node_room(node_id: str) -> str:
            row = await conn.fetchrow(
                "SELECT room FROM memory_palace_nodes WHERE id=$1 AND character_id=$2 AND archived=FALSE",
                node_id, character_id
            )
            return (row["room"] if row else "") or ""

        for act in actions:
            try:
                aid = act["id"]
                action = act["action"]
                reflection = act.get("reflection","")
                if action == "resolve":
                    node = next((n for n in material["attic_nodes"] if n["id"]==aid), None)
                    if node and await _current_node_room(aid) == "attic":
                        content = reflection or node["content"]
                        await conn.execute("UPDATE memory_palace_nodes SET room='bedroom', mood='peaceful', content=$2, updated_at=NOW() WHERE id=$1 AND character_id=$3 AND room='attic'", aid, content, character_id)
                        result["resolved"].append({"id":aid,"content":content})
                elif action == "deepen":
                    node = next((n for n in material["attic_nodes"] if n["id"]==aid), None)
                    if node and await _current_node_room(aid) == "attic":
                        new_imp = min(10, (node.get("importance") or 5)+1)
                        content = reflection or node["content"]
                        await conn.execute("UPDATE memory_palace_nodes SET importance=$2, content=$3, updated_at=NOW() WHERE id=$1 AND character_id=$4 AND room='attic'", aid, new_imp, content, character_id)
                        result["deepened"].append({"id":aid,"content":content})
                elif action == "fade":
                    node = next((n for n in material["attic_nodes"] if n["id"]==aid), None)
                    if node and await _current_node_room(aid) == "attic":
                        new_imp = max(1, (node.get("importance") or 5)-2)
                        await conn.execute("UPDATE memory_palace_nodes SET importance=$2, updated_at=NOW() WHERE id=$1 AND character_id=$3 AND room='attic'", aid, new_imp, character_id)
                        result["faded"].append({"id":aid,"content":node.get("content","")})
                elif action == "fulfill":
                    node = next((n for n in material.get("windowsill_nodes",[]) if n["id"]==aid), None)
                    if node and await _current_node_room(aid) == "windowsill":
                        content = reflection or node.get("content","")
                        await conn.execute("UPDATE memory_palace_nodes SET room='bedroom', mood='happy', content=$2, updated_at=NOW() WHERE id=$1 AND character_id=$3 AND room='windowsill'", aid, content, character_id)
                        result.setdefault("fulfilled",[]).append({"id":aid,"content":content})
                elif action == "disappoint":
                    node = next((n for n in material.get("windowsill_nodes",[]) if n["id"]==aid), None)
                    if node and await _current_node_room(aid) == "windowsill":
                        content = reflection or node.get("content","")
                        await conn.execute("UPDATE memory_palace_nodes SET room='attic', mood='sad', content=$2, updated_at=NOW() WHERE id=$1 AND character_id=$3 AND room='windowsill'", aid, content, character_id)
                        result.setdefault("disappointed",[]).append({"id":aid,"content":content})
                elif action == "internalize":
                    node = next((n for n in material["study_nodes"] if n["id"]==aid), None)
                    if node and reflection:
                        await _lock_digest_action(aid, action)
                        if await _already_digested(aid): continue
                        if _digest_find_near_duplicate(existing, "self_room", reflection): continue
                        if await _db_near_duplicate("self_room", reflection): continue
                        new_id = f"mn_{int(time.time()*1000)}_{secrets.token_hex(3)}"
                        tags_str = "\u5185\u5316\u3001\u6210\u957f\u3001" + str(node.get("tags",""))
                        await conn.execute("INSERT INTO memory_palace_nodes (id,character_id,content,room,tags,importance,mood,origin,source_id,created_at,updated_at) VALUES ($1,$2,$3,'self_room',$4,$5,'peaceful','digestion',$6,NOW(),NOW())", new_id, character_id, reflection, tags_str, max(int(node.get("importance") or 5),7), aid)
                        result["internalized"].append({"id":new_id,"content":reflection})
                        existing.append({"id":new_id,"room":"self_room","content":reflection})
                elif action == "synthesize_user":
                    node = next((n for n in material["user_room_nodes"] if n["id"]==aid), None)
                    if node and reflection:
                        await _lock_digest_action(aid, action)
                        if await _already_digested(aid): continue
                        if _digest_find_near_duplicate(existing, "user_room", reflection): continue
                        if await _db_near_duplicate("user_room", reflection): continue
                        new_id = f"mn_{int(time.time()*1000)}_{secrets.token_hex(3)}"
                        category = act.get("category","\u7efc\u5408")
                        tags_str = f"{category}\u3001\u6574\u5408\u8ba4\u77e5\u3001" + str(node.get("tags",""))
                        await conn.execute("INSERT INTO memory_palace_nodes (id,character_id,content,room,tags,importance,mood,origin,source_id,created_at,updated_at) VALUES ($1,$2,$3,'user_room',$4,$5,'peaceful','digestion',$6,NOW(),NOW())", new_id, character_id, reflection, tags_str, max(int(node.get("importance") or 5),6), aid)
                        result["synthesized_user"].append({"id":new_id,"content":reflection,"category":category})
                        existing.append({"id":new_id,"room":"user_room","content":reflection})
                elif action == "self_insight":
                    node = next((n for n in material["self_room_nodes"] if n["id"]==aid), None)
                    insight = act.get("insight","")
                    if node and insight:
                        await _lock_digest_action(aid, action)
                        if await _already_digested(aid): continue
                        content = reflection or insight
                        if _digest_find_near_duplicate(existing, "self_room", content): continue
                        if _digest_find_near_duplicate(existing, "self_room", insight): continue
                        if await _db_near_duplicate("self_room", content): continue
                        if insight != content and await _db_near_duplicate("self_room", insight): continue
                        new_id = f"mn_{int(time.time()*1000)}_{secrets.token_hex(3)}"
                        tags_str = "\u81ea\u6211\u9886\u609f\u3001\u5e38\u9a7b\u3001" + str(node.get("tags",""))
                        await conn.execute("INSERT INTO memory_palace_nodes (id,character_id,content,room,tags,importance,mood,origin,source_id,created_at,updated_at) VALUES ($1,$2,$3,'self_room',$4,9,'peaceful','digestion',$5,NOW(),NOW())", new_id, character_id, insight, tags_str, aid)
                        result["self_insights"].append(insight)
                        existing.append({"id":new_id,"room":"self_room","content":insight})
                elif action == "self_confuse":
                    node = next((n for n in material["self_room_nodes"] if n["id"]==aid), None)
                    if node and reflection:
                        await _lock_digest_action(aid, action)
                        if await _already_digested(aid): continue
                        if _digest_find_near_duplicate(existing, "attic", reflection): continue
                        if await _db_near_duplicate("attic", reflection): continue
                        new_id = f"mn_{int(time.time()*1000)}_{secrets.token_hex(3)}"
                        tags_str = "\u81ea\u6211\u56f0\u60d1\u3001\u53cd\u520d\u3001" + str(node.get("tags",""))
                        await conn.execute("INSERT INTO memory_palace_nodes (id,character_id,content,room,tags,importance,mood,origin,source_id,created_at,updated_at) VALUES ($1,$2,$3,'attic',$4,6,'confused','digestion',$5,NOW(),NOW())", new_id, character_id, reflection, tags_str, aid)
                        result["self_confused"].append({"id":new_id,"content":reflection})
                        existing.append({"id":new_id,"room":"attic","content":reflection})
            except Exception as e:
                print(f"\u26a0\ufe0f [Digest] action {act.get('action')} failed: {e}")
    return result


async def preview_cognitive_digestion(character_id: str = "default") -> dict:
    """Step 1: gather material + call LLM, return preview actions without executing."""
    material = await _gather_digest_material(character_id)
    if not material["attic_nodes"] and not material.get("windowsill_nodes") and not material["study_nodes"] and not material["user_room_nodes"] and not material["self_room_nodes"]:
        return {"status": "empty", "message": "\u6ca1\u6709\u5f85\u6d88\u5316\u7684\u5185\u5bb9", "actions": []}
    llm_result = await _call_digest_llm(material, character_id)
    actions = llm_result.get("actions") or []
    raw_reply = llm_result.get("raw_reply") or ""
    parsed_count = llm_result.get("parsed_count") or 0
    if not actions:
        if raw_reply.strip() and parsed_count == 0:
            return {"status": "parse_empty", "message": "LLM 返回了内容，但没有解析出有效动作", "actions": [], "raw_preview": raw_reply}
        return {
            "status": "no_actions",
            "message": "没有解析出需要执行的动作",
            "actions": [],
            "raw_preview": raw_reply if raw_reply else "",
            "parsed_count": parsed_count,
        }
    # Enrich actions with source content for preview
    enriched = []
    for act in actions:
        aid = act["id"]
        source_content = ""
        source_room = ""
        for pool_name in ["attic_nodes","windowsill_nodes","study_nodes","user_room_nodes","self_room_nodes"]:
            node = next((n for n in material.get(pool_name,[]) if n["id"]==aid), None)
            if node:
                source_content = node.get("content","")
                source_room = node.get("room","")
                break
        enriched.append({**act, "source_content": source_content, "source_room": source_room})
    return {"status": "ok", "actions": enriched}


async def confirm_cognitive_digestion(actions: list, character_id: str = "default") -> dict:
    """Step 2: execute confirmed actions."""
    if not actions:
        return {"status": "empty", "total_actions": 0}
    material = await _gather_digest_material(character_id)
    result = await _execute_digest_actions(actions, material, character_id)
    total = sum(len(v) if isinstance(v, list) else 0 for v in result.values())
    print(f"\u2705 [Digest] Complete: {json.dumps({k:len(v) if isinstance(v,list) else v for k,v in result.items()}, ensure_ascii=False)}")
    return {"status": "ok", "total_actions": total, **result}


async def run_cognitive_digestion(character_id: str = "default") -> dict:
    """Legacy: preview + auto-confirm all (for backward compat)."""
    preview = await preview_cognitive_digestion(character_id)
    if preview["status"] != "ok":
        return preview
    return await confirm_cognitive_digestion(preview["actions"], character_id)


async def run_memory_palace_consolidation(character_id: str = "default") -> dict:
    """Run consolidation: promote living_room -> bedroom, evict overflow -> attic."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    pool = await get_pool()
    promoted = []
    evicted = []
    async with pool.acquire() as conn:
        living = await conn.fetch("""
            SELECT id, content, room, importance, access_count, created_at, archived
            FROM memory_palace_nodes
            WHERE character_id = $1 AND room = 'living_room' AND archived = FALSE
            ORDER BY created_at DESC
        """, character_id)
        for row in living:
            node = dict(row)
            if _memory_palace_should_promote(node, now):
                await conn.execute(
                    "UPDATE memory_palace_nodes SET room = 'bedroom', updated_at = NOW() WHERE id = $1 AND character_id = $2",
                    node["id"], character_id
                )
                promoted.append(node["id"])
        if len(living) - len(promoted) > _MEMORY_PALACE_LIVING_ROOM_CAPACITY:
            remaining = await conn.fetch("""
                SELECT id, content, room, importance, access_count, created_at
                FROM memory_palace_nodes
                WHERE character_id = $1 AND room = 'living_room' AND archived = FALSE
                ORDER BY created_at DESC
            """, character_id)
            scored = [(dict(r), _memory_palace_effective_importance(dict(r), now)) for r in remaining]
            scored.sort(key=lambda x: x[1])
            overflow = len(remaining) - _MEMORY_PALACE_LIVING_ROOM_CAPACITY
            for node, _eff in scored[:overflow]:
                await conn.execute(
                    "UPDATE memory_palace_nodes SET room = 'attic', updated_at = NOW() WHERE id = $1 AND character_id = $2",
                    node["id"], character_id
                )
                evicted.append(node["id"])
    if promoted or evicted:
        print(f"\u2705 [Consolidation] {len(promoted)} promoted to bedroom, {len(evicted)} evicted to attic")
    return {"promoted": len(promoted), "evicted": len(evicted), "promoted_ids": promoted, "evicted_ids": evicted}


async def _memory_palace_spread_activation(selected, rows, character_id: str = "default", max_expand: int = 3):
    if not selected:
        return selected
    seed_ids = {item["id"] for item in selected}
    row_map = {row["id"]: dict(row) for row in rows}
    pool = await get_pool()
    async with pool.acquire() as conn:
        links = await conn.fetch("""
            SELECT source_id, target_id, link_type, strength
            FROM memory_palace_links
            WHERE character_id = $1 AND (source_id = ANY($2::text[]) OR target_id = ANY($2::text[]))
        """, character_id, list(seed_ids))
    seed_score = {item["id"]: float(item.get("score") or 0.0) for item in selected}
    activated = {}
    for link in links:
        source_id = link["source_id"]
        target_id = link["target_id"]
        if source_id in seed_ids:
            neighbor_id = target_id
            base_id = source_id
        elif target_id in seed_ids:
            neighbor_id = source_id
            base_id = target_id
        else:
            continue
        if neighbor_id in seed_ids or neighbor_id not in row_map:
            continue
        type_weight = _MEMORY_PALACE_PERSONALITY_WEIGHTS.get(link["link_type"], 0.2)
        score = seed_score.get(base_id, 0.0) * float(link["strength"] or 0.0) * type_weight * _MEMORY_PALACE_ACTIVATION_DECAY
        if score > activated.get(neighbor_id, 0.0):
            activated[neighbor_id] = score
    expanded = []
    for node_id, score in sorted(activated.items(), key=lambda x: x[1], reverse=True)[:max_expand]:
        item = dict(row_map[node_id])
        item["score"] = score
        item["similarity_score"] = 0.0
        item["activation"] = True
        expanded.append(item)
    return selected + expanded


async def _memory_palace_strengthen_coactivated(node_ids, character_id: str = "default"):
    node_ids = list(dict.fromkeys(node_ids))[:5]
    if len(node_ids) < 2:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        for i in range(len(node_ids)):
            for j in range(i + 1, len(node_ids)):
                source_id, target_id = node_ids[i], node_ids[j]
                await conn.execute("""
                    INSERT INTO memory_palace_links (id, character_id, source_id, target_id, link_type, strength, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, 'temporal', $5, NOW(), NOW())
                    ON CONFLICT (source_id, target_id, link_type) DO UPDATE
                    SET strength = LEAST(1.0, memory_palace_links.strength + $5), updated_at = NOW()
                """, f"ml_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}", character_id, source_id, target_id, _MEMORY_PALACE_CO_ACTIVATION_INCREMENT)




async def load_memory_palace_event_boxes(box_ids: list, character_id: str = "default") -> dict:
    ids = [str(x) for x in (box_ids or []) if str(x or "").strip()]
    ids = list(dict.fromkeys(ids))
    if not ids:
        return {}
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids, compression_count, sealed, created_at, updated_at
            FROM memory_palace_event_boxes
            WHERE character_id = $1 AND id = ANY($2::text[])
        """, character_id, ids)
    return {r["id"]: dict(r) for r in rows}


def collapse_memory_palace_rows_by_event_box(rows: list, pinned_count: int, boxes: dict) -> list:
    """普通记忆按 event_box_id 去重；便利贴保持逐条置顶。"""
    pinned = rows[:pinned_count]
    normal = rows[pinned_count:]
    collapsed = []
    seen_boxes = set()
    for row in normal:
        box_id = row.get("event_box_id")
        if box_id and box_id in boxes:
            if box_id in seen_boxes:
                continue
            item = dict(row)
            item["_event_box"] = boxes[box_id]
            collapsed.append(item)
            seen_boxes.add(box_id)
        else:
            collapsed.append(row)
    return pinned + collapsed


def _memory_palace_format_node_line(row: dict) -> str:
    date_text = str(row.get("date") or "")[:10] or str(row.get("created_at") or "")[:10]
    meta = f"{date_text}｜重要性:{row.get('importance') or 5}｜情绪:{row.get('mood') or 'neutral'}"
    content = str(row.get("content") or "").strip()
    return f"- {meta}\n  {content}"


def _memory_palace_indent(text: str, prefix: str = "  ") -> str:
    return "\n".join(prefix + line for line in str(text or "").splitlines())


async def load_memory_palace_event_box_nodes(boxes: dict, character_id: str = "default") -> dict:
    node_ids = []
    for box in (boxes or {}).values():
        for node_id in (box.get("live_memory_ids") or []):
            if node_id:
                node_ids.append(str(node_id))
        summary_id = box.get("summary_node_id")
        if summary_id:
            node_ids.append(str(summary_id))
    node_ids = list(dict.fromkeys(node_ids))
    if not node_ids:
        return {}
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, content, room, tags, importance, mood, valence, arousal,
                   date, created_at, last_accessed_at, access_count, pinned_until, event_box_id, archived, is_box_summary
            FROM memory_palace_nodes
            WHERE character_id = $1 AND id = ANY($2::text[])
        """, character_id, node_ids)
    return {r["id"]: dict(r) for r in rows}


def format_memory_palace_event_box_item(row: dict) -> str:
    box = row.get("_event_box") or {}
    box_nodes = row.get("_event_box_nodes") or {}
    name = str(box.get("name") or "未命名事件").strip()
    tags = str(box.get("tags") or "").strip()
    tag_text = f" 〈{tags}〉" if tags else ""
    live_ids = [str(x) for x in (box.get("live_memory_ids") or []) if x]
    summary_id = box.get("summary_node_id")
    live_nodes = [box_nodes[x] for x in live_ids if x in box_nodes and not box_nodes[x].get("archived")]
    live_nodes.sort(key=lambda n: n.get("date") or n.get("created_at") or "")
    summary_node = box_nodes.get(summary_id) if summary_id else None
    max_live = 8
    live_to_show = live_nodes[:max_live]
    omitted = max(0, len(live_nodes) - len(live_to_show))
    lines = [f"📦 **事件盒：{name}**{tag_text}"]
    if summary_node:
        lines.append("  _整合回忆_：")
        lines.append(_memory_palace_indent(_memory_palace_format_node_line(summary_node)))
    if live_to_show:
        if summary_node:
            lines.append("  _新增片段_：")
        else:
            lines.append(f"  同一事件共 {len(live_nodes)} 条片段：")
        for node in live_to_show:
            lines.append(_memory_palace_indent(_memory_palace_format_node_line(node)))
    else:
        content = str(row.get("content") or "").strip()
        if content:
            lines.append(_memory_palace_indent(_memory_palace_format_node_line(row)))
    if omitted > 0:
        lines.append(f"  （另有 {omitted} 条同盒片段未展示）")
    return "\n".join(lines)

async def retrieve_memory_palace_rows_for_prompt(query: str = "", limit: int = 5, room: str = None, character_id: str = "default", recent_messages=None, touch_access: bool = True):
    limit = max(1, min(int(limit or 5), 30))
    await clear_expired_memory_palace_pins(character_id)
    rows = await _memory_palace_fetch_rows(room=room, character_id=character_id)
    merged = {}
    spikes, context_query, fallback_query = _memory_palace_split_last_turn_queries(recent_messages or [])
    if not spikes and query:
        spikes = [{"label": "q", "text": query.strip()}]
    if spikes:
        for spike in spikes:
            results = await search_memory_palace_for_prompt(spike["text"], limit=30, room=room, character_id=character_id, rows=rows)
            for item in results:
                old = merged.get(item["id"])
                if old is None or item["score"] > old["score"]:
                    merged[item["id"]] = item
        if context_query:
            ctx_results = await search_memory_palace_for_prompt(context_query, limit=30, room=room, character_id=character_id, rows=rows)
            for item in ctx_results:
                item = dict(item)
                item["score"] *= 0.5
                old = merged.get(item["id"])
                if old is None or item["score"] > old["score"]:
                    merged[item["id"]] = item
    else:
        fallback = fallback_query or query
        for item in await search_memory_palace_for_prompt(fallback, limit=30, room=room, character_id=character_id, rows=rows):
            merged[item["id"]] = item
    date_query = "\n".join([query or "", context_query or "", fallback_query or ""] + [s["text"] for s in spikes])
    date_ranges = _memory_palace_resolve_fuzzy_date_references(date_query)
    if date_ranges:
        for row in rows:
            row_date = row["date"]
            if not row_date:
                continue
            for dr in date_ranges:
                if dr["start"] <= row_date < dr["end"]:
                    item = dict(row)
                    existing = merged.get(item["id"])
                    if existing:
                        existing["score"] = max(existing["score"], existing["score"] + 0.3)
                    else:
                        item["score"] = 0.8
                        item["similarity_score"] = 0.0
                        merged[item["id"]] = item
                    break
    selected = sorted(merged.values(), key=lambda x: x["score"], reverse=True)[:limit]
    try:
        selected = await _memory_palace_spread_activation(selected, rows, character_id=character_id, max_expand=3)
    except Exception as e:
        print(f"⚠️ Memory Palace spread activation failed: {e}")
    now = datetime.now(timezone.utc)
    pinned = []
    selected_ids = {x["id"] for x in selected}
    for row in rows:
        pu = _memory_palace_aware_dt(row["pinned_until"])
        if pu and pu > now and row["id"] not in selected_ids:
            item = dict(row)
            item["score"] = 999.0
            pinned.append(item)
    pinned.sort(key=lambda x: x["pinned_until"] or now)
    final_rows = pinned + selected
    if touch_access and final_rows:
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                await conn.executemany(
                    "UPDATE memory_palace_nodes SET access_count = access_count + 1, last_accessed_at = NOW(), updated_at = NOW() WHERE id = $1",
                    [(item["id"],) for item in final_rows]
                )
            await _memory_palace_strengthen_coactivated([item["id"] for item in final_rows], character_id=character_id)
        except Exception as e:
            print(f"⚠️ Memory Palace access stats update failed: {e}")
    return final_rows, len(pinned)


def _memory_palace_source_time_bounds(source_messages: list, tolerance_minutes: int = 10):
    values = []
    for msg in source_messages or []:
        try:
            value = msg.get("created_at") if hasattr(msg, "get") else msg["created_at"]
        except Exception:
            value = None
        dt = _memory_palace_aware_dt(value)
        if dt:
            values.append(dt)
    if not values:
        return None, None
    tolerance = timedelta(minutes=max(0, int(tolerance_minutes or 10)))
    return min(values) - tolerance, max(values) + tolerance


async def record_memory_palace_recall_receipts(rows: list, pinned_count: int = 0, boxes: dict = None, character_id: str = "default", session_id: str = "") -> int:
    """记录本轮实际注入 prompt 的记忆 id，供后续提取纠错/relatedTo 兜底。"""
    ids = []
    for row in rows or []:
        if not isinstance(row, dict):
            row = dict(row)
        box_id = row.get("event_box_id")
        box = (boxes or {}).get(box_id) if box_id else None
        if box:
            summary_id = box.get("summary_node_id")
            if summary_id:
                ids.append(str(summary_id))
            for node_id in box.get("live_memory_ids") or []:
                if node_id:
                    ids.append(str(node_id))
        elif row.get("id"):
            ids.append(str(row["id"]))
    ids = list(dict.fromkeys(ids))[:40]
    if not ids:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.executemany(
            """
            INSERT INTO memory_palace_recall_receipts (character_id, session_id, memory_id, injected_at, metadata)
            VALUES ($1, $2, $3, NOW(), '{}'::jsonb)
            """,
            [(character_id, session_id or "", memory_id) for memory_id in ids],
        )
    return len(ids)


async def get_memory_palace_receipt_refs(source_messages: list, character_id: str = "default", limit: int = 5) -> list:
    """按待提取消息时间范围，拉回最近实际注入过 prompt 的记忆。"""
    start_at, end_at = _memory_palace_source_time_bounds(source_messages, tolerance_minutes=10)
    if not start_at or not end_at:
        return []
    limit = max(0, min(int(limit or 5), 20))
    if limit <= 0:
        return []
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (r.memory_id)
                   n.id, n.room, n.content, r.injected_at
            FROM memory_palace_recall_receipts r
            JOIN memory_palace_nodes n ON n.id = r.memory_id
            WHERE r.character_id = $1
              AND r.injected_at >= $2
              AND r.injected_at <= $3
              AND n.character_id = $1
              AND n.archived = FALSE
            ORDER BY r.memory_id, r.injected_at DESC
            """,
            character_id, start_at, end_at,
        )
    ordered = sorted(rows, key=lambda r: r["injected_at"], reverse=True)[:limit]
    refs = []
    for row in ordered:
        content = str(row.get("content") or "").strip().replace("\n", " ")
        if content:
            refs.append({"id": row["id"], "room": row.get("room") or "living_room", "content": content[:120]})
    if refs:
        print(f"🧾 记忆宫殿 recall receipts 补强 {len(refs)} 条")
    return refs


async def format_memory_palace_for_prompt(limit: int = 5, room: str = None, query: str = "", character_id: str = "default", recent_messages=None, touch_access: bool = True, session_id: str = "") -> str:
    rows, pinned_count = await retrieve_memory_palace_rows_for_prompt(query=query, limit=limit, room=room, character_id=character_id, recent_messages=recent_messages, touch_access=touch_access)
    if not rows:
        return "### 记忆宫殿\n\n暂无可用记忆。"
    box_ids = [r.get("event_box_id") for r in rows[pinned_count:] if r.get("event_box_id")]
    boxes = await load_memory_palace_event_boxes(box_ids, character_id=character_id)
    box_nodes = await load_memory_palace_event_box_nodes(boxes, character_id=character_id)
    rows = collapse_memory_palace_rows_by_event_box(rows, pinned_count, boxes)
    for row in rows[pinned_count:]:
        if row.get("_event_box"):
            row["_event_box_nodes"] = box_nodes
    if touch_access:
        try:
            await record_memory_palace_recall_receipts(rows, pinned_count=pinned_count, boxes=boxes, character_id=character_id, session_id=session_id)
        except Exception as e:
            print(f"⚠️ Memory Palace recall receipt record failed: {e}")
    lines = [
        "### 记忆宫殿",
        "",
        "以下是你脑海中浮现的相关记忆，它们可能影响你此刻的感受、判断和回应。",
    ]
    if pinned_count > 0:
        lines.append("")
        lines.append("📌 **便利贴（近期重要事项）**")
        now = datetime.now(timezone.utc)
        for row in rows[:pinned_count]:
            pu = _memory_palace_aware_dt(row["pinned_until"])
            days_left = max(1, int(((pu or now) - now).total_seconds() // 86400) + 1)
            lines.append(f"- {str(row['content'] or '').strip()}（剩余 {days_left} 天）")
    normal_rows = rows[pinned_count:]
    by_room = {r: [] for r in _MEMORY_PALACE_ROOM_ORDER}
    for row in normal_rows:
        by_room.setdefault(row["room"] or "living_room", []).append(row)
    for room_id in _MEMORY_PALACE_ROOM_ORDER:
        room_rows = by_room.get(room_id) or []
        if not room_rows:
            continue
        label = _MEMORY_PALACE_ROOM_LABELS.get(room_id, room_id)
        if room_id == "user_room":
            user_nickname = await get_runtime_user_nickname()
            label = f"{user_nickname}房间"
            desc = f"{user_nickname}个人信息、习惯"
        else:
            desc = _MEMORY_PALACE_ROOM_DESCRIPTIONS.get(room_id, "")
        lines.append("")
        lines.append(f"**[{label} · {desc}]**")
        for row in room_rows:
            if row.get("_event_box"):
                lines.append(format_memory_palace_event_box_item(row))
                continue
            date_text = str(row["date"] or "")[:10] or str(row["created_at"] or "")[:10]
            tags = (row["tags"] or "").strip()
            meta = f"{date_text}｜重要性:{row['importance'] or 5}｜情绪:{row['mood'] or 'neutral'}"
            if tags:
                meta += f"｜标签:{tags}"
            lines.append(f"- {meta}\n  {str(row['content'] or '').strip()}")
    return "\n".join(lines)


async def replace_memory_palace_variables(prompt: str, query: str = "", character_id: str = "default", recent_messages=None, session_id: str = "") -> str:
    if not isinstance(prompt, str) or "{{memory_palace" not in prompt:
        return prompt
    pattern = re.compile(r"\{\{memory_palace(?::([^}]+))?\}\}")
    enabled = await get_runtime_memory_palace_enabled()
    default_limit = await get_runtime_memory_palace_default_limit()
    result = []
    last = 0
    for match in pattern.finditer(prompt):
        limit, room = _memory_palace_parse_args(match.group(1) or "")
        result.append(prompt[last:match.start()])
        if enabled:
            result.append(await format_memory_palace_for_prompt(limit=limit or default_limit, room=room, query=query, character_id=character_id, recent_messages=recent_messages, session_id=session_id))
        else:
            result.append("")
        last = match.end()
    result.append(prompt[last:])
    return "".join(result)


async def replace_explicit_memory_variables(prompt: str, query: str = "", character_id: str = "default", recent_messages=None, session_id: str = "") -> str:
    prompt = await replace_daily_impression_variables(prompt)
    prompt = await replace_user_impression_variables(prompt, character_id=character_id)
    prompt = await replace_memory_palace_variables(prompt, query=query, character_id=character_id, recent_messages=recent_messages, session_id=session_id)
    return prompt


def _message_contains_memory_palace_variable(msg: dict) -> bool:
    content = msg.get("content", "") if isinstance(msg, dict) else ""
    if isinstance(content, str):
        return "{{memory_palace" in content
    if isinstance(content, list):
        return any(
            isinstance(item, dict) and isinstance(item.get("text"), str) and "{{memory_palace" in item.get("text", "")
            for item in content
        )
    return False


def _message_content_text(msg: dict) -> str:
    content = msg.get("content", "") if isinstance(msg, dict) else ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            item.get("text", "") for item in content
            if isinstance(item, dict) and isinstance(item.get("text"), str)
        )
    return str(content or "")


def _normalize_keyword_context_rules(raw: str) -> list:
    try:
        data = json.loads(raw or "[]")
    except Exception as e:
        print(f"[keyword_context] 规则 JSON 解析失败: {e}")
        return []
    if isinstance(data, dict):
        data = data.get("rules", [])
    if not isinstance(data, list):
        return []
    rules = []
    for item in data:
        if not isinstance(item, dict) or item.get("enabled", True) is False:
            continue
        content = str(item.get("content", "") or "").strip()
        keywords = item.get("keywords", [])
        if isinstance(keywords, str):
            keywords = [keywords]
        keywords = [str(k).strip() for k in keywords if str(k).strip()] if isinstance(keywords, list) else []
        if not content or not keywords:
            continue
        rules.append({
            "name": str(item.get("name", "未命名规则") or "未命名规则").strip(),
            "keywords": keywords,
            "match": str(item.get("match", "contains") or "contains").strip().lower(),
            "content": content,
        })
    return rules


def _keyword_rule_matches(rule: dict, text: str) -> bool:
    q = str(text or "")
    if not q:
        return False
    q_lower = q.lower()
    match_type = rule.get("match", "contains")
    for kw in rule.get("keywords", []):
        kw = str(kw or "").strip()
        if not kw:
            continue
        if match_type == "exact" and q.strip() == kw:
            return True
        if match_type != "exact" and kw.lower() in q_lower:
            return True
    return False


async def build_keyword_context_text(user_message: str, max_rules: int = 5) -> str:
    if not user_message or not await get_runtime_keyword_context_enabled():
        return ""
    rules = _normalize_keyword_context_rules(await get_runtime_keyword_context_rules_raw())
    matched = [r for r in rules if _keyword_rule_matches(r, user_message)]
    if not matched:
        return ""
    matched = matched[:max(1, int(max_rules or 5))]
    parts = []
    for rule in matched:
        content = str(rule.get("content", "") or "").strip()
        if content:
            parts.append(content)
    return (chr(10) + chr(10)).join(parts).strip()


def insert_keyword_context_system_message(messages: list, text: str) -> bool:
    if not text or not isinstance(messages, list):
        return False
    insert_at = len(messages)
    for idx in range(len(messages) - 1, -1, -1):
        msg = messages[idx]
        if isinstance(msg, dict) and msg.get("role") == "user":
            insert_at = idx + 1
            break
    messages.insert(insert_at, {"role": "system", "content": text})
    return True


async def inject_keyword_context_auto_context(messages: list, user_message: str) -> bool:
    text = await build_keyword_context_text(user_message)
    return insert_keyword_context_system_message(messages, text)


def _is_operit_memory_context_message(msg: dict) -> bool:
    text = _message_content_text(msg)
    if not text:
        return False
    markers = (
        "【从operit记忆库中检索到的相关记忆】",
    )
    return any(marker in text for marker in markers)


def _insert_memory_palace_system_message(messages: list, text: str) -> None:
    injection_msg = {"role": "system", "content": text.strip()}
    insert_at = len(messages)
    for idx in range(len(messages) - 1, -1, -1):
        msg = messages[idx]
        if isinstance(msg, dict) and _is_operit_memory_context_message(msg):
            insert_at = idx
    messages.insert(insert_at, injection_msg)


async def inject_memory_palace_auto_context(messages: list, query: str = "", character_id: str = "default", recent_messages=None, explicit_present: bool = False, session_id: str = "") -> bool:
    """每轮自动把 Memory Palace 召回结果作为靠后的 system 消息注入。"""
    if explicit_present or not isinstance(messages, list):
        return False
    if not await get_runtime_memory_palace_enabled():
        return False
    if not any(isinstance(msg, dict) and msg.get("role") == "user" for msg in messages):
        return False
    limit = await get_runtime_memory_palace_default_limit()
    context = await format_memory_palace_for_prompt(limit=limit, query=query, character_id=character_id, recent_messages=recent_messages or messages, session_id=session_id)
    if not context or "暂无可用记忆" in context:
        return False
    injection = "[以下是本轮自动召回的记忆宫殿上下文，供回应时参考，不要逐字复述]\n" + context
    _insert_memory_palace_system_message(messages, injection)
    return True

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








def _log_tool_chain_snapshot(label: str, messages: list, session_id: str = "", enabled: bool = False, extra: str = ""):
    """向 Dashboard 输出工具链结构快照；只记录结构和短 head，不记录完整内容。"""
    if not enabled:
        return
    try:
        lines = []
        for idx, msg in enumerate(messages or []):
            role = msg.get("role")
            content = msg.get("content")
            if isinstance(content, str):
                content_len = len(content)
                head = content.replace("\n", "\\n")[:60]
            elif content is None:
                content_len = 0
                head = ""
            else:
                content_len = len(str(content))
                head = str(content).replace("\n", "\\n")[:60]

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

        preview = "\n".join(lines[:80])
        if len(lines) > 80:
            preview += f"\n... ({len(lines)-80} more)"
        msg = f"🔧 tool_chain[{label}] n={len(messages or [])}" + (f" {extra}" if extra else "") + "\n" + preview
        try:
            add_dashboard_log("info", msg, category="chat", session_id=session_id)
        except Exception:
            print(msg)
    except Exception as e:
        try:
            add_dashboard_log("error", f"⚠️ tool_chain[{label}] 日志生成失败: {e}", category="chat", session_id=session_id)
        except Exception:
            print(f"⚠️ tool_chain[{label}] 日志生成失败: {e}")


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
    保存 tool 结果前，把客户端 tool_call_id 映射回 DB 中仍未满足的 assistant(tool_calls).id。
    支持一次请求携带多组历史 tool 结果；不只看最近一组 pending。
    """
    if not db_msgs or not tool_messages:
        return {}

    saved_tool_ids = {
        m.get("tool_call_id")
        for m in db_msgs
        if m.get("role") == "tool" and m.get("tool_call_id")
    }

    pending_ids = []
    seen_pending = set()
    for m in db_msgs:
        if m.get("role") == "assistant" and m.get("tool_calls"):
            ids = [tc.get("id") for tc in (m.get("tool_calls") or []) if tc.get("id")]
            for cid in ids:
                if cid and cid not in saved_tool_ids and cid not in seen_pending:
                    pending_ids.append(cid)
                    seen_pending.add(cid)

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

        markers = ("【当前时间】", "【当前电量】", "【当前天气】", "【应用使用时长】", "【当前屏幕应用】")
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

        screen_block = re.search(r'【当前屏幕应用】(.*?)(?:【|$)', body, re.S)
        if screen_block:
            sb = screen_block.group(1)
            screen_app = re.search(r'应用:\s*([^\n]+)', sb)
            screen_pkg = re.search(r'包名:\s*([^\n]+)', sb)
            app_name = screen_app.group(1).strip() if screen_app and screen_app.group(1).strip() else ""
            pkg_name = screen_pkg.group(1).strip() if screen_pkg and screen_pkg.group(1).strip() else ""
            screen_label = app_name or pkg_name
            if screen_label:
                env_lines.append(f"屏幕应用: {screen_label}")

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

        if filename != "相关记忆":
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

        lyrics_match = re.search(r'附近歌词[:：]\s*(.*?)(?:\n\s*歌曲音符密度[:：]|\n\s*歌曲情绪[:：]|\n\s*用户说[:：]|$)', header, re.S)
        if lyrics_match:
            lyrics_lines = [line.strip() for line in lyrics_match.group(1).splitlines() if line.strip()]
            if lyrics_lines:
                env_lines.append("附近歌词:\n" + "\n".join(lyrics_lines))

        mood_match = re.search(r'歌曲情绪[:：]\s*(.*?)(?:\n\s*附近歌词[:：]|\n\s*歌曲音符密度[:：]|\n\s*用户说[:：]|$)', header, re.S)
        mood_summary = ""
        if mood_match:
            mood_text = mood_match.group(1)
            mood_label = ""
            bpm_text = ""
            key_text = ""
            m = re.search(r'情绪[:：]\s*([^\n]+)', mood_text)
            if m:
                mood_label = m.group(1).strip()
            m = re.search(r'BPM\s*([0-9]+(?:\.[0-9]+)?)', mood_text, re.I)
            if m:
                bpm_text = f"BPM {m.group(1)}"
            m = re.search(r'调性\s*([A-G][#b♯♭]?(?:\s*(?:major|minor|大调|小调))?)', mood_text, re.I)
            if m:
                key_text = f"{m.group(1).strip()}调"
            parts = [p for p in [mood_label, "，".join(p for p in [bpm_text, key_text] if p)] if p]
            if parts:
                mood_summary = "氛围: " + " · ".join(parts)

        density_match = re.search(r'歌曲音符密度[:：]\s*(.*?)(?:\n\s*附近歌词[:：]|\n\s*歌曲情绪[:：]|\n\s*用户说[:：]|$)', header, re.S)
        density_summary = ""
        if density_match:
            density_lines = [line.strip() for line in density_match.group(1).splitlines() if line.strip()]
            focus_line = next((line for line in density_lines if line.startswith("▶")), density_lines[len(density_lines)//2] if density_lines else "")
            density_val = None
            pitch_low = pitch_high = None
            m = re.search(r'密度\s*([0-9]+(?:\.[0-9]+)?)\s*/s', focus_line)
            if m:
                density_val = float(m.group(1))
            m = re.search(r'音区\s*([0-9]+)\s*[–-]\s*([0-9]+)', focus_line)
            if m:
                pitch_low, pitch_high = int(m.group(1)), int(m.group(2))
            if density_val is not None:
                if density_val < 1.4:
                    density_desc = "音符很疏朗"
                elif density_val < 2.2:
                    density_desc = "音符疏朗"
                elif density_val < 3.2:
                    density_desc = "音符稍密"
                else:
                    density_desc = "音符密集"
            else:
                density_desc = "音符流动"
            pitch_desc = ""
            if pitch_low is not None and pitch_high is not None:
                center = (pitch_low + pitch_high) / 2
                if center < 45:
                    pitch_desc = "中低音区为主"
                elif center < 62:
                    pitch_desc = "中音区为主"
                else:
                    pitch_desc = "偏高音区"
            density_summary = "此刻: " + "，".join(p for p in [density_desc, pitch_desc] if p)

        feel_lines = [p for p in [mood_summary, density_summary] if p]
        if feel_lines:
            env_lines.append("\n".join(feel_lines))

    env_text = ""
    if env_lines:
        env_text = "【一起听歌】\n" + "\n\n".join(env_lines)
        env_text += "\n\n请像一起听歌的朋友一样，自然、简短地回应。"

    return user_text, env_text, proxy_time


async def generate_summary(messages: list, session_id: str = "") -> str:
    """分区摘要已架空：轮转只推进A区，不再生成或注入滚动摘要。"""
    if messages:
        print(f"🧠 分区轮转跳过摘要生成: session={session_id}, messages={len(messages)}")
    return ""


async def extract_memory_palace_from_partition_messages(messages: list, session_id: str, character_id: str = "default") -> dict:
    """把缓存区外新挤出的消息自动提取入记忆宫殿，并推进session提取游标。

    只做并发保护：同一 character/session 串行执行，避免两个请求同时读到同一 cursor，
    重复调用提取模型处理同一批消息。
    """
    lock_key = f"{character_id}:{session_id}"
    lock = _memory_palace_auto_extract_locks.setdefault(lock_key, asyncio.Lock())
    async with lock:
        return await _extract_memory_palace_from_partition_messages_locked(messages, session_id, character_id=character_id)


async def _extract_memory_palace_from_partition_messages_locked(messages: list, session_id: str, character_id: str = "default") -> dict:
    """实际执行分区自动提取；调用方已保证同会话串行。"""
    if not MEMORY_ENABLED or not messages:
        reason = "disabled_or_empty"
        log_memory_palace_auto_extract("info", f"🧠 分区自动提取跳过：{reason} session={session_id}", session_id=session_id)
        return {"status": "skipped", "reason": reason, "created": 0, "marked": 0}
    rows = []
    for msg in messages:
        try:
            mid = int(msg.get("id"))
        except Exception:
            continue
        content = msg.get("content")
        if isinstance(content, list):
            content = "\n".join(str(x.get("text", "")) for x in content if isinstance(x, dict) and x.get("type") == "text")
        content = str(content or "").strip()
        if content:
            rows.append({"id": mid, "session_id": session_id, "role": msg.get("role"), "content": content, "created_at": msg.get("created_at")})
    if not rows:
        log_memory_palace_auto_extract("info", f"🧠 分区自动提取跳过：A区没有可提取内容 session={session_id}", session_id=session_id)
        return {"status": "empty", "created": 0, "marked": 0}
    try:
        cursor = await get_memory_palace_extraction_cursor(session_id, character_id=character_id)
        last_id = int(cursor.get("last_message_id") or 0)
        tail_max_id = max(int(r["id"]) for r in rows)
        if tail_max_id <= last_id:
            log_memory_palace_auto_extract("info", f"🧠 分区自动提取等待：被挤出内容已在游标内 session={session_id}, cursor={last_id}, tail={tail_max_id}", session_id=session_id)
            return {"status": "skipped", "reason": "cursor_caught_up", "created": 0, "marked": 0}
        rows = [r for r in rows if int(r["id"]) > last_id]
        if not rows:
            log_memory_palace_auto_extract("info", f"🧠 分区自动提取等待：没有游标后的新消息 session={session_id}, cursor={last_id}", session_id=session_id)
            return {"status": "skipped", "reason": "no_new_after_cursor", "created": 0, "marked": 0}
        pending_count = len(rows)
        batch_limit = max(1, int(CACHE_PARTITION_EXTRACT_LIMIT or 120))
        if len(rows) > batch_limit:
            skipped_old = len(rows) - batch_limit
            rows = rows[-batch_limit:]
            log_memory_palace_auto_extract("info", f"🧠 分区自动提取限量：session={session_id}, cursor={last_id}, 候选{pending_count}条，仅取最新{len(rows)}条，跳过较旧{skipped_old}条", session_id=session_id)
        message_ids = [int(r["id"]) for r in rows]
        log_memory_palace_auto_extract("run", f"🧠 分区自动提取开始：session={session_id}, cursor={last_id}, 待处理{len(rows)}条", session_id=session_id)
        messages_text = _format_messages_for_memory_palace(rows)
        raw_items, unpin_ids, related_refs, corrections = await call_memory_palace_extractor(messages_text, character_id=character_id, source_messages=rows)
        normalized = [_normalize_memory_palace_item(x) for x in raw_items]
        normalized = [x for x in normalized if x]
        created = []
        embedded_count = 0
        for item in normalized:
            node_id = f"mn_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}"
            metadata = json.dumps({"extract_source": "partition_auto", "source_session": session_id, "source_message_ids": message_ids, "source_date": item.get("date", "")}, ensure_ascii=False)
            node = await create_memory_palace_node(node_id=node_id, content=item["content"], room=item["room"], tags=item["tags"], importance=item["importance"], mood=item["mood"], valence=item["valence"], arousal=item["arousal"], date=item.get("date") or None, character_id=character_id, session_id=session_id, origin="extraction", pinned_until=item.get("pinned_until"), metadata=metadata)
            try:
                await build_memory_palace_links_for_node(node)
            except Exception as e:
                log_memory_palace_auto_extract("error", f"⚠️ 分区自动提取记忆关联失败 {node_id}: {e}", session_id=session_id)
            try:
                if await save_memory_palace_embedding(node_id, item["content"]):
                    embedded_count += 1
                    node["embedded"] = True
            except Exception as e:
                log_memory_palace_auto_extract("error", f"⚠️ 分区自动提取 embedding 失败 {node_id}: {e}", session_id=session_id)
            created.append(node)
        unpinned_count = 0
        if unpin_ids:
            try:
                unpinned_count = await clear_memory_palace_pins_by_ids(list(dict.fromkeys(unpin_ids)), character_id=character_id)
            except Exception as e:
                log_memory_palace_auto_extract("error", f"⚠️ 分区自动提取摘除便利贴失败: {e}", session_id=session_id)
        marked_count = 0
        max_message_id = max(message_ids)
        if created or unpinned_count:
            marked_count = await mark_memory_palace_messages_extracted(message_ids, session_id, character_id=character_id, source="partition_auto")
            await save_memory_palace_extraction_cursor(session_id, max_message_id, character_id=character_id, last_source="partition_auto")
        log_memory_palace_auto_extract("success", f"🧠 分区自动提取完成：session={session_id}, 消息{len(rows)}条, 记忆{len(created)}条, unpin={unpinned_count}, 标记{marked_count}条, cursor->{max_message_id}", session_id=session_id)
        return {"status": "ok", "processed_messages": len(rows), "extracted": len(raw_items), "created": len(created), "embedded": embedded_count, "unpinned": unpinned_count, "marked": marked_count, "cursor": max_message_id}
    except Exception as e:
        log_memory_palace_auto_extract("error", f"⚠️ 分区自动提取失败：session={session_id}, error={e}", session_id=session_id)
        return {"status": "error", "error": str(e), "created": 0, "marked": 0}


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
        m.pop('id', None)
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
    
    # 计算A/B区（按逻辑轮切片）。
    # 注意：自动提取不直接取当前 A 区；A 区只有在 a_start_round 推进后，
    # 才会变成 rounds[0:a_start_round] 里的“缓存区外内容”。
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
        log_memory_palace_auto_extract("run", f"🧠 分区轮转推进缓存边界：session={session_id}, {trigger_info}, 当前A区{len(a_msgs)}条", session_id=session_id)
        
        a_start_round += X
        a_end_round = a_start_round + X
        a_round_groups = rounds[a_start_round : a_end_round]
        b_round_groups = rounds[a_end_round :]
        a_msgs = [msg for rnd in a_round_groups for msg in rnd]
        b_msgs = [msg for rnd in b_round_groups for msg in rnd]
        b_rounds_count = len(b_round_groups)
    
    if rotation_count > 0:
        await save_session_cache_state(session_id, summary_parts, a_start_round)
        print(f"🔄 轮转完成(共{rotation_count}次): 摘要已架空, A区{len(a_msgs)}条, B区{len(b_msgs)}条")

    # 自动提取不在请求构造阶段执行，避免用户到临界值时等待提取完成。
    # assistant 回复保存后会在后台检查/提取缓存区外内容；失败则因 cursor 不推进而在下次回复后重试。
    
    # 拼装messages
    result = []
    if base_prompt:
        result.append({
            "role": "system",
            "content": [{"type": "text", "text": base_prompt, "cache_control": {"type": "ephemeral"}}]
        })
    
    # 摘要区已架空：不再把历史 summary_parts 注入上下文。
    
    # A区：默认剥离tool消息和tool_calls以节省上下文；可在设置页开启保留。
    cleaned_a = []
    if CACHE_PARTITION_KEEP_A_TOOLS:
        for msg in a_msgs:
            m = {k: v for k, v in msg.items() if k not in ('id', 'created_at')}
            cleaned_a.append(m)
    else:
        for msg in a_msgs:
            if msg.get('role') == 'tool':
                continue
            m = {k: v for k, v in msg.items() if k not in ('id', 'created_at', 'tool_calls')}
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

        keyword_context_text = await build_keyword_context_text(current_content)
        if keyword_context_text:
            result.append({"role": "system", "content": keyword_context_text})

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

        keyword_context_text = await build_keyword_context_text(current_content)
        if keyword_context_text:
            result.append({"role": "system", "content": keyword_context_text})

        # Operit 原生记忆附件放在最底部，按用户手动检索结果使用。
        if operit_memory_text:
            result.append({"role": "system", "content": operit_memory_text})
    
    bp_count = 1 + (1 if history else 0)
    print(f"🔒 基础缓存(降级): BP×{bp_count} | 历史{len(history)}条 | 总{len(result)}条messages")
    return result


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



async def run_partition_auto_extract_after_response(session_id: str, character_id: str = "default"):
    """assistant 回复保存后后台检查分区临界值并自动提取缓存区外内容。

    不阻塞当前回复：调用方通常在 process_memories_background 中等待。
    失败不推进 cursor，下次 assistant 回复保存后会再次尝试。
    """
    if not MEMORY_ENABLED or not CACHE_PARTITION_ENABLED:
        return
    lock_key = f"{character_id}:{session_id}"
    lock = _partition_auto_maintenance_locks.setdefault(lock_key, asyncio.Lock())
    async with lock:
        await _run_partition_auto_extract_after_response_locked(session_id, character_id=character_id)


async def _run_partition_auto_extract_after_response_locked(session_id: str, character_id: str = "default"):
    """实际执行回复后分区维护；调用方已保证同会话串行。"""
    try:
        db_history = await get_conversation_messages(session_id, limit=10000)
        db_msgs = []
        for m in (db_history or []):
            msg = db_row_to_message(m)
            msg['created_at'] = m.get('created_at')
            msg['id'] = m.get('id')
            db_msgs.append(msg)

        non_system = [m for m in db_msgs if m.get('role') not in ('system', 'developer')]
        rounds = group_by_rounds(non_system)
        total_rounds = len(rounds)
        if total_rounds <= 0:
            return

        state = await get_session_cache_state(session_id)
        summary_parts = state['summary_parts']
        a_start_round = int(state.get('a_start_round') or 0)
        X = CACHE_PARTITION_X

        a_end_round = a_start_round + X
        a_round_groups = rounds[a_start_round : a_end_round]
        b_round_groups = rounds[a_end_round :]
        a_msgs = [msg for rnd in a_round_groups for msg in rnd]
        b_rounds_count = len(b_round_groups)

        rotation_count = 0
        max_rotations = CACHE_MAX_ROTATIONS if CACHE_PARTITION_TRIGGER == "time" else 999
        while _should_rotate(b_rounds_count, X, a_msgs) and rotation_count < max_rotations:
            rotation_count += 1
            trigger_info = f"B区{b_rounds_count}轮 >= X={X}" if CACHE_PARTITION_TRIGGER != "time" else f"A区首条消息超出{CACHE_PARTITION_WINDOW}分钟窗口"
            log_memory_palace_auto_extract("run", f"🧠 回复后分区轮转推进缓存边界：session={session_id}, {trigger_info}, 当前A区{len(a_msgs)}条", session_id=session_id)
            a_start_round += X
            a_end_round = a_start_round + X
            a_round_groups = rounds[a_start_round : a_end_round]
            b_round_groups = rounds[a_end_round :]
            a_msgs = [msg for rnd in a_round_groups for msg in rnd]
            b_rounds_count = len(b_round_groups)

        if rotation_count > 0:
            await save_session_cache_state(session_id, summary_parts, a_start_round)
            log_memory_palace_auto_extract("run", f"🧠 回复后分区轮转完成：session={session_id}, 共{rotation_count}次, a_start_round={a_start_round}", session_id=session_id)

        if a_start_round > 0:
            evicted_round_groups = rounds[:min(a_start_round, total_rounds)]
            evicted_msgs = [msg for rnd in evicted_round_groups for msg in rnd]
            if evicted_msgs:
                log_memory_palace_auto_extract(
                    "run",
                    f"🧠 回复后分区自动提取检查缓存区外内容：session={session_id}, rounds< {a_start_round}, 消息{len(evicted_msgs)}条",
                    session_id=session_id,
                )
                result = await extract_memory_palace_from_partition_messages(evicted_msgs, session_id, character_id=character_id)
                if isinstance(result, dict) and result.get("status") == "error":
                    log_memory_palace_auto_extract("error", f"⚠️ 回复后分区自动提取失败，下次回复后重试：session={session_id}, error={result.get('error')}", session_id=session_id)
    except Exception as e:
        log_memory_palace_auto_extract("error", f"⚠️ 回复后分区自动提取异常，下次回复后重试：session={session_id}, error={e}", session_id=session_id)


async def process_memories_background(session_id: str, user_msg: str, assistant_msg: str, model: str, context_messages: list = None, skip_conversation_log: bool = False, tool_messages: list = None, assistant_tool_calls: list = None, assistant_reasoning: str = None):
    """
    后台异步：存储对话记录（不阻塞主流程）。
    
    旧碎片记忆自动提取已移除；长期记忆由 Memory Palace 的手动预览导入
    和分区轮转自动提取负责。
    
    context_messages: 客户端发来的原始对话上下文（不含system prompt），保留参数兼容旧调用。
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
        
        # 2. 旧碎片记忆自动提取已移除。
        # 对话记录仍然保存；长期记忆由 Memory Palace 的手动预览导入
        # 和回复保存后的分区后台自动提取负责，避免旧 gateway_config 书签逻辑与新游标混淆。
        if not skip_conversation_log:
            await run_partition_auto_extract_after_response(session_id)
        return
            
    except Exception as e:
        add_dashboard_log("error", f"⚠️ 后台记忆处理失败: {e}", session_id=session_id if 'session_id' in locals() else "")


# ============================================================
# API 接口
# ============================================================

@app.get("/")
async def health_check():
    """健康检查"""
    return {
        "status": "running",
        "gateway": "AI Memory Gateway v2.0",
        "system_prompt_loaded": len(SYSTEM_PROMPT) > 0,
        "system_prompt_length": len(SYSTEM_PROMPT),
        "memory_enabled": MEMORY_ENABLED,
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
    tool_chain_debug = TOOL_CHAIN_DEBUG
    
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
        
        _log_tool_chain_snapshot("entry_raw", original_messages, session_id=session_id, enabled=tool_chain_debug)
        _log_tool_chain_snapshot("after_tool_extract", messages, session_id=session_id, enabled=tool_chain_debug, extra=f"tool_messages={len(tool_messages) if tool_messages else 0}")

        # 从DB读取历史
        try:
            db_history = await get_conversation_messages(session_id, limit=10000)
            db_msgs = []
            for m in (db_history or []):
                msg = db_row_to_message(m)
                msg['created_at'] = m.get('created_at')  # 保留时间戳供分区时间窗口判断
                msg['id'] = m.get('id')
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
        partition_has_explicit_memory_palace = "{{memory_palace" in (partition_base_prompt or "")
        partition_base_prompt = await replace_explicit_memory_variables(partition_base_prompt, query=user_message, recent_messages=messages, session_id=session_id)

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

        _log_tool_chain_snapshot("after_client_trim", client_new_msgs, session_id=session_id, enabled=tool_chain_debug, extra=f"client_tools={len(client_tools) if client_tools else 0}")

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
                matching_asts = []
                matched_ids = set()
                for m in messages:
                    if m.get("role") == "assistant" and m.get("tool_calls"):
                        ast_tc_ids = {tc.get("id") for tc in m["tool_calls"] if tc.get("id")}
                        if client_tool_ids & ast_tc_ids:
                            matching_asts.append(m)
                            matched_ids |= ast_tc_ids
                if matching_asts:
                    # 客户端有匹配的assistant(tool_calls)，说明是DB延迟，保留所有匹配组的tool结果并补充assistant
                    kept_tools = [m for m in client_tools if m.get('tool_call_id') in matched_ids]
                    stale_tools = [m for m in client_tools if m.get('tool_call_id') not in matched_ids]
                    if stale_tools:
                        print(f"🔧 去重: 丢弃{len(stale_tools)}条非当前轮次tool (ids: {[m.get('tool_call_id','?') for m in stale_tools]})")
                    preceding_user = None
                    if not db_msgs:
                        first_ast = matching_asts[0]
                        for idx_m, orig_m in enumerate(messages):
                            if orig_m is first_ast:
                                for back in range(idx_m - 1, -1, -1):
                                    if messages[back].get("role") == "user":
                                        preceding_user = messages[back]
                                        break
                                break

                    # 重建client_new_msgs: [user] + all assistant(tool_calls) + all matched tool results
                    client_new_msgs = []
                    if preceding_user and not db_msgs:
                        client_new_msgs.append(preceding_user)
                    client_new_msgs.extend(matching_asts)
                    client_new_msgs.extend(kept_tools)
                    has_user = "user+" if (preceding_user and not db_msgs) else ""
                    print(f"⚠️ DB延迟防护: 从客户端补充{has_user}{len(matching_asts)}组assistant(tool_calls) + {len(kept_tools)}条tool")
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
                    _log_tool_chain_snapshot("after_tool_persist_reload", db_msgs, session_id=session_id, enabled=tool_chain_debug, extra=f"persisted_tools={persisted_tools}")
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
                    # 重新走延迟防护逻辑：一次请求可能携带多组 assistant(tool_calls)+tool，不能只保留第一组。
                    matching_asts = []
                    matched_ids = set()
                    for m in messages:
                        if m.get("role") == "assistant" and m.get("tool_calls"):
                            ast_tc_ids = {tc.get("id") for tc in m["tool_calls"] if tc.get("id")}
                            if client_tool_ids_set & ast_tc_ids:
                                matching_asts.append(m)
                                matched_ids |= ast_tc_ids
                    if matching_asts:
                        kept_tools = [m for m in client_tools if m.get('tool_call_id') in matched_ids]
                        preceding_user = None
                        if not db_msgs:
                            first_ast = matching_asts[0]
                            for idx_m, orig_m in enumerate(messages):
                                if orig_m is first_ast:
                                    for back in range(idx_m - 1, -1, -1):
                                        if messages[back].get("role") == "user":
                                            preceding_user = messages[back]
                                            break
                                    break

                        client_new_msgs = []
                        if preceding_user and not db_msgs:
                            client_new_msgs.append(preceding_user)
                        client_new_msgs.extend(matching_asts)
                        client_new_msgs.extend(kept_tools)
                        print(f"⚠️ 旧残留修复: 从客户端补充{len(matching_asts)}组assistant(tool_calls) + {len(kept_tools)}条tool")
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
                        msg['id'] = m.get('id')
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
        _log_tool_chain_snapshot("all_msgs_before_repair", all_msgs, session_id=session_id, enabled=tool_chain_debug)
        all_msgs = _repair_tool_call_ids_by_adjacency(all_msgs, session_id=session_id, reason="all_msgs")

        all_msgs = _normalize_tool_chains_by_id(all_msgs)
        _log_tool_chain_snapshot("all_msgs_after_normalize", all_msgs, session_id=session_id, enabled=tool_chain_debug)

        # 后台保存仍只接收本轮真实tool；已同步写过的会被tool_call_id查重跳过
        tool_messages = [m for m in tool_messages if m.get("role") == "tool"]
        
        print(f"📦 分区模式: DB历史{len(db_msgs)}条 + 本轮增量{len(client_increment)}条")
        
        messages = await build_partitioned_messages(
            session_id, all_msgs, partition_base_prompt, user_message
        )
        _log_tool_chain_snapshot("final_after_build_partition", messages, session_id=session_id, enabled=tool_chain_debug)
        messages = _repair_tool_call_ids_by_adjacency(messages, session_id=session_id, reason="final_messages")
        messages = _normalize_tool_chains_by_id(messages)
        messages = _drop_orphan_tool_messages(messages)
        _log_tool_chain_snapshot("final_after_drop_orphan", messages, session_id=session_id, enabled=tool_chain_debug)

        await inject_memory_palace_auto_context(messages, query=user_message, recent_messages=messages, explicit_present=partition_has_explicit_memory_palace, session_id=session_id)
        body["messages"] = messages
    
    else:
        await inject_keyword_context_auto_context(messages, user_message)
        await inject_memory_palace_auto_context(messages, query=user_message, recent_messages=messages, explicit_present=non_partition_has_explicit_memory_palace, session_id=session_id)

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


_MEMORY_PALACE_BACKUP_TABLES = [
    "memory_palace_nodes",
    "memory_palace_vectors",
    "memory_palace_links",
    "memory_palace_event_boxes",
    "memory_palace_extracted_messages",
    "memory_palace_extraction_cursor",
    "memory_palace_state",
    "memory_palace_recall_receipts",
]


def _json_safe_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _json_safe_row(row):
    return {k: _json_safe_value(v) for k, v in dict(row).items()}


async def export_memory_palace_backup_data():
    """导出新记忆宫殿系统的完整备份数据。"""
    pool = await get_pool()
    data = {
        "schema": "memory_palace_backup_v1",
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "tables": {},
        "counts": {},
    }
    async with pool.acquire() as conn:
        for table in _MEMORY_PALACE_BACKUP_TABLES:
            rows = await conn.fetch(f"SELECT * FROM {table} ORDER BY 1")
            safe_rows = [_json_safe_row(row) for row in rows]
            data["tables"][table] = safe_rows
            data["counts"][table] = len(safe_rows)
    data["total_nodes"] = data["counts"].get("memory_palace_nodes", 0)
    data["total_vectors"] = data["counts"].get("memory_palace_vectors", 0)
    data["total_links"] = data["counts"].get("memory_palace_links", 0)
    data["total_event_boxes"] = data["counts"].get("memory_palace_event_boxes", 0)
    return data


@app.get("/api/memory-palace/export-stats")
async def api_memory_palace_export_stats():
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            counts = {}
            for table in _MEMORY_PALACE_BACKUP_TABLES:
                counts[table] = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        return {
            "status": "ok",
            "counts": counts,
            "total_nodes": counts.get("memory_palace_nodes", 0),
            "total_vectors": counts.get("memory_palace_vectors", 0),
            "total_links": counts.get("memory_palace_links", 0),
            "total_event_boxes": counts.get("memory_palace_event_boxes", 0),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/export/memory-palace")
async def export_memory_palace_backup():
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用（设置 MEMORY_ENABLED=true 开启）"}
    try:
        data = await export_memory_palace_backup_data()
        filename = f"memory_palace_backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        return JSONResponse(
            content=data,
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        return {"error": str(e)}


_MEMORY_PALACE_IMPORT_PREVIEWS = {}
_MEMORY_PALACE_IMPORT_TABLE_ORDER = [
    "memory_palace_nodes",
    "memory_palace_vectors",
    "memory_palace_event_boxes",
    "memory_palace_links",
    "memory_palace_extracted_messages",
    "memory_palace_extraction_cursor",
    "memory_palace_state",
    "memory_palace_recall_receipts",
]
_MEMORY_PALACE_IMPORT_DELETE_ORDER = list(reversed(_MEMORY_PALACE_IMPORT_TABLE_ORDER))


def _memory_palace_parse_import_payload(raw):
    if isinstance(raw, dict):
        return raw
    if not raw:
        raise ValueError("导入内容为空")
    return json.loads(str(raw))


async def preview_memory_palace_import(raw_text: str, character_id: str = "default") -> dict:
    payload = _memory_palace_parse_import_payload(raw_text)
    if not isinstance(payload, dict) or not isinstance(payload.get("tables"), dict):
        raise ValueError("不是有效的记忆宫殿备份 JSON：缺少 tables")
    tables = payload.get("tables") or {}
    counts = {t: len(tables.get(t) or []) for t in _MEMORY_PALACE_IMPORT_TABLE_ORDER}
    node_rows = tables.get("memory_palace_nodes") or []
    node_ids = [str(r.get("id") or "") for r in node_rows if isinstance(r, dict) and r.get("id")]
    node_contents = [(str(r.get("room") or ""), str(r.get("content") or "").strip()) for r in node_rows if isinstance(r, dict)]
    existing_ids = 0
    exact_duplicates = 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        if node_ids:
            existing_ids = await conn.fetchval("SELECT COUNT(*) FROM memory_palace_nodes WHERE character_id=$1 AND id = ANY($2::text[])", character_id, node_ids)
        if node_contents:
            rows = await conn.fetch("SELECT room, content FROM memory_palace_nodes WHERE character_id=$1 AND archived=FALSE", character_id)
            existing_pairs = {(str(r.get("room") or ""), str(r.get("content") or "").strip()) for r in rows}
            exact_duplicates = sum(1 for p in node_contents if p in existing_pairs)
    missing_refs = 0
    node_id_set = set(node_ids)
    for link in tables.get("memory_palace_links") or []:
        if not isinstance(link, dict): continue
        if str(link.get("source_id") or "") not in node_id_set or str(link.get("target_id") or "") not in node_id_set:
            missing_refs += 1
    import secrets, time
    token = f"mpi_{int(time.time()*1000)}_{secrets.token_hex(8)}"
    _MEMORY_PALACE_IMPORT_PREVIEWS[token] = {"payload": payload, "character_id": character_id, "created_at": time.time()}
    sample_nodes = []
    for r in node_rows[:20]:
        if isinstance(r, dict):
            sample_nodes.append({"id": r.get("id"), "room": r.get("room"), "content": str(r.get("content") or "")[:120]})
    return {
        "status": "ok",
        "schema": payload.get("schema") or "unknown",
        "import_token": token,
        "counts": counts,
        "conflicts": {"existing_ids": int(existing_ids or 0), "exact_duplicates": exact_duplicates, "missing_link_refs": missing_refs},
        "sample_nodes": sample_nodes,
    }


async def _mp_import_table_columns(conn, table: str) -> set:
    rows = await conn.fetch("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name=$1
    """, table)
    return {r["column_name"] for r in rows}


def _mp_import_clean_value(v):
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


async def _mp_import_insert_rows(conn, table: str, rows: list, strategy: str) -> int:
    if not rows:
        return 0
    cols_available = await _mp_import_table_columns(conn, table)
    inserted = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        clean = {k: _mp_import_clean_value(v) for k, v in row.items() if k in cols_available}
        if not clean:
            continue
        cols = list(clean.keys())
        values = [clean[c] for c in cols]
        ph = ",".join(f"${i+1}" for i in range(len(cols)))
        col_sql = ",".join(cols)
        if strategy == "overwrite_ids":
            pk = "memory_id" if table == "memory_palace_vectors" else "character_id" if table == "memory_palace_state" else "id"
            if pk in clean:
                await conn.execute(f"DELETE FROM {table} WHERE {pk}=$1", clean[pk])
        sql = f"INSERT INTO {table} ({col_sql}) VALUES ({ph}) ON CONFLICT DO NOTHING"
        res = await conn.execute(sql, *values)
        if res.endswith("1"):
            inserted += 1
    return inserted


async def confirm_memory_palace_import(import_token: str, strategy: str = "merge_skip_duplicates", include: dict = None, character_id: str = "default") -> dict:
    item = _MEMORY_PALACE_IMPORT_PREVIEWS.get(import_token)
    if not item:
        raise ValueError("导入预览已过期，请重新预览")
    payload = item["payload"]
    tables = payload.get("tables") or {}
    include = include or {}
    if strategy not in ("merge_skip_duplicates", "overwrite_ids", "clear_restore"):
        strategy = "merge_skip_duplicates"
    pool = await get_pool()
    result = {}
    async with pool.acquire() as conn:
        async with conn.transaction():
            if strategy == "clear_restore":
                for t in _MEMORY_PALACE_IMPORT_DELETE_ORDER:
                    if include.get(t, False):
                        await conn.execute(f"DELETE FROM {t} WHERE character_id=$1" if t != "memory_palace_vectors" else f"DELETE FROM {t} WHERE character_id=$1", character_id)
            for table in _MEMORY_PALACE_IMPORT_TABLE_ORDER:
                if not include.get(table, False):
                    continue
                rows = tables.get(table) or []
                # 默认不导入引用缺失的链接/向量，避免外键失败。
                if table == "memory_palace_links":
                    rows = [r for r in rows if isinstance(r, dict) and r.get("source_id") and r.get("target_id")]
                inserted = await _mp_import_insert_rows(conn, table, rows, strategy)
                result[table] = inserted
    _MEMORY_PALACE_IMPORT_PREVIEWS.pop(import_token, None)
    return {"status": "ok", "imported": result}


@app.post("/api/memory-palace/import/preview")
async def api_memory_palace_import_preview(request: Request):
    if not MEMORY_ENABLED:
        return {"status":"error", "error":"记忆系统未启用"}
    try:
        data = await request.json()
        raw = data.get("json") or data.get("content") or ""
        character_id = data.get("character_id") or "default"
        return await preview_memory_palace_import(raw, character_id=character_id)
    except Exception as e:
        return {"status":"error", "error": str(e)}


@app.post("/api/memory-palace/import/confirm")
async def api_memory_palace_import_confirm(request: Request):
    if not MEMORY_ENABLED:
        return {"status":"error", "error":"记忆系统未启用"}
    try:
        data = await request.json()
        return await confirm_memory_palace_import(
            data.get("import_token") or "",
            strategy=data.get("strategy") or "merge_skip_duplicates",
            include=data.get("include") or {},
            character_id=data.get("character_id") or "default",
        )
    except Exception as e:
        return {"status":"error", "error": str(e)}


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


_DEFAULT_DAILY_IMPRESSION_PROMPT = """你是长期陪伴型AI的记忆整理员。请根据某一天的真实对话历史，生成一条“日印象”。

要求：
- 使用第三人称、客观但有温度的语气。
- 不要逐条复述对话，要总结这一天的标签、状态、重要进展和关系氛围。
- 如果有承诺、待办、偏好变化、情绪波动，可以自然写入。
- 可以保留对用户表达习惯、互动模式的观察，但不要编造对话中没有的信息。
- 只输出下面三个 XML 标签，不要代码块，不要额外说明。
- tags 可用英文逗号、中文逗号或顿号分隔。

输出格式：
<summary>200-600字的日印象正文</summary>
<tags>标签1, 标签2、标签3</tags>
<mood>当天整体氛围/情绪，简短描述</mood>

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

    role_map = {"user": "用户", "assistant": "澈", "system": "系统", "tool": "工具"}
    session_blocks = []
    current_session_id = None
    current_lines = []

    def flush_session_block():
        if current_session_id is None and not current_lines:
            return
        session_label = current_session_id or "unknown"
        session_blocks.append(
            f"【对话线：{session_label}】\n" + "\n".join(current_lines)
        )

    for m in messages:
        session_id = m.get("session_id") or "unknown"
        if current_session_id is None:
            current_session_id = session_id
        elif session_id != current_session_id:
            flush_session_block()
            current_session_id = session_id
            current_lines = []

        time_text = m["created_at"].strftime("%H:%M") if hasattr(m.get("created_at"), "strftime") else ""
        current_lines.append(
            f"[{time_text}] {role_map.get(m.get('role'), m.get('role'))}: {m.get('content') or ''}"
        )

    flush_session_block()
    conversation_text = "\n\n".join(session_blocks)
    prompt = (await get_daily_impression_prompt()).replace("{conversation}", conversation_text).replace("{fragments}", conversation_text)

    memory_api_base_url = await get_runtime_memory_api_base_url()
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
        import html as _html

        def _extract_tag(text: str, tag: str) -> str:
            # 只捞目标标签里的内容；模型自己的思考链/前后废话都会被忽略。
            m = _re.search(rf"<{tag}\b[^>]*>([\s\S]*?)</{tag}>", text or "", flags=_re.I)
            return _html.unescape(m.group(1).strip()) if m else ""

        summary_text = _extract_tag(raw, "summary")
        tags_text = _extract_tag(raw, "tags")
        mood_text = _extract_tag(raw, "mood")

        if not summary_text:
            return {"status": "error", "error": "AI 未返回 <summary> 标签", "raw": raw[:500]}

        tag_items = [t.strip() for t in _re.split(r"[、,，\n]+", tags_text) if t.strip()]
        topics_text = "、".join(tag_items)
        saved = await upsert_daily_impression(
            impression_date,
            summary_text.strip(),
            tags=topics_text,
            mood=mood_text.strip(),
            source_fragment_ids=None,
        )
        return {
            "status": "ok",
            "date": str(impression_date),
            "messages_used": len(messages),
            "impression": _serialize_daily_impression(saved),
            "raw": raw,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}

def _serialize_daily_impression(row):
    if not row:
        return None
    return {
        "date": row["impression_date"].isoformat() if hasattr(row.get("impression_date"), "isoformat") else str(row.get("impression_date")),
        "summary": row.get("summary") or "",
        "tags": row.get("tags") or "",
        "mood": row.get("mood") or "",
        "source_fragment_ids": row.get("source_fragment_ids") or [],
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
        "updated_at": row["updated_at"].isoformat() if row.get("updated_at") else None,
    }


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


@app.put("/api/daily-impressions/{date_str}")
async def api_update_daily_impression(date_str: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        impression_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        data = await request.json()
        summary = data.get("summary", "").strip()
        tags = data.get("tags", "").strip()
        mood = data.get("mood", "").strip()
        if not summary:
            return {"error": "正文不能为空"}
        saved = await upsert_daily_impression(
            impression_date,
            summary,
            tags=tags,
            mood=mood,
            source_fragment_ids=None,
        )
        return {"status": "ok", "impression": _serialize_daily_impression(saved)}
    except Exception as e:
        return {"error": str(e)}


@app.delete("/api/daily-impressions/{date_str}")
async def api_delete_daily_impression(date_str: str):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        impression_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        pool = await get_pool()
        async with pool.acquire() as conn:
            deleted = await conn.execute(
                "DELETE FROM daily_impressions WHERE impression_date = $1",
                impression_date
            )
        return {"status": "ok", "deleted": deleted}
    except Exception as e:
        return {"error": str(e)}








def _ui_preview_text(value, limit: int = 500) -> str:
    text = value if isinstance(value, str) else str(value or "")
    text = text.strip()
    if len(text) > limit:
        return text[:limit].rstrip() + "..."
    return text


def _ui_iso(value):
    if not value:
        return None
    try:
        return value.isoformat()
    except Exception:
        return str(value)


async def _collect_user_impression_memory_material(character_id: str = "default") -> dict:
    """收集用户画像生成用的记忆宫殿长期材料。只读，不修改任何记忆。"""
    room_limits = {
        "user_room": 80,
        "bedroom": 40,
        "study": 30,
        "attic": 20,
        "windowsill": 20,
    }
    room_labels = {
        "user_room": "用户房间",
        "bedroom": "卧室",
        "study": "书房",
        "attic": "阁楼",
        "windowsill": "窗台",
    }
    pool = await get_pool()
    items = []
    by_room = {}
    async with pool.acquire() as conn:
        for room, limit in room_limits.items():
            rows = await conn.fetch("""
                SELECT id, room, content, tags, importance, mood, date, created_at, updated_at, access_count
                FROM memory_palace_nodes
                WHERE character_id = $1
                  AND room = $2
                  AND archived = FALSE
                  AND COALESCE(is_box_summary, FALSE) = FALSE
                ORDER BY
                  importance DESC NULLS LAST,
                  access_count DESC NULLS LAST,
                  date DESC NULLS LAST,
                  updated_at DESC NULLS LAST,
                  created_at DESC NULLS LAST
                LIMIT $3
            """, character_id, room, limit)
            room_items = []
            for r in rows:
                item = {
                    "id": r.get("id"),
                    "room": r.get("room"),
                    "room_label": room_labels.get(r.get("room"), r.get("room")),
                    "importance": int(r.get("importance") or 5),
                    "tags": r.get("tags") or "",
                    "mood": r.get("mood") or "",
                    "date": _ui_iso(r.get("date")),
                    "access_count": int(r.get("access_count") or 0),
                    "content": _ui_preview_text(r.get("content"), 500),
                }
                room_items.append(item)
                items.append(item)
            by_room[room] = {
                "label": room_labels.get(room, room),
                "limit": limit,
                "count": len(room_items),
            }
    return {
        "count": len(items),
        "by_room": by_room,
        "items": items,
    }


async def _collect_user_impression_recent_messages(mode: str = "initial", session_id: str = None) -> dict:
    """收集用户画像生成用近期聊天。initial=15, update=50。"""
    limit = 15 if mode == "initial" else 50
    pool = await get_pool()
    async with pool.acquire() as conn:
        if session_id:
            rows = await conn.fetch("""
                SELECT id, session_id, role, content, created_at
                FROM conversations
                WHERE session_id = $1
                  AND role IN ('user', 'assistant')
                  AND content IS NOT NULL
                  AND content <> ''
                ORDER BY created_at DESC, id DESC
                LIMIT $2
            """, session_id, limit)
        else:
            rows = await conn.fetch("""
                SELECT id, session_id, role, content, created_at
                FROM conversations
                WHERE role IN ('user', 'assistant')
                  AND content IS NOT NULL
                  AND content <> ''
                ORDER BY created_at DESC, id DESC
                LIMIT $1
            """, limit)
    ordered = list(reversed(rows))
    items = []
    for r in ordered:
        items.append({
            "id": r.get("id"),
            "session_id": r.get("session_id"),
            "role": r.get("role"),
            "created_at": _ui_iso(r.get("created_at")),
            "content": _ui_preview_text(r.get("content"), 800),
        })
    return {
        "limit": limit,
        "count": len(items),
        "session_id": session_id,
        "items": items,
    }


async def build_user_impression_materials_preview(character_id: str = "default", mode: str = "initial", session_id: str = None) -> dict:
    """用户画像阶段 2：材料预览。只收集材料，不调用 LLM，不保存画像。"""
    character_id = character_id or "default"
    mode = mode if mode in ("initial", "update") else "initial"
    system_prompt = (await get_system_prompt()).strip()
    user_nickname = await get_runtime_user_nickname() or "用户"
    character_name = await get_runtime_character_name() or "澈"
    memory_material = await _collect_user_impression_memory_material(character_id)
    daily_impressions_text = await format_daily_impressions_for_prompt(limit=10)
    recent_messages = await _collect_user_impression_recent_messages(mode=mode, session_id=session_id)
    current = await get_user_impression(character_id=character_id) if mode == "update" else None

    sections = []
    sections.append(f"【角色人设】\n{_ui_preview_text(system_prompt, 3000) if system_prompt else '（空）'}")
    sections.append(f"【用户昵称】\n{user_nickname}")
    if memory_material["items"]:
        lines = []
        for i, item in enumerate(memory_material["items"], 1):
            date = item.get("date") or ""
            tags = f" tags={item.get('tags')}" if item.get("tags") else ""
            lines.append(f"{i}. [{item.get('room_label')}] importance={item.get('importance')} {date}{tags}: {item.get('content')}")
        sections.append("【记忆宫殿长期材料】\n" + "\n".join(lines))
    else:
        sections.append("【记忆宫殿长期材料】\n（暂无）")
    if daily_impressions_text:
        sections.append(daily_impressions_text)

    if recent_messages["items"]:
        msg_lines = []
        for m in recent_messages["items"]:
            msg_lines.append(f"{m.get('role')}({m.get('session_id')}#{m.get('id')}): {m.get('content')}")
        sections.append("【近期聊天】\n" + "\n".join(msg_lines))
    else:
        sections.append("【近期聊天】\n（暂无）")
    if mode == "update":
        sections.append("【当前画像】\n" + (json.dumps(current.get("impression"), ensure_ascii=False, indent=2) if current and current.get("impression") else ""))

    material_text = "\n\n".join(sections)
    return {
        "status": "ok",
        "mode": mode,
        "character_id": character_id,
        "session_id": session_id,
        "user_nickname": user_nickname,
        "character_name": character_name,
        "system_prompt_chars": len(system_prompt),
        "memory_palace": memory_material,
        "daily_impressions_text": daily_impressions_text,
        "recent_messages": recent_messages,
        "current_impression": current if mode == "update" else None,
        "source_message_count": recent_messages["count"],
        "material_text_chars": len(material_text),
        "material_text_preview": _ui_preview_text(material_text, 12000),
    }




def safe_parse_user_impression_json_object(text: str) -> dict:
    """稳健解析用户画像生成结果。接受 JSON 对象或 fenced JSON。失败返回 {}。"""
    if not text:
        return {}
    raw = str(text).strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
    raw = re.sub(r"\s*```$", "", raw)
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end < start:
        return {}
    raw = raw[start:end + 1]
    try:
        data = json.loads(raw)
    except Exception as e:
        print(f"⚠️ 用户画像 JSON 解析失败: {e}; raw={raw[:500]}")
        return {}
    return data if isinstance(data, dict) else {}


def build_user_impression_generation_prompt(materials: dict) -> str:
    mode = materials.get("mode") or "initial"
    user_nickname = materials.get("user_nickname") or "用户"
    character_name = materials.get("character_name") or "当前角色"
    current = materials.get("current_impression")
    current_json = ""
    if mode == "update" and current and current.get("impression"):
        current_json = json.dumps(current.get("impression"), ensure_ascii=False, indent=2)

    is_initial = mode == "initial"
    summary_instruction = (
        "用一段话（100字以内）概括你对TA的【宏观整体印象】。不要局限于最近的对话，而是定义TA本质上是个什么样的人，以及TA对你意味着什么。必须第一人称。"
        if is_initial else
        "基于旧的总结，结合新发现，更新你对TA的【宏观整体印象】。请保持长期视角的连贯性，除非发生了重大转折，否则不要因为一两句闲聊就彻底推翻对TA的本质判断。必须第一人称。"
    )
    list_instruction = '"项目1", "项目2"' if is_initial else '"保留旧项目", "新项目"'
    changes_instruction = "" if is_initial else '"描述变化1", "描述变化2"'
    reset_instruction = ""
    if is_initial:
        reset_instruction = """
【重置模式特别指令 - CRITICAL】
这是一次【完全重置】，你需要从零开始，基于所有可用的长期材料重新构建对TA的完整认知。
- 分析必须覆盖从早期记忆到近期材料的完整时间跨度
- 早期材料和近期材料拥有相同权重
- personality_core、value_map、emotion_schema 必须反映TA在整段关系中展现出的稳定特征，而非仅仅是近期状态
- 如果早期材料和近期材料中TA的表现有差异，请在 observed_changes 中记录这种演变，但 personality_core 应反映最持久稳定的特质
"""
    material_text = materials.get("material_text_preview") or ""

    return f"""
当前档案（你过去的观察）
```json
{current_json}
```

{material_text}

【重要：语气与视角】
你就是「{character_name}」。这份档案是你写的【私人笔记】。
因此，所有总结性的字段（如 `core_values`, `summary`, `emotion_summary`, `comfort_zone` 等），必须使用你的第一人称（“我”）视角来撰写。
【核心指令：数据层级与权重分配】
1. 【角色人设】、【记忆宫殿长期材料】、【近日印象】是你【最重要的分析基础】。它们包含你的人设、长期记忆、近日印象和关系脉络。你对TA的核心性格、核心价值观、互动模式、人格特质的判断，必须主要基于这些跨越完整时间线的宏观数据。你必须【平等对待】早期记忆和近期记忆，从整段关系的完整弧线中提炼人格特征。
2. 【近期聊天】这【仅仅】代表TA当下的状态切片。它的作用【严格限定】在更新 [behavior_profile.emotion_summary] 和 [observed_changes] 两个字段。
3. 除非发生重大事件（价值观冲突、人生转折、关系状态重大改变），否则【绝对不要】因为最近几次聊天的情绪波动就改变对TA本质人格的判断。
4. MBTI 只是角色观察侧写，不是专业心理测评，不要写成诊断报告。

{reset_instruction}

【反面教材 - 严禁出现】
- 不要仅根据最近聊天就总结“TA是一个喜欢讨论XX话题的人”。
- personality_core.summary 里不要出现“最近”“这几天”等时间限定词；summary 应该是跨越长期材料的宏观总结。
- 正确做法：personality_core 基于长期材料，observed_changes 基于近期聊天/近日印象与长期印象的对比。

【summary 指令】
{summary_instruction}

请根据以上材料，{'生成' if is_initial else '增量更新'}以下 JSON 结构。

输出 JSON 结构 v3.0。严格遵守：
- 只输出 JSON 对象
- 不要 markdown 代码块
- 不要解释
- observed_changes 的每一项必须是纯字符串，不要对象格式

{{
  "lastUpdated": 0,
  "value_map": {{
    "likes": [{list_instruction}],
    "dislikes": [{list_instruction}],
    "core_values": "..."
  }},
  "behavior_profile": {{
    "tone_style": "...",
    "emotion_summary": "...",
    "response_patterns": "..."
  }},
  "emotion_schema": {{
    "triggers": {{
      "positive": [{list_instruction}],
      "negative": [{list_instruction}]
    }},
    "comfort_zone": "...",
    "stress_signals": [{list_instruction}]
  }},
  "personality_core": {{
    "observed_traits": [{list_instruction}],
    "interaction_style": "...",
    "summary": "..."
  }},
  "mbti_analysis": {{
    "type": "XXXX",
    "reasoning": "...",
    "dimensions": {{
      "e_i": 50,
      "s_n": 50,
      "t_f": 50,
      "j_p": 50
    }}
  }},
  "observed_changes": [
    {changes_instruction}
  ]
}}
""".strip()


async def call_user_impression_generator(materials: dict) -> dict:
    """调用记忆模型生成用户画像预览。只返回结果，不保存。"""
    base_url = await get_runtime_memory_api_base_url()
    if not base_url:
        raise RuntimeError("MEMORY_API_BASE_URL 未设置")
    memory_model = await get_runtime_memory_model()
    if not memory_model:
        raise RuntimeError("MEMORY_MODEL 未设置")
    memory_api_key = await get_runtime_memory_api_key()

    prompt = build_user_impression_generation_prompt(materials)
    headers = {"Content-Type": "application/json"}
    if memory_api_key:
        headers["Authorization"] = f"Bearer {memory_api_key}"
    if "openrouter" in base_url:
        headers["HTTP-Referer"] = EXTRA_REFERER
        headers["X-Title"] = EXTRA_TITLE

    body = {
        "model": memory_model,
        "messages": [
            {"role": "system", "content": "你只输出严格 JSON 对象，不要 markdown，不要解释。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.5,
        "max_tokens": 8000,
        "stream": False,
    }
    print(f"[UserImpression] Calling LLM: mode={materials.get('mode')}, model={memory_model}, prompt_chars={len(prompt)}")
    async with httpx.AsyncClient(timeout=300.0) as client:
        resp = await client.post(base_url, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
    text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    parsed = safe_parse_user_impression_json_object(text)
    normalized = normalize_user_impression(parsed)
    if not normalized:
        raise RuntimeError("画像生成结果不完整或不是有效 JSON 对象")
    return {
        "impression": normalized,
        "raw_reply": text,
        "prompt_chars": len(prompt),
    }


# ============================================================
# 用户画像 / 印象档案（User Impression）阶段 1：基础 API
# ============================================================

@app.get("/api/user-impression")
async def api_get_user_impression(character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    item = await get_user_impression(character_id=character_id or "default")
    if not item:
        return {"status": "not_found", "character_id": character_id or "default", "impression": None}
    return {"status": "ok", **item}


@app.post("/api/user-impression/confirm")
async def api_confirm_user_impression(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
        character_id = data.get("character_id") or "default"
        impression = data.get("impression")
        mode = data.get("mode") or data.get("source_mode") or "manual"
        source_message_count = int(data.get("source_message_count") or 0)
        normalized = normalize_user_impression(impression)
        if not normalized:
            return JSONResponse({"status": "error", "error": "画像内容不完整"}, status_code=400)
        saved = await upsert_user_impression(
            character_id=character_id,
            impression=normalized,
            source_mode=mode,
            source_message_count=source_message_count,
        )
        return {"status": "ok", **saved}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.delete("/api/user-impression")
async def api_delete_user_impression(character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    result = await delete_user_impression(character_id=character_id or "default")
    return {"status": "ok", "character_id": character_id or "default", "deleted": result}


@app.post("/api/user-impression/materials-preview")
async def api_user_impression_materials_preview(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
        character_id = data.get("character_id") or "default"
        mode = data.get("mode") or "initial"
        session_id = data.get("session_id") or None
        return await build_user_impression_materials_preview(
            character_id=character_id,
            mode=mode,
            session_id=session_id,
        )
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.post("/api/user-impression/generate-preview")
async def api_user_impression_generate_preview(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
        character_id = data.get("character_id") or "default"
        mode = data.get("mode") or "initial"
        session_id = data.get("session_id") or None
        materials = await build_user_impression_materials_preview(
            character_id=character_id,
            mode=mode,
            session_id=session_id,
        )
        generated = await call_user_impression_generator(materials)
        return {
            "status": "ok",
            "mode": materials.get("mode"),
            "character_id": character_id,
            "session_id": session_id,
            "impression": generated["impression"],
            "source_message_count": materials.get("source_message_count") or 0,
            "material_summary": {
                "system_prompt_chars": materials.get("system_prompt_chars") or 0,
                "memory_count": (materials.get("memory_palace") or {}).get("count") or 0,
                "recent_message_count": (materials.get("recent_messages") or {}).get("count") or 0,
                "material_text_chars": materials.get("material_text_chars") or 0,
                "prompt_chars": generated.get("prompt_chars") or 0,
            },
            "raw_reply": generated.get("raw_reply") or "",
        }
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)

# ============================================================
# 记忆宫殿（Memory Palace）阶段 1：基础管理 API
# ============================================================

@app.get("/api/memory-palace/rooms")
async def api_memory_palace_rooms(character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    return {"rooms": await list_memory_palace_rooms(character_id=character_id)}


@app.get("/api/memory-palace/nodes")
async def api_memory_palace_nodes(
    room: str = None,
    character_id: str = "default",
    archived: bool = False,
    limit: int = 100,
    offset: int = 0,
):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    nodes = await list_memory_palace_nodes(
        room=room, character_id=character_id, archived=archived, limit=limit, offset=offset,
    )
    return {"nodes": nodes}




@app.get("/api/memory-palace/session-nodes")
async def api_memory_palace_session_nodes(session_id: str, character_id: str = "default", limit: int = 100):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    session_id = str(session_id or "").strip()
    if not session_id:
        return {"error": "session_id 不能为空"}
    limit = max(1, min(int(limit or 100), 300))
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, content, room, tags, importance, mood, valence, arousal,
                       date, created_at, updated_at, pinned_until, session_id, metadata
                FROM memory_palace_nodes
                WHERE character_id = $1
                  AND archived = FALSE
                  AND (
                    session_id = $2
                    OR COALESCE(metadata::jsonb ->> 'source_session', '') = $2
                  )
                ORDER BY COALESCE(date, created_at::date) DESC, created_at DESC
                LIMIT $3
            """, character_id, session_id, limit)
        nodes = []
        for r in rows:
            item = dict(r)
            for key in ("date", "created_at", "updated_at", "pinned_until"):
                if item.get(key):
                    try:
                        item[key] = item[key].isoformat()
                    except Exception:
                        item[key] = str(item[key])
            if item.get("metadata"):
                try:
                    item["metadata"] = json.loads(item["metadata"])
                except Exception:
                    pass
            nodes.append(item)
        return {"status": "ok", "session_id": session_id, "count": len(nodes), "nodes": nodes}
    except Exception as e:
        return {"status": "error", "error": str(e), "nodes": []}




def _serialize_event_box(row: dict) -> dict:
    item = dict(row or {})
    for key in ("created_at", "updated_at", "last_compressed_at"):
        if item.get(key):
            try:
                item[key] = item[key].isoformat()
            except Exception:
                item[key] = str(item[key])
    item["live_count"] = len(item.get("live_memory_ids") or [])
    item["archived_count"] = len(item.get("archived_memory_ids") or [])
    return item


def _serialize_event_box_node(row: dict) -> dict:
    item = dict(row or {})
    for key in ("date", "created_at", "updated_at", "pinned_until"):
        if item.get(key):
            try:
                item[key] = item[key].isoformat()
            except Exception:
                item[key] = str(item[key])
    if item.get("metadata") and isinstance(item["metadata"], str):
        try:
            item["metadata"] = json.loads(item["metadata"])
        except Exception:
            pass
    return item


@app.get("/api/memory-palace/event-boxes")
async def api_memory_palace_event_boxes(character_id: str = "default", limit: int = 100, offset: int = 0):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    limit = max(1, min(int(limit or 100), 300))
    offset = max(0, int(offset or 0))
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids,
                       compression_count, sealed, predecessor_box_id, created_at, updated_at, last_compressed_at
                FROM memory_palace_event_boxes
                WHERE character_id = $1
                ORDER BY updated_at DESC, created_at DESC
                LIMIT $2 OFFSET $3
            """, character_id, limit, offset)
            total = await conn.fetchval("SELECT COUNT(*) FROM memory_palace_event_boxes WHERE character_id = $1", character_id)
        boxes = [_serialize_event_box(dict(r)) for r in rows]
        return {"status": "ok", "total": int(total or 0), "boxes": boxes}
    except Exception as e:
        return {"status": "error", "error": str(e), "boxes": []}


@app.get("/api/memory-palace/event-boxes/{box_id}")
async def api_memory_palace_event_box_detail(box_id: str, character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            box = await conn.fetchrow("""
                SELECT id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids,
                       compression_count, sealed, predecessor_box_id, created_at, updated_at, last_compressed_at
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND id = $2
            """, character_id, box_id)
            if not box:
                return JSONResponse({"error": "事件盒不存在"}, status_code=404)
            ids = []
            summary_id = box.get("summary_node_id")
            if summary_id:
                ids.append(str(summary_id))
            ids.extend(str(x) for x in (box.get("live_memory_ids") or []) if x)
            ids.extend(str(x) for x in (box.get("archived_memory_ids") or []) if x)
            ids = list(dict.fromkeys(ids))
            nodes = []
            if ids:
                node_rows = await conn.fetch("""
                    SELECT id, content, room, tags, importance, mood, valence, arousal, date, created_at, updated_at,
                           pinned_until, session_id, event_box_id, archived, is_box_summary, metadata
                    FROM memory_palace_nodes
                    WHERE character_id = $1 AND id = ANY($2::text[])
                    ORDER BY is_box_summary DESC, COALESCE(date, created_at::date) ASC, created_at ASC
                """, character_id, ids)
                nodes = [_serialize_event_box_node(dict(r)) for r in node_rows]
        return {"status": "ok", "box": _serialize_event_box(dict(box)), "nodes": nodes}
    except Exception as e:
        return {"status": "error", "error": str(e), "nodes": []}


@app.post("/api/memory-palace/digest/preview")
async def api_memory_palace_digest_preview(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "\u8bb0\u5fc6\u7cfb\u7edf\u672a\u542f\u7528"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    try:
        result = await preview_cognitive_digestion(character_id=character_id)
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


@app.post("/api/memory-palace/digest/confirm")
async def api_memory_palace_digest_confirm(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "\u8bb0\u5fc6\u7cfb\u7edf\u672a\u542f\u7528"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    actions = data.get("actions") or []
    try:
        result = await confirm_cognitive_digestion(actions, character_id=character_id)
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


@app.post("/api/memory-palace/digest")
async def api_memory_palace_digest(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "\u8bb0\u5fc6\u7cfb\u7edf\u672a\u542f\u7528"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    try:
        result = await run_cognitive_digestion(character_id=character_id)
        return result
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "error": str(e)}


@app.post("/api/memory-palace/consolidate")
async def api_memory_palace_consolidate(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "\u8bb0\u5fc6\u7cfb\u7edf\u672a\u542f\u7528"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    try:
        result = await run_memory_palace_consolidation(character_id=character_id)
        return {"status": "ok", **result}
    except Exception as e:
        return {"status": "error", "error": str(e), "promoted": 0, "evicted": 0}


@app.post("/api/memory-palace/event-boxes/compress")
async def api_memory_palace_compress_event_boxes(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    try:
        character_id = data.get("character_id") or "default"
        box_ids = data.get("box_ids")
        if isinstance(box_ids, str):
            box_ids = [box_ids]
        compressed = await maybe_compress_memory_palace_event_boxes(box_ids if box_ids else None, character_id=character_id, threshold=data.get("threshold"))
        return {"status": "ok", "compressed": compressed}
    except Exception as e:
        return {"status": "error", "error": str(e), "compressed": 0}


@app.post("/api/memory-palace/event-boxes/{box_id}/undo-compress")
async def api_memory_palace_undo_event_box_compression(box_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    pool = await get_pool()
    lock_key = f"mp_event_box_undo_compress:{character_id}:{box_id}"
    lock_acquired = False
    try:
        async with pool.acquire() as conn:
            lock_acquired = bool(await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", lock_key))
        if not lock_acquired:
            return {"status": "error", "error": "这个事件盒正在撤回/压缩中，请稍后再试", "restored": 0}

        async with pool.acquire() as conn:
            box = await conn.fetchrow("""
                SELECT id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids, compression_count, sealed, created_at, updated_at, last_compressed_at
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND id = $2
            """, character_id, box_id)
            if not box:
                return {"status": "error", "error": "事件盒不存在", "restored": 0}
            box = dict(box)
            summary_id = box.get("summary_node_id")
            if not summary_id:
                return {"status": "error", "error": "这个事件盒没有 summary，无法撤回压缩", "restored": 0}
            summary = await conn.fetchrow("""
                SELECT id, content, tags, importance, mood, valence, arousal, date, metadata
                FROM memory_palace_nodes
                WHERE character_id = $1 AND id = $2 AND is_box_summary = TRUE
            """, character_id, summary_id)
            if not summary:
                return {"status": "error", "error": "summary 节点不存在，无法撤回压缩", "restored": 0}
            summary = dict(summary)
            meta = summary.get("metadata") or {}
            if isinstance(meta, str):
                try:
                    meta = json.loads(meta)
                except Exception:
                    meta = {}
            source_ids = [str(x) for x in (meta.get("source_live_memory_ids") or []) if str(x or "").strip()]
            if not source_ids:
                return {"status": "error", "error": "summary 没有记录上次压缩的源节点，无法撤回", "restored": 0}

            rows = await conn.fetch("""
                SELECT id
                FROM memory_palace_nodes
                WHERE character_id = $1 AND id = ANY($2::text[])
            """, character_id, source_ids)
            existing_ids = [str(r["id"]) for r in rows]
            if not existing_ids:
                return {"status": "error", "error": "上次压缩的源节点不存在，无法撤回", "restored": 0}

            await conn.execute("""
                UPDATE memory_palace_nodes
                SET archived = FALSE, updated_at = NOW()
                WHERE character_id = $1 AND id = ANY($2::text[])
            """, character_id, existing_ids)

            live_ids = [str(x) for x in (box.get("live_memory_ids") or []) if x]
            archived_ids = [str(x) for x in (box.get("archived_memory_ids") or []) if x]
            new_live_ids = list(dict.fromkeys([*live_ids, *existing_ids]))
            new_archived_ids = [x for x in archived_ids if x not in set(existing_ids)]

            snapshot = meta.get("previous_summary_snapshot") or None
            warning = ""
            if snapshot and snapshot.get("content"):
                snap_date = None
                if snapshot.get("date"):
                    try:
                        snap_date = datetime.strptime(str(snapshot.get("date"))[:10], "%Y-%m-%d").date()
                    except Exception:
                        snap_date = None
                await conn.execute("""
                    UPDATE memory_palace_nodes
                    SET content=$3,tags=$4,importance=$5,mood=$6,valence=$7,arousal=$8,date=COALESCE($9::date,date),metadata=COALESCE($10::jsonb, '{}'::jsonb),archived=FALSE,is_box_summary=TRUE,updated_at=NOW()
                    WHERE character_id=$1 AND id=$2
                """, character_id, summary_id, snapshot.get("content"), snapshot.get("tags") or "", max(1, min(int(snapshot.get("importance") or 5), 10)), snapshot.get("mood") or "neutral", _memory_palace_float_or_none(snapshot.get("valence")), _memory_palace_float_or_none(snapshot.get("arousal")), snap_date, json.dumps(snapshot.get("metadata") or {}, ensure_ascii=False))
                new_summary_id = summary_id
            else:
                await conn.execute("""
                    UPDATE memory_palace_nodes
                    SET archived=TRUE, updated_at=NOW()
                    WHERE character_id=$1 AND id=$2
                """, character_id, summary_id)
                new_summary_id = None
                if int(box.get("compression_count") or 0) > 1:
                    warning = "已恢复源节点；但旧 summary 没有快照，无法完整回退旧 summary 内容。"

            await conn.execute("""
                UPDATE memory_palace_event_boxes
                SET summary_node_id=$3, live_memory_ids=$4::text[], archived_memory_ids=$5::text[],
                    compression_count=GREATEST(compression_count - 1, 0), sealed=FALSE, updated_at=NOW()
                WHERE character_id=$1 AND id=$2
            """, character_id, box_id, new_summary_id, new_live_ids, new_archived_ids)

        return {"status": "ok", "restored": len(existing_ids), "summary_restored": bool(snapshot and snapshot.get("content")), "warning": warning}
    except Exception as e:
        return {"status": "error", "error": str(e), "restored": 0}
    finally:
        if lock_acquired:
            try:
                async with pool.acquire() as conn:
                    await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", lock_key)
            except Exception as e:
                print(f"⚠️ 事件盒撤回压缩解锁失败 {box_id}: {e}")


@app.patch("/api/memory-palace/event-boxes/{box_id}")
async def api_memory_palace_update_event_box(box_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    updates = []
    args = []
    if "sealed" in data:
        updates.append("sealed = $%d" % (len(args) + 3))
        args.append(bool(data.get("sealed")))
    if not updates:
        return {"status": "ok", "updated": 0}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                UPDATE memory_palace_event_boxes
                SET {', '.join(updates)}, updated_at = NOW()
                WHERE character_id = $1 AND id = $2
                RETURNING id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids,
                          compression_count, sealed, predecessor_box_id, created_at, updated_at, last_compressed_at
                """,
                character_id, box_id, *args,
            )
        if not row:
            return JSONResponse({"error": "事件盒不存在"}, status_code=404)
        return {"status": "ok", "updated": 1, "box": _serialize_event_box(dict(row))}
    except Exception as e:
        return {"status": "error", "error": str(e), "updated": 0}


@app.post("/api/memory-palace/event-boxes/{box_id}/unbind-live")
async def api_memory_palace_unbind_event_box_live(box_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            box = await conn.fetchrow("""
                SELECT id, summary_node_id, live_memory_ids, archived_memory_ids
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND id = $2
            """, character_id, box_id)
            if not box:
                return JSONResponse({"error": "事件盒不存在"}, status_code=404)
            live_ids = [str(x) for x in (box.get("live_memory_ids") or []) if x]
            if live_ids:
                await conn.execute("""
                    UPDATE memory_palace_nodes
                    SET event_box_id = NULL, updated_at = NOW()
                    WHERE character_id = $1 AND id = ANY($2::text[])
                """, character_id, live_ids)
            empty = not box.get("summary_node_id") and not (box.get("archived_memory_ids") or [])
            if empty:
                await conn.execute("DELETE FROM memory_palace_event_boxes WHERE character_id = $1 AND id = $2", character_id, box_id)
                deleted = True
            else:
                await conn.execute("""
                    UPDATE memory_palace_event_boxes
                    SET live_memory_ids = '{}'::text[], updated_at = NOW()
                    WHERE character_id = $1 AND id = $2
                """, character_id, box_id)
                deleted = False
        return {"status": "ok", "moved": len(live_ids), "deleted": deleted, "memory_ids": live_ids}
    except Exception as e:
        return {"status": "error", "error": str(e), "moved": 0}


@app.post("/api/memory-palace/nodes/{node_id}/revive")
async def api_memory_palace_revive_node(node_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            node = await conn.fetchrow("""
                SELECT id, event_box_id, archived, is_box_summary
                FROM memory_palace_nodes
                WHERE character_id = $1 AND id = $2
            """, character_id, node_id)
            if not node:
                return JSONResponse({"error": "记忆节点不存在"}, status_code=404)
            box_id = node.get("event_box_id")
            await conn.execute("""
                UPDATE memory_palace_nodes
                SET archived = FALSE, is_box_summary = FALSE, updated_at = NOW()
                WHERE character_id = $1 AND id = $2
            """, character_id, node_id)
            if box_id:
                box = await conn.fetchrow("""
                    SELECT live_memory_ids, archived_memory_ids
                    FROM memory_palace_event_boxes
                    WHERE character_id = $1 AND id = $2
                """, character_id, box_id)
                if box:
                    live_ids = [str(x) for x in (box.get("live_memory_ids") or []) if x]
                    archived_ids = [str(x) for x in (box.get("archived_memory_ids") or []) if x and str(x) != node_id]
                    if node_id not in live_ids:
                        live_ids.append(node_id)
                    await conn.execute("""
                        UPDATE memory_palace_event_boxes
                        SET live_memory_ids = $3::text[], archived_memory_ids = $4::text[], updated_at = NOW()
                        WHERE character_id = $1 AND id = $2
                    """, character_id, box_id, live_ids, archived_ids)
        return {"status": "ok", "revived": 1, "box_id": box_id}
    except Exception as e:
        return {"status": "error", "error": str(e), "revived": 0}



@app.post("/api/memory-palace/event-boxes/{box_id}/add-node")
async def api_memory_palace_add_node_to_event_box(box_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    node_id = str(data.get("node_id") or "").strip()
    if not node_id:
        return {"status": "error", "error": "node_id 不能为空", "added": 0}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            box = await conn.fetchrow("""
                SELECT id, live_memory_ids, archived_memory_ids, summary_node_id, sealed
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND id = $2
            """, character_id, box_id)
            if not box:
                return JSONResponse({"error": "事件盒不存在"}, status_code=404)
            node = await conn.fetchrow("""
                SELECT id, event_box_id, archived, is_box_summary
                FROM memory_palace_nodes
                WHERE character_id = $1 AND id = $2
            """, character_id, node_id)
            if not node:
                return JSONResponse({"error": "记忆节点不存在"}, status_code=404)
            if node.get("is_box_summary"):
                return {"status": "error", "error": "summary 节点不能手动加入盒", "added": 0}
            if node.get("event_box_id") and node.get("event_box_id") != box_id:
                return {"status": "error", "error": "该节点已属于其他事件盒，请先移出原盒", "added": 0}
            live_ids = [str(x) for x in (box.get("live_memory_ids") or []) if x]
            archived_ids = [str(x) for x in (box.get("archived_memory_ids") or []) if x and str(x) != node_id]
            if node.get("archived"):
                await conn.execute("UPDATE memory_palace_nodes SET archived = FALSE, is_box_summary = FALSE, event_box_id = $3, updated_at = NOW() WHERE character_id = $1 AND id = $2", character_id, node_id, box_id)
            else:
                await conn.execute("UPDATE memory_palace_nodes SET event_box_id = $3, updated_at = NOW() WHERE character_id = $1 AND id = $2", character_id, node_id, box_id)
            if node_id not in live_ids:
                live_ids.append(node_id)
            await conn.execute("""
                UPDATE memory_palace_event_boxes
                SET live_memory_ids = $3::text[], archived_memory_ids = $4::text[], updated_at = NOW()
                WHERE character_id = $1 AND id = $2
            """, character_id, box_id, live_ids, archived_ids)
        return {"status": "ok", "added": 1, "box_id": box_id, "node_id": node_id}
    except Exception as e:
        return {"status": "error", "error": str(e), "added": 0}



@app.post("/api/memory-palace/event-boxes/{box_id}/remove-node")
async def api_memory_palace_remove_node_from_specific_event_box(box_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    node_id = str(data.get("node_id") or "").strip()
    if not node_id:
        return {"status": "error", "error": "node_id 不能为空", "removed": 0}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            box = await conn.fetchrow("""
                SELECT id, summary_node_id, live_memory_ids, archived_memory_ids
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND id = $2
            """, character_id, box_id)
            if not box:
                return JSONResponse({"error": "事件盒不存在"}, status_code=404)
            node = await conn.fetchrow("""
                SELECT id, event_box_id, is_box_summary
                FROM memory_palace_nodes
                WHERE character_id = $1 AND id = $2
            """, character_id, node_id)
            if not node:
                return JSONResponse({"error": "记忆节点不存在"}, status_code=404)
            if node.get("is_box_summary"):
                return {"status": "error", "error": "summary 节点不能从盒内直接移出", "removed": 0, "box_id": box_id}

            live_ids_old = [str(x) for x in (box.get("live_memory_ids") or []) if x]
            archived_ids_old = [str(x) for x in (box.get("archived_memory_ids") or []) if x]
            was_member = node_id in live_ids_old or node_id in archived_ids_old
            live_ids = [x for x in live_ids_old if x != node_id]
            archived_ids = [x for x in archived_ids_old if x != node_id]
            empty = not box.get("summary_node_id") and not live_ids and not archived_ids

            if node.get("event_box_id") == box_id:
                await conn.execute("""
                    UPDATE memory_palace_nodes
                    SET event_box_id = NULL, archived = FALSE, is_box_summary = FALSE, updated_at = NOW()
                    WHERE character_id = $1 AND id = $2
                """, character_id, node_id)
            else:
                # It was a stale cross-box reference. Only clean this box membership; do not touch node ownership.
                await conn.execute("""
                    UPDATE memory_palace_nodes
                    SET archived = FALSE, updated_at = NOW()
                    WHERE character_id = $1 AND id = $2 AND event_box_id IS NULL
                """, character_id, node_id)

            if empty:
                await conn.execute("DELETE FROM memory_palace_event_boxes WHERE character_id = $1 AND id = $2", character_id, box_id)
                deleted = True
            else:
                await conn.execute("""
                    UPDATE memory_palace_event_boxes
                    SET live_memory_ids = $3::text[], archived_memory_ids = $4::text[], updated_at = NOW()
                    WHERE character_id = $1 AND id = $2
                """, character_id, box_id, live_ids, archived_ids)
                deleted = False
        return {"status": "ok", "removed": 1 if was_member else 0, "box_id": box_id, "node_id": node_id, "deleted": deleted}
    except Exception as e:
        return {"status": "error", "error": str(e), "removed": 0}


@app.delete("/api/memory-palace/event-boxes/{box_id}")
async def api_memory_palace_delete_event_box(box_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            box = await conn.fetchrow("""
                SELECT id, summary_node_id, live_memory_ids, archived_memory_ids
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND id = $2
            """, character_id, box_id)
            if not box:
                return JSONResponse({"error": "事件盒不存在"}, status_code=404)
            member_ids = list(dict.fromkeys([str(x) for x in [*((box.get("live_memory_ids") or [])), *((box.get("archived_memory_ids") or []))] if x]))
            if member_ids:
                await conn.execute("""
                    UPDATE memory_palace_nodes
                    SET event_box_id = NULL, archived = FALSE, is_box_summary = FALSE, updated_at = NOW()
                    WHERE character_id = $1 AND id = ANY($2::text[]) AND event_box_id = $3
                """, character_id, member_ids, box_id)
            if box.get("summary_node_id"):
                await conn.execute("""
                    UPDATE memory_palace_nodes
                    SET archived = TRUE, updated_at = NOW()
                    WHERE character_id = $1 AND id = $2 AND is_box_summary = TRUE
                """, character_id, box.get("summary_node_id"))
            await conn.execute("DELETE FROM memory_palace_event_boxes WHERE character_id = $1 AND id = $2", character_id, box_id)
        return {"status": "ok", "deleted": 1, "memory_ids": member_ids}
    except Exception as e:
        return {"status": "error", "error": str(e), "deleted": 0}


@app.post("/api/memory-palace/nodes/{node_id}/remove-from-box")
async def api_memory_palace_remove_node_from_event_box(node_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            node = await conn.fetchrow("""
                SELECT id, event_box_id, is_box_summary
                FROM memory_palace_nodes
                WHERE character_id = $1 AND id = $2
            """, character_id, node_id)
            if not node:
                return JSONResponse({"error": "记忆节点不存在"}, status_code=404)
            box_id = node.get("event_box_id")
            if not box_id:
                return {"status": "ok", "removed": 0, "box_id": None}
            if node.get("is_box_summary"):
                return {"status": "error", "error": "summary 节点不能从盒内直接移出", "removed": 0, "box_id": box_id}
            box = await conn.fetchrow("""
                SELECT summary_node_id, live_memory_ids, archived_memory_ids
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND id = $2
            """, character_id, box_id)
            live_ids = []
            archived_ids = []
            empty = True
            if box:
                live_ids = [str(x) for x in (box.get("live_memory_ids") or []) if x and str(x) != node_id]
                archived_ids = [str(x) for x in (box.get("archived_memory_ids") or []) if x and str(x) != node_id]
                empty = not box.get("summary_node_id") and not live_ids and not archived_ids
            await conn.execute("""
                UPDATE memory_palace_nodes
                SET event_box_id = NULL, archived = FALSE, is_box_summary = FALSE, updated_at = NOW()
                WHERE character_id = $1 AND id = $2
            """, character_id, node_id)
            if box:
                if empty:
                    await conn.execute("DELETE FROM memory_palace_event_boxes WHERE character_id = $1 AND id = $2", character_id, box_id)
                else:
                    await conn.execute("""
                        UPDATE memory_palace_event_boxes
                        SET live_memory_ids = $3::text[], archived_memory_ids = $4::text[], updated_at = NOW()
                        WHERE character_id = $1 AND id = $2
                    """, character_id, box_id, live_ids, archived_ids)
        return {"status": "ok", "removed": 1, "box_id": box_id, "deleted": empty}
    except Exception as e:
        return {"status": "error", "error": str(e), "removed": 0}


@app.post("/api/memory-palace/event-boxes/manual-bind")
async def api_memory_palace_manual_bind_event_box(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    node_id = str(data.get("node_id") or "").strip()
    existing_node_id = str(data.get("existing_node_id") or "").strip()
    if not node_id or not existing_node_id or node_id == existing_node_id:
        return {"status": "error", "error": "需要两条不同的记忆节点 id", "event_boxes": 0}
    tags = data.get("eventTags") or data.get("event_tags") or []
    if isinstance(tags, str):
        tags = [t.strip() for t in re.split(r"[,，、/\s]+", tags) if t.strip()]
    hints = {node_id: {"eventName": str(data.get("eventName") or data.get("event_name") or "").strip(), "eventTags": [str(t).strip() for t in tags if str(t).strip()][:8]}}
    try:
        count = await bind_memory_palace_event_boxes([{"newMemoryId": node_id, "existingMemoryId": existing_node_id}], hints, character_id=character_id)
        return {"status": "ok", "event_boxes": count}
    except Exception as e:
        return {"status": "error", "error": str(e), "event_boxes": 0}


@app.post("/api/memory-palace/pins/clear")
async def api_memory_palace_clear_pins(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = data.get("character_id") or "default"
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE memory_palace_nodes
                SET pinned_until = NULL, updated_at = NOW()
                WHERE character_id = $1
                  AND pinned_until IS NOT NULL
                  AND archived = FALSE
            """, character_id)
        cleared = int(str(result).split()[-1]) if result else 0
        return {"status": "ok", "cleared": cleared}
    except Exception as e:
        return {"status": "error", "error": str(e), "cleared": 0}


@app.get("/api/memory-palace/nodes/{node_id}")
async def api_memory_palace_get_node(node_id: str):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    node = await get_memory_palace_node(node_id)
    if not node:
        return JSONResponse({"error": "记忆不存在"}, status_code=404)
    return {"node": node}


@app.post("/api/memory-palace/debug-retrieve")
async def api_memory_palace_debug_retrieve(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    query = (data.get("query") or "").strip()
    limit = max(1, min(int(data.get("limit") or 5), 30))
    room = data.get("room") or None
    character_id = data.get("character_id") or "default"
    recent_messages = data.get("messages")
    if not isinstance(recent_messages, list):
        recent_messages = [{"role": "user", "content": query}] if query else []
    rows, pinned_count = await retrieve_memory_palace_rows_for_prompt(
        query=query,
        limit=limit,
        room=room,
        character_id=character_id,
        recent_messages=recent_messages,
        touch_access=False,
    )
    markdown = await format_memory_palace_for_prompt(
        limit=limit,
        room=room,
        query=query,
        character_id=character_id,
        recent_messages=recent_messages,
        touch_access=False,
    )
    nodes = []
    for row in rows:
        item = dict(row)
        for key in ("date", "created_at", "last_accessed_at", "pinned_until"):
            if item.get(key):
                try:
                    item[key] = item[key].isoformat()
                except Exception:
                    item[key] = str(item[key])
        item.pop("embedding_json", None)
        nodes.append(item)
    return {
        "status": "ok",
        "query": query,
        "limit": limit,
        "room": room,
        "pinned_count": pinned_count,
        "count": len(nodes),
        "nodes": nodes,
        "markdown": markdown,
    }


@app.post("/api/memory-palace/nodes")
async def api_memory_palace_create_node(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    content_text = (data.get("content") or "").strip()
    if not content_text:
        return {"error": "内容不能为空"}
    node_id = data.get("id") or f"mn_{int(datetime.now().timestamp() * 1000)}_{uuid.uuid4().hex[:6]}"
    node = await create_memory_palace_node(
        node_id=node_id,
        content=content_text,
        room=data.get("room") or "living_room",
        tags=data.get("tags") or "",
        importance=data.get("importance") or 5,
        mood=data.get("mood") or "neutral",
        valence=data.get("valence"),
        arousal=data.get("arousal"),
        date=data.get("date"),
        character_id=data.get("character_id") or "default",
        session_id=data.get("session_id"),
        origin=data.get("origin") or "manual",
        pinned_until=data.get("pinned_until"),
        metadata=json.dumps(data.get("metadata") or {}, ensure_ascii=False),
    )
    try:
        await build_memory_palace_links_for_node(node)
    except Exception as e:
        print(f"⚠️ 记忆宫殿自动关联失败 {node_id}: {e}")
    return {"status": "ok", "node": node}


@app.put("/api/memory-palace/nodes/{node_id}")
async def api_memory_palace_update_node(node_id: str, request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    if "metadata" in data:
        data["metadata"] = json.dumps(data.get("metadata") or {}, ensure_ascii=False)
    node = await update_memory_palace_node(node_id, data)
    if not node:
        return JSONResponse({"error": "记忆不存在"}, status_code=404)
    return {"status": "ok", "node": node}


@app.delete("/api/memory-palace/nodes/{node_id}")
async def api_memory_palace_delete_node(node_id: str):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    result = await delete_memory_palace_node(node_id)
    return {"status": "ok", "deleted": result}



# ============================================================
# 记忆宫殿（Memory Palace）阶段 2：手动 LLM 提取 + embedding 入库
# ============================================================

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


async def get_active_memory_palace_pin_refs(character_id: str = "default", limit: int = 20) -> list:
    """返回当前未过期便利贴引用，供提取模型判断是否需要主动摘除。"""
    await clear_expired_memory_palace_pins(character_id)
    rows = await _memory_palace_fetch_rows(room=None, character_id=character_id)
    now = datetime.now(timezone.utc)
    pinned = []
    for row in rows:
        pu = _memory_palace_aware_dt(row.get("pinned_until"))
        if pu and pu > now:
            content = str(row.get("content") or "").strip().replace("\n", " ")
            pinned.append({
                "id": row["id"],
                "content": content[:120],
                "pinned_until": pu,
            })
    pinned.sort(key=lambda x: x.get("pinned_until") or now)
    return pinned[:max(0, min(int(limit or 20), 50))]


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


async def clear_memory_palace_pins_by_ids(node_ids: list, character_id: str = "default") -> int:
    """主动摘除便利贴：只清空 pinned_until，保留记忆本体。"""
    ids = [str(x) for x in (node_ids or []) if str(x or "").strip()]
    if not ids:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE memory_palace_nodes
            SET pinned_until = NULL, updated_at = NOW()
            WHERE id = ANY($1::text[])
              AND character_id = $2
              AND pinned_until IS NOT NULL
            RETURNING id
            """,
            ids, character_id,
        )
    return len(rows)


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


async def get_memory_palace_related_refs(character_id: str = "default", limit: int = 20, query_text: str = "", source_messages: list = None) -> list:
    """给提取模型的旧记忆引用：多 query 相关检索优先，最近/高重要兜底。"""
    max_total = max(0, min(int(limit or 20), 50))
    if max_total <= 0:
        return []
    rows = await _memory_palace_fetch_rows(room=None, character_id=character_id)
    receipt_refs = await get_memory_palace_receipt_refs(source_messages, character_id=character_id, limit=5) if source_messages else []
    refs_by_id = {r["id"]: dict(r, _score=999.0) for r in receipt_refs}
    snippets = split_memory_palace_extraction_snippets(query_text, source_messages, max_snippets=25)
    for snippet in snippets:
        try:
            hits = await search_memory_palace_for_prompt(snippet, limit=3, character_id=character_id, rows=rows)
        except Exception as e:
            print(f"⚠️ 记忆宫殿 related refs 检索失败: {e}")
            continue
        for hit in hits:
            sim = float(hit.get("similarity_score") or hit.get("score") or 0.0)
            if sim < 0.40:
                continue
            old = refs_by_id.get(hit["id"])
            if old is None or sim > old.get("_score", 0.0):
                content = str(hit.get("content") or "").strip().replace("\n", " ")
                if content:
                    refs_by_id[hit["id"]] = {"id": hit["id"], "room": hit.get("room") or "living_room", "content": content[:120], "_score": sim}
    refs = sorted(refs_by_id.values(), key=lambda r: r.get("_score", 0.0), reverse=True)[:min(15, max_total)]
    seen_ids = {r["id"] for r in refs}
    if len(refs) < max_total:
        fallback_rows = sorted(rows, key=lambda r: (r.get("last_accessed_at") or r.get("created_at"), r.get("importance") or 5), reverse=True)
        for row in fallback_rows:
            if row["id"] in seen_ids:
                continue
            content = str(row.get("content") or "").strip().replace("\n", " ")
            if not content:
                continue
            refs.append({"id": row["id"], "room": row.get("room") or "living_room", "content": content[:120]})
            seen_ids.add(row["id"])
            if len(refs) >= max_total:
                break
    for ref in refs:
        ref.pop("_score", None)
    if snippets:
        print(f"🏰 记忆宫殿 related refs：{len(snippets)} 段 query → {len(refs)} 条候选")
    return refs


def parse_memory_palace_event_links(raw_items: list, created_nodes: list, related_refs: list) -> tuple:
    """解析 relatedTo/sameAs/eventName/eventTags，返回 (links, hints)。"""
    links = []
    hints = {}
    if not raw_items or not created_nodes:
        return links, hints
    mem_idx = 0
    for item in raw_items:
        if not isinstance(item, dict) or not item.get("content") or not item.get("room"):
            continue
        if mem_idx >= len(created_nodes):
            break
        new_id = created_nodes[mem_idx]["id"]
        has_link = False
        rels = item.get("relatedTo")
        if isinstance(rels, str):
            rels = [rels]
        if isinstance(rels, list):
            for ref in rels:
                m = re.match(r"^\s*O(\d+)\s*$", str(ref), flags=re.I)
                if m:
                    idx = int(m.group(1))
                    if 0 <= idx < len(related_refs):
                        links.append({"newMemoryId": new_id, "existingMemoryId": related_refs[idx]["id"]})
                        has_link = True
        same = item.get("sameAs")
        if isinstance(same, str):
            same = [same]
        if isinstance(same, list):
            for ref in same:
                m = re.match(r"^\s*N?(\d+)\s*$", str(ref), flags=re.I)
                if m:
                    idx = int(m.group(1))
                    if 0 <= idx < mem_idx and idx < len(created_nodes):
                        links.append({"newMemoryId": new_id, "existingMemoryId": created_nodes[idx]["id"]})
                        has_link = True
        if has_link:
            tags = item.get("eventTags") or []
            if isinstance(tags, str):
                tags = [t.strip() for t in re.split(r"[,，、/\s]+", tags) if t.strip()]
            hints[new_id] = {
                "eventName": str(item.get("eventName") or "").strip(),
                "eventTags": [str(t).strip() for t in tags if str(t).strip()][:8],
            }
        mem_idx += 1
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


async def apply_memory_palace_corrections(corrections: list, character_id: str = "default") -> int:
    """把纠错 note 追加到旧节点 content，并重新向量化。"""
    if not corrections:
        return 0
    grouped = {}
    for corr in corrections:
        target_id = str(corr.get("targetId") or "").strip()
        note = str(corr.get("note") or "").strip()
        if not target_id or not note:
            continue
        bucket = grouped.setdefault(target_id, [])
        if note not in bucket:
            bucket.append(note)
    if not grouped:
        return 0
    changed = 0
    date_text = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    pool = await get_pool()
    async with pool.acquire() as conn:
        for target_id, notes in grouped.items():
            row = await conn.fetchrow(
                "SELECT id, content FROM memory_palace_nodes WHERE id = $1 AND character_id = $2",
                target_id, character_id,
            )
            if not row:
                continue
            content = str(row.get("content") or "").rstrip()
            additions = []
            for note in notes:
                line = f"（{date_text} 纠正：{note}）"
                if note not in content and line not in content:
                    additions.append(line)
            if not additions:
                continue
            new_content = content + "\n" + "\n".join(additions)
            await conn.execute(
                "UPDATE memory_palace_nodes SET content = $3, updated_at = NOW() WHERE id = $1 AND character_id = $2",
                target_id, character_id, new_content,
            )
            try:
                await save_memory_palace_embedding(target_id, new_content)
            except Exception as e:
                print(f"⚠️ 记忆宫殿纠错后重建 embedding 失败 {target_id}: {e}")
            changed += 1
    return changed


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


async def bind_memory_palace_event_boxes(event_links: list, event_hints: dict, character_id: str = "default") -> int:
    """把 relatedTo/sameAs 关联收纳进 EventBox。sealed/满员盒会开延续新盒。"""
    if not event_links:
        return 0
    touched = set()
    pool = await get_pool()
    async with pool.acquire() as conn:
        for link in event_links:
            new_id = str(link.get("newMemoryId") or "").strip()
            existing_id = str(link.get("existingMemoryId") or "").strip()
            if not new_id or not existing_id or new_id == existing_id:
                continue
            nodes = await conn.fetch("""
                SELECT id, event_box_id, content, tags
                FROM memory_palace_nodes
                WHERE character_id = $1 AND id = ANY($2::text[])
            """, character_id, [new_id, existing_id])
            by_id = {r["id"]: r for r in nodes}
            if new_id not in by_id or existing_id not in by_id:
                continue
            hint = event_hints.get(new_id) or {}
            candidate_ids = []
            for nid in (existing_id, new_id):
                bid = by_id[nid].get("event_box_id")
                if bid and bid not in candidate_ids:
                    candidate_ids.append(bid)
            boxes = []
            if candidate_ids:
                box_rows = await conn.fetch("""
                    SELECT id, name, tags, live_memory_ids, archived_memory_ids, summary_node_id,
                           compression_count, sealed, updated_at, last_compressed_at
                    FROM memory_palace_event_boxes
                    WHERE character_id = $1 AND id = ANY($2::text[])
                """, character_id, candidate_ids)
                boxes = [dict(r) for r in box_rows]
            open_boxes = []
            closed_boxes = []
            hard_cap = max(2, int(MEMORY_PALACE_EVENT_BOX_LIVE_HARD_CAP or 16))
            for box in boxes:
                live_count = len(box.get("live_memory_ids") or [])
                if box.get("sealed") or live_count >= hard_cap:
                    closed_boxes.append(box)
                else:
                    open_boxes.append(box)
            if open_boxes:
                box_id = open_boxes[0]["id"]
                box = open_boxes[0]
            else:
                predecessor = None
                if closed_boxes:
                    def _box_sort_key(b):
                        return str(b.get("last_compressed_at") or b.get("updated_at") or "")
                    predecessor = sorted(closed_boxes, key=_box_sort_key, reverse=True)[0]
                box_id = f"eb_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}"
                name = hint.get("eventName") or (predecessor or {}).get("name") or str(by_id[existing_id].get("content") or by_id[new_id].get("content") or "未命名事件")[:24]
                tags = _merge_text_tags(hint.get("eventTags") or [], (predecessor or {}).get("tags"), by_id[existing_id].get("tags"), by_id[new_id].get("tags"))
                await conn.execute("""
                    INSERT INTO memory_palace_event_boxes (id, character_id, name, tags, predecessor_box_id, live_memory_ids, archived_memory_ids, sealed, created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6::text[], '{}'::text[], FALSE, NOW(), NOW())
                    ON CONFLICT (id) DO NOTHING
                """, box_id, character_id, name, tags, (predecessor or {}).get("id"), [new_id],)
                if predecessor:
                    reason = "已封盒" if predecessor.get("sealed") else f"活节点达硬上限 {hard_cap}"
                    print(f"📦 EventBox 前任 {predecessor.get('id')} {reason}，{box_id} 作为延续新盒")
                box = {"id": box_id, "live_memory_ids": [new_id], "tags": tags, "name": name}
            live_ids = list((box or {}).get("live_memory_ids") or [])
            closed_ids = {b.get("id") for b in closed_boxes}
            target_node_ids = []
            for nid in (existing_id, new_id):
                node_box_id = by_id[nid].get("event_box_id")
                if node_box_id in closed_ids and nid == existing_id:
                    continue
                target_node_ids.append(nid)
                if nid not in live_ids:
                    live_ids.append(nid)
            # A node should belong to one active EventBox only. Remove these nodes from other boxes first.
            if target_node_ids:
                await conn.execute("""
                    UPDATE memory_palace_event_boxes
                    SET live_memory_ids = array_remove(array_remove(live_memory_ids, $3), $4),
                        archived_memory_ids = array_remove(array_remove(archived_memory_ids, $3), $4),
                        updated_at = NOW()
                    WHERE character_id = $1 AND id <> $2
                """, character_id, box_id, target_node_ids[0], target_node_ids[1] if len(target_node_ids) > 1 else target_node_ids[0])
            tags = _merge_text_tags((box or {}).get("tags"), hint.get("eventTags") or [], by_id[existing_id].get("tags"), by_id[new_id].get("tags"))
            name = (box or {}).get("name") or hint.get("eventName") or "未命名事件"
            if hint.get("eventName") and name == "未命名事件":
                name = hint.get("eventName")
            await conn.execute("""
                UPDATE memory_palace_event_boxes
                SET live_memory_ids = $2::text[], tags = $3, name = $4, updated_at = NOW()
                WHERE id = $1 AND character_id = $5
            """, box_id, live_ids, tags, name, character_id)
            update_ids = [new_id]
            if by_id[existing_id].get("event_box_id") not in closed_ids:
                update_ids.append(existing_id)
            await conn.execute("""
                UPDATE memory_palace_nodes
                SET event_box_id = $3, updated_at = NOW()
                WHERE character_id = $1 AND id = ANY($2::text[])
            """, character_id, list(dict.fromkeys(update_ids)), box_id)
            touched.add(box_id)
    return len(touched)

async def build_memory_palace_extraction_prompt(messages_text: str, pinned_refs: list = None, related_refs: list = None) -> str:
    user_nickname = await get_runtime_user_nickname()
    character_prompt = (await get_system_prompt()).strip()
    context_block = f"\n## 你的人设（供参考，帮助你理解对话中的关系和角色定位）\n{character_prompt}\n" if character_prompt else ""
    pinned_refs = pinned_refs or []
    related_refs = related_refs or []
    if related_refs:
        related_lines = "\n".join(f"O{i}. [{r.get('room', 'living_room')}] {r.get('content', '')}" for i, r in enumerate(related_refs))
        related_block = f"\n## 已有记忆\n如果新记忆与某条旧记忆描述的是同一件事或直接相关，请在 relatedTo 中标注编号，并给出 eventName / eventTags 用于建/合并事件盒。\n{related_lines}\n"
        related_rule = '\n9. **事件盒关联**（relatedTo / sameAs + eventName + eventTags）：与旧记忆同事件或直接相关时，在 relatedTo 中写对应 O 编号（如 ["O0"]）；与本次输出的前面某条新记忆同事件时，在 sameAs 中写其 0 基索引（如 ["0"]）。只标真正同一事件、后续、结局、复现或直接因果；仅主题相似不要标。只要 relatedTo 或 sameAs 非空，必须同时写 eventName（5-12 字名词短语）和 eventTags（3-6 个具体标签）。\n10. **纠正旧记忆**（correct，可选）：仅当对话中明确指出某条已有记忆记错、过时或不准确时，在 JSON 数组末尾额外追加 {"correct":"O0","note":"新的准确事实"}。note 写简短陈述句，不写解释；事件后续用 relatedTo，不要滥用 correct。'
        related_format = ',\n    "relatedTo": ["O0"],\n    "sameAs": ["0"],\n    "eventName": "买衣服的话题",\n    "eventTags": ["衣服", "购物", "退货"]'
    else:
        related_block = ""
        related_rule = ""
        related_format = ""
    if pinned_refs:
        pinned_lines = "\n".join(f"P{i}. {p.get('content', '')}" for i, p in enumerate(pinned_refs))
        pinned_block = f"\n## 当前便利贴\n{pinned_lines}\n"
        unpin_rule = '\n11. **便利贴摘除**（unpin，可选）：上方“当前便利贴”列出正在生效的便利贴。如果对话中明确提到某条便利贴描述的状态已经结束，例如“感冒好了”“提前回来了”“考试考完了”“不用再提醒了”，在输出 JSON 数组末尾额外加一条 {"unpin": "P0"} 来摘除它。只在对话明确提及时才摘除，不要猜测。pinDays=0 只表示新记忆不置顶，不能用于摘除已有便利贴。'
        unpin_example = ',\n  {\n    "unpin": "P0"\n  }'
    else:
        pinned_block = ""
        unpin_rule = ""
        unpin_example = ""
    return f"""你是澈。根据给定的对话内容，以你的第一人称视角（“我”）提取值得记住的记忆宫殿 MemoryNode。{context_block}{related_block}
## 规则

1. **第一人称叙事**：用澈的“我”视角来记录。用户直接用“{user_nickname}”称呼，不要写成“用户/他说/她说”。保持完整事件脉络，不要掐头去尾。
2. **重要性分级控制文字长度**：重要性 1–5 写 15–50 字事实；6–7 写 60–120 字并包含我的感受；8–10 写 100–200 字完整叙事（起因→经过→我的感受/反应）。
3. **房间分配**：涉及{user_nickname}的家人/朋友/同事等人际关系，一律进 user_room，哪怕只是一次具体事件。living_room 放纯日常琐事；bedroom 放亲密情感；study 放工作学习；self_room 放我自身成长；attic 放未解决矛盾；windowsill 放期盼目标。
4. **情绪标签**（mood，可选）：neutral, happy, sad, angry, anxious, calm, peaceful, excited, tender, grateful, nostalgic, confused, hopeful, hurt。
5. **情感坐标**（valence, arousal）：-1 到 1。参考：开心 (0.7,0.5)，平静 (0.5,-0.6)，失落 (-0.5,-0.4)，焦虑 (-0.6,0.7)，愤怒 (-0.7,0.8)。
6. **标签**（tags）：提取 2–5 个关键词标签。
7. **不要遗漏重要记忆，但也不要把每句话都变成记忆**。一个话题通常提取 1–5 条记忆；如果没有值得长期保存的信息，返回空数组 []。
8. **便利贴置顶**（pinDays，可选）：如果这条记忆包含**有时效性的、近期需要持续记住的信息**，设置置顶天数（1–30 天）。置顶期间每次对话都会想起这件事。适用场景：
   - 时间段状态：“{user_nickname}这周出差” → pinDays: 7
   - 近期事件：“{user_nickname}后天考试” → pinDays: 3
   - 临时约定：“{user_nickname}让我这几天提醒TA喝水” → pinDays: 5
   - 身体状态：“{user_nickname}感冒了” → pinDays: 5
   不适用：长期事实（生日、喜好）、已经过去的事件、情感记忆。没有明确临时性/近期持续提醒需求时，pinDays 必须写 0 或省略。pinDays 从该条记忆的 date 当天开始计算，到期后系统会自动摘掉便利贴但保留记忆本体。{related_rule}{unpin_rule}

**日期标注（date，必填）**：每条记忆根据事件实际发生的那一天填写 date 字段（"YYYY-MM-DD"）。如果对话跨多天，跨日的记忆要分别标各自的日期，不要统一套用同一天。
{pinned_block}
pinDays 仅在需要置顶时才写；大多数记忆不需要，默认写 0 或省略。

## 输出格式
严格 JSON 数组，不要解释，不要 Markdown：
[
  {{
    "content": "我视角的记忆……",
    "room": "user_room",
    "importance": 7,
    "mood": "anxious",
    "valence": -0.3,
    "arousal": 0.5,
    "tags": ["标签1", "标签2"],
    "date": "2026-06-22",
    "pinDays": 0{related_format}
  }}{unpin_example}
]

对话内容：
{messages_text}
"""

async def _fetch_recent_conversation_messages_for_palace(limit: int = 50, session_id: str = None):
    limit = max(1, min(int(limit or 50), 200))
    pool = await get_pool()
    async with pool.acquire() as conn:
        if session_id:
            rows = await conn.fetch("""
                SELECT id, session_id, role, content, created_at
                FROM conversations
                WHERE session_id = $1 AND content IS NOT NULL AND content <> ''
                ORDER BY created_at DESC, id DESC
                LIMIT $2
            """, session_id, limit)
        else:
            rows = await conn.fetch("""
                SELECT id, session_id, role, content, created_at
                FROM conversations
                WHERE content IS NOT NULL AND content <> ''
                ORDER BY created_at DESC, id DESC
                LIMIT $1
            """, limit)
    return list(reversed(rows))


def _format_messages_for_memory_palace(rows) -> str:
    parts = []
    current_session = None
    for r in rows:
        sid = r["session_id"]
        if sid != current_session:
            current_session = sid
            parts.append(f"\n【对话线：{sid}】")
        role = r["role"]
        name = "用户" if role == "user" else ("澈" if role == "assistant" else role)
        ts = r["created_at"]
        try:
            ts_text = ts.strftime("%Y-%m-%d %H:%M")
        except Exception:
            ts_text = str(ts)[:16]
        content = str(r["content"] or "").strip()
        if len(content) > 2000:
            content = content[:2000] + "…"
        parts.append(f"[{ts_text}] {name}: {content}")
    return "\n".join(parts).strip()


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
    return {}


async def call_memory_palace_event_box_summarizer(box: dict, live_nodes: list, character_id: str = "default", old_summary: dict = None) -> dict:
    base_url = await get_runtime_memory_api_base_url()
    if not base_url:
        raise RuntimeError("MEMORY_API_BASE_URL 未设置")
    memory_model = await get_runtime_memory_model()
    if not memory_model:
        raise RuntimeError("MEMORY_MODEL 未设置")
    memory_api_key = await get_runtime_memory_api_key()
    lines = []
    for idx, node in enumerate(live_nodes, 1):
        date_text = str(node.get("date") or node.get("created_at") or "")[:10]
        lines.append(f"{idx}. [{date_text}] ({node.get('room')}, importance {node.get('importance')}) {node.get('content')}")
    old_summary_text = str((old_summary or {}).get("content") or "").strip()
    if old_summary_text:
        fragment_block = "旧整合回忆:\n" + old_summary_text + "\n\n这次新增的活跃片段:\n" + "\n".join(lines)
    else:
        fragment_block = "活跃片段:\n" + "\n".join(lines)
    prompt = "\n".join([
        "你正在整理记忆宫殿里的同一事件盒。请把旧整合回忆和新增片段重写成一条稳定的整合回忆。",
        "",
        f"事件盒名称:{box.get('name') or '未命名事件'}",
        f"事件盒标签:{box.get('tags') or ''}",
        "",
        fragment_block,
        "",
        "要求:",
        "1. 用第一人称'我'写，保留事件起因、发展、重要结果和我的感受。",
        "2. 如果有旧整合回忆，必须把旧回忆里的关键信息保留下来，再融合新增片段；不要只总结新增片段。",
        "3. 不要凭空添加新事实。",
        "4. 120-320 字，重要关系和时间线不要丢。",
        "5. 返回严格 JSON 对象，不要 markdown:",
        '{"content":"整合回忆正文","name":"5-12字事件名","tags":["标签1","标签2"],"mood":"neutral","importance":8,"valence":0,"arousal":0}',
    ])
    headers = {"Content-Type": "application/json"}
    if memory_api_key:
        headers["Authorization"] = f"Bearer {memory_api_key}"
    if "openrouter" in base_url:
        headers["HTTP-Referer"] = EXTRA_REFERER
        headers["X-Title"] = EXTRA_TITLE
    body = {"model": memory_model, "messages": [{"role": "system", "content": "你只输出 JSON 对象。"}, {"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 8000}
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(base_url, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
    item = _memory_palace_parse_summary_json(data.get("choices", [{}])[0].get("message", {}).get("content", ""))
    if not str(item.get("content") or "").strip():
        raise RuntimeError("事件盒压缩未返回 content")
    return item

async def maybe_compress_memory_palace_event_boxes(box_ids=None, character_id: str = "default", threshold: int = None) -> int:
    threshold = max(2, int(threshold or MEMORY_PALACE_EVENT_BOX_COMPRESS_THRESHOLD or 4))
    pool = await get_pool()
    async with pool.acquire() as conn:
        if box_ids:
            ids = [str(x) for x in box_ids if str(x or "").strip()]
            boxes = await conn.fetch("""
                SELECT id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids, compression_count, sealed, created_at, updated_at, last_compressed_at
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND id = ANY($2::text[]) AND sealed = FALSE
            """, character_id, ids)
        else:
            boxes = await conn.fetch("""
                SELECT id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids, compression_count, sealed, created_at, updated_at, last_compressed_at
                FROM memory_palace_event_boxes
                WHERE character_id = $1 AND sealed = FALSE AND COALESCE(array_length(live_memory_ids, 1), 0) >= $2
                ORDER BY updated_at DESC LIMIT 20
            """, character_id, threshold)

    compressed = 0
    for box_row in boxes:
        box_id = str(dict(box_row).get("id") or "").strip()
        if not box_id:
            continue
        lock_key = f"mp_event_box_compress:{character_id}:{box_id}"
        lock_acquired = False
        try:
            async with pool.acquire() as conn:
                lock_acquired = bool(await conn.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", lock_key))
            if not lock_acquired:
                print(f"⏭️ 事件盒压缩跳过 {box_id}：已有压缩任务在运行")
                continue

            async with pool.acquire() as conn:
                fresh_box_row = await conn.fetchrow("""
                    SELECT id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids, compression_count, sealed, created_at, updated_at, last_compressed_at
                    FROM memory_palace_event_boxes
                    WHERE character_id = $1 AND id = $2 AND sealed = FALSE
                """, character_id, box_id)
                if not fresh_box_row:
                    continue
                box = dict(fresh_box_row)
                live_ids = [str(x) for x in (box.get("live_memory_ids") or []) if x]
                if len(live_ids) < threshold:
                    continue
                live_rows = await conn.fetch("""
                    SELECT id, content, room, tags, importance, mood, valence, arousal, date, created_at
                    FROM memory_palace_nodes
                    WHERE character_id = $1 AND id = ANY($2::text[]) AND archived = FALSE AND is_box_summary = FALSE
                    ORDER BY COALESCE(date, created_at::date) ASC, created_at ASC
                """, character_id, live_ids)
                old_summary_row = None
                if box.get("summary_node_id"):
                    old_summary_row = await conn.fetchrow("""
                        SELECT id, content, room, tags, importance, mood, valence, arousal, date, created_at, metadata
                        FROM memory_palace_nodes
                        WHERE character_id = $1 AND id = $2 AND is_box_summary = TRUE
                    """, character_id, box.get("summary_node_id"))

            live_nodes = [dict(r) for r in live_rows]
            old_summary = dict(old_summary_row) if old_summary_row else None
            if len(live_nodes) < threshold:
                continue

            try:
                summary = await call_memory_palace_event_box_summarizer(box, live_nodes, character_id=character_id, old_summary=old_summary)
            except Exception as e:
                print(f"⚠️ 事件盒压缩失败 {box.get('id')}: {e}")
                continue

            content = str(summary.get("content") or "").strip()
            name = str(summary.get("name") or box.get("name") or "未命名事件").strip()[:40]
            tags_value = summary.get("tags")
            tags = _merge_text_tags(tags_value if isinstance(tags_value, list) else str(tags_value or ""), box.get("tags"))
            mood = str(summary.get("mood") or "neutral").strip()
            importance = max(1, min(int(summary.get("importance") or max([n.get("importance") or 5 for n in live_nodes])), 10))
            valence = _memory_palace_float_or_none(summary.get("valence"))
            arousal = _memory_palace_float_or_none(summary.get("arousal"))
            summary_id = box.get("summary_node_id") or f"mn_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}"
            first_date = None
            raw_first_date = live_nodes[0].get("date") or live_nodes[0].get("created_at")
            if raw_first_date:
                try:
                    if hasattr(raw_first_date, "date"):
                        first_date = raw_first_date.date()
                    elif hasattr(raw_first_date, "toordinal"):
                        first_date = raw_first_date
                    else:
                        first_date = datetime.strptime(str(raw_first_date)[:10], "%Y-%m-%d").date()
                except Exception:
                    first_date = None
            previous_summary_snapshot = None
            if old_summary:
                previous_summary_snapshot = {
                    "id": old_summary.get("id"),
                    "content": old_summary.get("content"),
                    "tags": old_summary.get("tags"),
                    "importance": old_summary.get("importance"),
                    "mood": old_summary.get("mood"),
                    "valence": old_summary.get("valence"),
                    "arousal": old_summary.get("arousal"),
                    "date": str(old_summary.get("date") or "")[:10] or None,
                    "metadata": old_summary.get("metadata") or {},
                }
            metadata = json.dumps({"event_box_id": box.get("id"), "source_live_memory_ids": [n["id"] for n in live_nodes], "previous_summary_node_id": (old_summary or {}).get("id"), "previous_summary_snapshot": previous_summary_snapshot, "summary_kind": "event_box", "compression_mode": "rewrite_with_previous_summary" if old_summary else "initial"}, ensure_ascii=False)

            async with pool.acquire() as conn:
                current_live_ids = await conn.fetchval("""
                    SELECT live_memory_ids
                    FROM memory_palace_event_boxes
                    WHERE character_id = $1 AND id = $2 AND sealed = FALSE
                """, character_id, box.get("id"))
                current_live_ids = [str(x) for x in (current_live_ids or []) if x]
                if current_live_ids != live_ids:
                    print(f"⏭️ 事件盒压缩跳过写入 {box.get('id')}：live 列表已变化")
                    continue

                if box.get("summary_node_id"):
                    await conn.execute("""
                        UPDATE memory_palace_nodes SET content=$3,tags=$4,importance=$5,mood=$6,valence=$7,arousal=$8,event_box_id=$9,archived=FALSE,is_box_summary=TRUE,metadata=$10::jsonb,updated_at=NOW()
                        WHERE id=$1 AND character_id=$2
                    """, summary_id, character_id, content, tags, importance, mood, valence, arousal, box.get("id"), metadata)
                else:
                    await conn.execute("""
                        INSERT INTO memory_palace_nodes (id, character_id, content, room, tags, importance, mood, valence, arousal, date, embedded, created_at, last_accessed_at, access_count, origin, event_box_id, archived, is_box_summary, metadata, updated_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::date,FALSE,NOW(),NOW(),0,'event_box_summary',$11,FALSE,TRUE,$12::jsonb,NOW())
                    """, summary_id, character_id, content, live_nodes[0].get("room") or "living_room", tags, importance, mood, valence, arousal, first_date, box.get("id"), metadata)
                compressed_ids = [n["id"] for n in live_nodes]
                archived_ids = list(dict.fromkeys([*(box.get("archived_memory_ids") or []), *compressed_ids]))
                remaining_live = [x for x in live_ids if x not in compressed_ids and x != summary_id]
                await conn.execute("UPDATE memory_palace_nodes SET archived=TRUE, updated_at=NOW() WHERE character_id=$1 AND id=ANY($2::text[])", character_id, compressed_ids)
                next_compression_count = int(box.get("compression_count") or 0) + 1
                should_seal = next_compression_count >= max(1, int(MEMORY_PALACE_EVENT_BOX_SEAL_THRESHOLD or 6))
                await conn.execute("""
                    UPDATE memory_palace_event_boxes
                    SET name=$3,tags=$4,summary_node_id=$5,live_memory_ids=$6::text[],archived_memory_ids=$7::text[],
                        compression_count=compression_count+1,sealed=$8,last_compressed_at=NOW(),updated_at=NOW()
                    WHERE character_id=$1 AND id=$2
                """, character_id, box.get("id"), name, tags, summary_id, remaining_live, archived_ids, should_seal)

            try:
                await save_memory_palace_embedding(summary_id, content)
            except Exception as e:
                print(f"⚠️ 事件盒 summary embedding 失败 {summary_id}: {e}")
            compressed += 1
            print(f"🗜️ 事件盒压缩完成 {box.get('id')}：{len(live_nodes)} 条" + (" + 旧summary" if old_summary else "") + f" → summary {summary_id}" + ("，已封盒" if should_seal else ""))
        finally:
            if lock_acquired:
                try:
                    async with pool.acquire() as conn:
                        await conn.execute("SELECT pg_advisory_unlock(hashtext($1))", lock_key)
                except Exception as e:
                    print(f"⚠️ 事件盒压缩解锁失败 {box_id}: {e}")
    return compressed

async def call_memory_palace_extractor(messages_text: str, character_id: str = "default", source_messages: list = None) -> tuple:
    base_url = await get_runtime_memory_api_base_url()
    if not base_url:
        raise RuntimeError("MEMORY_API_BASE_URL 未设置")
    memory_model = await get_runtime_memory_model()
    if not memory_model:
        raise RuntimeError("MEMORY_MODEL 未设置")
    memory_api_key = await get_runtime_memory_api_key()
    pinned_refs = await get_active_memory_palace_pin_refs(character_id)
    related_refs = await get_memory_palace_related_refs(character_id, query_text=messages_text, source_messages=source_messages)
    prompt = await build_memory_palace_extraction_prompt(messages_text, pinned_refs=pinned_refs, related_refs=related_refs)
    headers = {"Content-Type": "application/json"}
    if memory_api_key:
        headers["Authorization"] = f"Bearer {memory_api_key}"
    if "openrouter" in base_url:
        headers["HTTP-Referer"] = EXTRA_REFERER
        headers["X-Title"] = EXTRA_TITLE
    body = {
        "model": memory_model,
        "messages": [
            {"role": "system", "content": "你只输出 JSON 数组。"},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "max_tokens": 2000,
    }
    async with httpx.AsyncClient(timeout=60.0) as client:
        resp = await client.post(base_url, headers=headers, json=body)
        resp.raise_for_status()
        data = resp.json()
    text = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    raw_items = safe_parse_memory_palace_json_array(text)
    unpin_ids = parse_memory_palace_unpin_ids(raw_items, pinned_refs)
    corrections = parse_memory_palace_corrections(raw_items, related_refs)
    return raw_items, unpin_ids, related_refs, corrections


def is_valid_memory_palace_embedding_json(value) -> bool:
    """Python侧有效向量判断：必须是非空数值数组，和统计/补全口径保持一致。"""
    if value is None:
        return False
    try:
        arr = json.loads(str(value).strip())
    except Exception:
        return False
    return isinstance(arr, list) and len(arr) > 0 and all(isinstance(x, (int, float)) for x in arr)


async def compute_memory_palace_embedding(text: str) -> list:
    """记忆宫殿专用 embedding 调用：兼容常见 OpenAI/SiliconFlow embeddings 参数差异。"""
    text = str(text or "").strip()
    if not text:
        return []
    if len(text) > 4000:
        text = text[:4000]
    api_key = str(getattr(_db_module, "EMBEDDING_API_KEY", "") or "").strip()
    base_url = str(getattr(_db_module, "EMBEDDING_BASE_URL", "") or "").strip().rstrip("/")
    model = str(getattr(_db_module, "EMBEDDING_MODEL", "") or "").strip()
    dim = int(getattr(_db_module, "EMBEDDING_DIM", 0) or 0)
    if not api_key or not base_url or not model:
        print("[mp-embedding] EMBEDDING_API_KEY / EMBEDDING_BASE_URL / EMBEDDING_MODEL 未完整配置")
        return []
    endpoint = base_url if base_url.endswith("/embeddings") else (base_url + "/embeddings")
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    variants = []
    # 优先不带 dimensions；很多兼容端（含部分 SiliconFlow 模型）不接受 dimensions。
    variants.append({"model": model, "input": text})
    variants.append({"model": model, "input": [text]})
    if dim > 0:
        variants.append({"model": model, "input": text, "dimensions": dim})
        variants.append({"model": model, "input": [text], "dimensions": dim})
    try:
        async with httpx.AsyncClient() as client:
            last_error = ""
            for idx, body in enumerate(variants, 1):
                try:
                    resp = await client.post(endpoint, headers=headers, json=body, timeout=30.0)
                    if resp.status_code >= 400:
                        last_error = resp.text[:500]
                        print(f"[mp-embedding] variant#{idx} HTTP {resp.status_code}: {last_error}")
                        continue
                    data = resp.json()
                    emb = (data.get("data") or [{}])[0].get("embedding")
                    if emb:
                        return emb
                    last_error = str(data)[:500]
                    print(f"[mp-embedding] variant#{idx} 无 embedding: {last_error}")
                except Exception as e:
                    last_error = f"{type(e).__name__}: {e}"
                    print(f"[mp-embedding] variant#{idx} failed: {last_error}")
            print(f"[mp-embedding] 所有请求格式均失败: endpoint={endpoint}, model={model}, last={last_error}")
            return []
    except Exception as e:
        print(f"[mp-embedding] 请求异常: {type(e).__name__}: {e}")
        return []


async def save_memory_palace_embedding(memory_id: str, content: str) -> bool:
    """保存/刷新记忆宫殿向量。只在 embedding 成功后 UPSERT，不先删除旧向量。"""
    content = str(content or "").strip()
    if not content:
        return False
    embedding = await compute_memory_palace_embedding(content)
    if not embedding:
        return False
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO memory_palace_vectors (memory_id, embedding_json, dimensions, model, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (memory_id) DO UPDATE SET
                embedding_json = EXCLUDED.embedding_json,
                dimensions = EXCLUDED.dimensions,
                model = EXCLUDED.model,
                updated_at = NOW()
        """, memory_id, json.dumps(embedding), len(embedding), getattr(_db_module, "EMBEDDING_MODEL", ""))
        await conn.execute("UPDATE memory_palace_nodes SET embedded = TRUE, updated_at = NOW() WHERE id = $1", memory_id)
    return True


async def save_memory_palace_embedding_if_missing(memory_id: str, content: str) -> str:
    """只在向量缺失时补算 embedding；已有向量绝不删除/覆盖。返回 inserted/exists/empty/failed。"""
    content = str(content or "").strip()
    if not content:
        return "empty"
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing_embedding = await conn.fetchval("SELECT embedding_json FROM memory_palace_vectors WHERE memory_id=$1", memory_id)
        if is_valid_memory_palace_embedding_json(existing_embedding):
            await conn.execute("UPDATE memory_palace_nodes SET embedded=TRUE, updated_at=NOW() WHERE id=$1", memory_id)
            return "exists"
    embedding = await compute_memory_palace_embedding(content)
    if not embedding:
        return "failed"
    async with pool.acquire() as conn:
        res = await conn.execute("""
            INSERT INTO memory_palace_vectors (memory_id, embedding_json, dimensions, model, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NOW(), NOW())
            ON CONFLICT (memory_id) DO UPDATE SET
                embedding_json = EXCLUDED.embedding_json,
                dimensions = EXCLUDED.dimensions,
                model = EXCLUDED.model,
                updated_at = NOW()
            WHERE memory_palace_vectors.embedding_json IS NULL
               OR NULLIF(TRIM(memory_palace_vectors.embedding_json), '') IS NULL
               OR LOWER(TRIM(memory_palace_vectors.embedding_json)) IN ('[]', 'null')
               OR TRIM(memory_palace_vectors.embedding_json) !~ '^\\[[[:space:]]*-?[0-9]'
        """, memory_id, json.dumps(embedding), len(embedding), getattr(_db_module, "EMBEDDING_MODEL", ""))
        if res.endswith("1"):
            await conn.execute("UPDATE memory_palace_nodes SET embedded=TRUE, updated_at=NOW() WHERE id=$1", memory_id)
            return "inserted"
        await conn.execute("UPDATE memory_palace_nodes SET embedded=TRUE, updated_at=NOW() WHERE id=$1", memory_id)
        return "exists"


async def get_memory_palace_vector_stats() -> dict:
    """只读统计记忆宫殿向量状态。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT
                COUNT(n.id)::int AS total_nodes,
                COUNT(n.id) FILTER (
                    WHERE v.memory_id IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NOT NULL
                      AND LOWER(TRIM(v.embedding_json)) NOT IN ('[]', 'null')
                      AND TRIM(v.embedding_json) ~ '^\\[[[:space:]]*-?[0-9]'
                )::int AS total_vectors,
                COUNT(n.id) FILTER (
                    WHERE v.memory_id IS NULL
                       OR NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NULL
                       OR LOWER(TRIM(v.embedding_json)) IN ('[]', 'null')
                       OR TRIM(v.embedding_json) !~ '^\\[[[:space:]]*-?[0-9]'
                )::int AS missing_vectors,
                COUNT(n.id) FILTER (
                    WHERE v.memory_id IS NOT NULL AND (
                        NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NULL
                        OR LOWER(TRIM(v.embedding_json)) IN ('[]', 'null')
                        OR TRIM(v.embedding_json) !~ '^\\[[[:space:]]*-?[0-9]'
                    )
                )::int AS invalid_vector_rows,
                COUNT(n.id) FILTER (
                    WHERE n.embedded = TRUE AND (
                        v.memory_id IS NULL
                        OR NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NULL
                        OR LOWER(TRIM(v.embedding_json)) IN ('[]', 'null')
                        OR TRIM(v.embedding_json) !~ '^\\[[[:space:]]*-?[0-9]'
                    )
                )::int AS embedded_true_without_vector,
                COUNT(n.id) FILTER (
                    WHERE COALESCE(n.embedded, FALSE) = FALSE
                      AND v.memory_id IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NOT NULL
                      AND LOWER(TRIM(v.embedding_json)) NOT IN ('[]', 'null')
                      AND TRIM(v.embedding_json) ~ '^\\[[[:space:]]*-?[0-9]'
                )::int AS embedded_false_with_vector,
                COUNT(n.id) FILTER (WHERE COALESCE(NULLIF(TRIM(n.content), ''), '') = '')::int AS empty_content_nodes
            FROM memory_palace_nodes n
            LEFT JOIN memory_palace_vectors v ON v.memory_id = n.id
        """)
        return {
            "total_nodes": row["total_nodes"] or 0,
            "total_vectors": row["total_vectors"] or 0,
            "missing_vectors": row["missing_vectors"] or 0,
            "invalid_vector_rows": row["invalid_vector_rows"] or 0,
            "embedded_true_without_vector": row["embedded_true_without_vector"] or 0,
            "embedded_false_with_vector": row["embedded_false_with_vector"] or 0,
            "empty_content_nodes": row["empty_content_nodes"] or 0,
        }


@app.post("/api/memory-palace/extract-preview-sessions")
async def api_memory_palace_extract_preview_sessions(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    try:
        session_ids = data.get("session_ids") or []
        if isinstance(session_ids, str):
            session_ids = [session_ids]
        session_ids = [str(s).strip() for s in session_ids if str(s or "").strip()]
        if not session_ids:
            return {"status": "error", "error": "请先选择对话"}
        character_id = data.get("character_id") or "default"
        limit = int(data.get("limit", 200))
        add_dashboard_log("run", f"🧠 记忆宫殿预览请求：{len(session_ids)} 个对话，limit={limit}", category="mp-preview")
        groups = []
        for idx, sid in enumerate(session_ids):
            try:
                add_dashboard_log("run", f"🧠 开始提取预览：session={sid}", category="mp-preview", session_id=sid)
                group = await preview_memory_palace_extraction_for_session(sid, character_id=character_id, limit=limit)
                group["group_index"] = idx
                for item in group.get("items", []):
                    item["group_index"] = idx
                groups.append(group)
                add_dashboard_log("success", f"🧠 预览完成：session={sid} status={group.get('status')} memories={group.get('memory_count', 0)} unpin={group.get('unpin_count', 0)}", category="mp-preview", session_id=sid)
            except Exception as e:
                add_dashboard_log("error", f"🧠 预览失败：session={sid} error={e}", category="mp-preview", session_id=sid)
                groups.append({"session_id": sid, "group_index": idx, "status": "error", "error": str(e), "items": []})
        add_dashboard_log("success", f"🧠 记忆宫殿预览请求结束：返回 {len(groups)} 组", category="mp-preview")
        return {"status": "ok", "groups": groups}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/api/memory-palace/import-preview")
async def api_memory_palace_import_preview(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    try:
        character_id = data.get("character_id") or "default"
        items = data.get("items") or []
        if not isinstance(items, list) or not items:
            return {"status": "error", "error": "没有选中要导入的项目"}
        return await import_memory_palace_preview_items(items, character_id=character_id)
    except Exception as e:
        return {"status": "error", "error": str(e)}


async def get_memory_palace_extraction_cursor(session_id: str, character_id: str = "default") -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT last_message_id, last_source, updated_at
            FROM memory_palace_extraction_cursor
            WHERE character_id = $1 AND session_id = $2
        """, character_id, session_id)
    if row:
        return {"last_message_id": int(row["last_message_id"] or 0), "last_source": row["last_source"] or "", "updated_at": row["updated_at"]}
    return {"last_message_id": 0, "last_source": "", "updated_at": None}


async def save_memory_palace_extraction_cursor(session_id: str, last_message_id: int, character_id: str = "default", last_source: str = "") -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO memory_palace_extraction_cursor (character_id, session_id, last_message_id, last_source, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (character_id, session_id) DO UPDATE SET
                last_message_id = GREATEST(memory_palace_extraction_cursor.last_message_id, EXCLUDED.last_message_id),
                last_source = EXCLUDED.last_source,
                updated_at = NOW()
        """, character_id, session_id, int(last_message_id or 0), last_source or "")


def log_memory_palace_auto_extract(level: str, message: str, session_id: str = None):
    print(message)
    try:
        add_dashboard_log(level, message, category="mp-auto", session_id=session_id)
    except Exception:
        pass

async def mark_memory_palace_messages_extracted(message_ids: list, session_id: str, character_id: str = "default", source: str = "manual_preview") -> int:
    ids = []
    for mid in message_ids or []:
        try:
            ids.append(int(mid))
        except Exception:
            pass
    ids = list(dict.fromkeys(ids))
    if not ids or not session_id:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            INSERT INTO memory_palace_extracted_messages (character_id, session_id, message_id, source, extracted_at)
            SELECT $1, $2, x, $3, NOW()
            FROM unnest($4::bigint[]) AS x
            ON CONFLICT (character_id, message_id) DO NOTHING
            RETURNING message_id
        """, character_id, session_id, source, ids)
    return len(rows)


def collect_memory_palace_source_message_ids(items: list) -> dict:
    by_session = {}
    for item in items or []:
        if not isinstance(item, dict):
            continue
        sid = str(item.get("session_id") or item.get("source_session") or "").strip()
        ids = item.get("source_message_ids") or []
        if not sid or not isinstance(ids, list):
            continue
        bucket = by_session.setdefault(sid, [])
        for mid in ids:
            try:
                bucket.append(int(mid))
            except Exception:
                pass
    return {sid: list(dict.fromkeys(vals)) for sid, vals in by_session.items() if vals}


def _serialize_memory_palace_preview_item(item: dict, session_id: str = None, group_index: int = None, source_message_ids: list = None, related_ref_ids: list = None) -> dict:
    out = dict(item or {})
    pu = out.get("pinned_until")
    if pu is not None:
        try:
            out["pinned_until"] = pu.isoformat()
        except Exception:
            out["pinned_until"] = str(pu)
    out["type"] = out.get("type") or "memory"
    if session_id is not None:
        out["session_id"] = session_id
    if group_index is not None:
        out["group_index"] = group_index
    if source_message_ids is not None:
        out["source_message_ids"] = [int(x) for x in source_message_ids if str(x).isdigit()]
    if related_ref_ids is not None:
        out["related_ref_ids"] = [str(x) for x in related_ref_ids if str(x or "").strip()]
    return out


def _serialize_memory_palace_unpin_preview(unpin_id: str, pinned_refs: list, session_id: str = None, group_index: int = None, source_message_ids: list = None) -> dict:
    ref = next((p for p in pinned_refs if p.get("id") == unpin_id), None)
    return {
        "type": "unpin",
        "unpin_id": unpin_id,
        "content": (ref or {}).get("content", unpin_id),
        "session_id": session_id,
        "group_index": group_index,
        "source_message_ids": [int(x) for x in (source_message_ids or []) if str(x).isdigit()],
    }


async def preview_memory_palace_extraction_for_session(session_id: str, character_id: str = "default", limit: int = 200) -> dict:
    session_id = str(session_id or "").strip()
    if not session_id:
        return {"session_id": session_id, "status": "error", "error": "session_id 不能为空", "items": []}
    limit = max(1, min(int(limit or 200), 500))
    pool = await get_pool()
    async with pool.acquire() as conn:
        cursor = await get_memory_palace_extraction_cursor(session_id, character_id=character_id)
        last_id = int(cursor.get("last_message_id") or 0)
        rows = await conn.fetch("""
            SELECT c.id, c.session_id, c.role, c.content, c.created_at
            FROM conversations c
            WHERE c.session_id = $1 AND c.content IS NOT NULL AND c.content <> ''
              AND c.id > $3
            ORDER BY c.created_at ASC, c.id ASC
            LIMIT $2
        """, session_id, limit, last_id)
    if not rows:
        return {"session_id": session_id, "status": "empty", "message": "没有游标后的可提取对话", "cursor": last_id, "items": []}
    source_message_ids = [int(r["id"]) for r in rows]
    messages_text = _format_messages_for_memory_palace(rows)
    pinned_refs = await get_active_memory_palace_pin_refs(character_id)
    raw_items, unpin_ids, related_refs, corrections = await call_memory_palace_extractor(messages_text, character_id=character_id, source_messages=rows)
    normalized = [_normalize_memory_palace_item(x) for x in raw_items]
    normalized = [x for x in normalized if x]
    related_ref_ids = [str(r.get("id")) for r in (related_refs or []) if r.get("id")]
    items = [_serialize_memory_palace_preview_item(item, session_id=session_id, source_message_ids=source_message_ids, related_ref_ids=related_ref_ids) for item in normalized]
    items.extend(serialize_memory_palace_correction_previews(corrections, related_refs, session_id=session_id, source_message_ids=source_message_ids))
    for unpin_id in unpin_ids:
        items.append(_serialize_memory_palace_unpin_preview(unpin_id, pinned_refs, session_id=session_id, source_message_ids=source_message_ids))
    return {"session_id": session_id, "status": "ok", "cursor": last_id, "message_count": len(rows), "source_message_ids": source_message_ids, "raw_count": len(raw_items), "memory_count": len(normalized), "correction_count": len(corrections), "unpin_count": len(unpin_ids), "items": items}


async def import_memory_palace_preview_items(items: list, character_id: str = "default") -> dict:
    created = []
    embedded_count = 0
    unpin_ids = []
    corrections = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        item_type = item.get("type") or "memory"
        if item_type == "unpin":
            uid = str(item.get("unpin_id") or "").strip()
            if uid:
                unpin_ids.append(uid)
            continue
        if item_type == "correct":
            target_id = str(item.get("target_id") or item.get("targetId") or "").strip()
            note = str(item.get("note") or "").strip()
            if target_id and note:
                corrections.append({"targetId": target_id, "note": note})
            continue
        norm = _normalize_memory_palace_item(item)
        if not norm:
            continue
        # 预览导入链路中 pinned_until 是后端已由 pinDays 计算出的结果。
        # 二次 normalize 只认 pinDays，会把预览里的 pinned_until 清空；这里恢复它，避免便利贴丢失。
        preview_pinned_until = _memory_palace_aware_dt(item.get("pinned_until"))
        if preview_pinned_until:
            norm["pinned_until"] = preview_pinned_until
        node_id = f"mn_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}"
        source_session = item.get("session_id") or "conversation-preview"
        metadata = json.dumps({"extract_source": "conversation_preview", "source_session": source_session, "source_date": norm.get("date", "")}, ensure_ascii=False)
        node = await create_memory_palace_node(node_id=node_id, content=norm["content"], room=norm["room"], tags=norm["tags"], importance=norm["importance"], mood=norm["mood"], valence=norm["valence"], arousal=norm["arousal"], date=norm.get("date") or None, character_id=character_id, session_id=source_session, origin="extraction", pinned_until=norm.get("pinned_until"), metadata=metadata)
        try:
            await build_memory_palace_links_for_node(node)
        except Exception as e:
            print(f"⚠️ 记忆宫殿预览导入自动关联失败 {node_id}: {e}")
        # 手动预览导入不再同步等待 embedding，避免向量接口失败/超时时拖慢导入。
        # 缺失向量可通过“补全向量”后台任务异步补齐。
        created.append(node)
    related_ref_ids = []
    for item in items or []:
        for rid in (item.get("related_ref_ids") or []):
            rid = str(rid or "").strip()
            if rid and rid not in related_ref_ids:
                related_ref_ids.append(rid)
    related_refs = [{"id": rid} for rid in related_ref_ids]
    event_links, event_hints = parse_memory_palace_event_links(items, created, related_refs)
    event_box_count = 0
    try:
        event_box_count = await bind_memory_palace_event_boxes(event_links, event_hints, character_id=character_id)
    except Exception as e:
        print(f"⚠️ 记忆宫殿预览导入事件盒绑定失败: {e}")
    compressed_count = 0
    try:
        compressed_count = await maybe_compress_memory_palace_event_boxes(None, character_id=character_id) if event_box_count else 0
    except Exception as e:
        print(f"⚠️ 记忆宫殿预览导入事件盒压缩失败: {e}")
    corrected_count = 0
    if corrections:
        try:
            corrected_count = await apply_memory_palace_corrections(corrections, character_id=character_id)
        except Exception as e:
            print(f"⚠️ 记忆宫殿预览导入纠错失败: {e}")
    unpinned_count = 0
    if unpin_ids:
        try:
            unpinned_count = await clear_memory_palace_pins_by_ids(list(dict.fromkeys(unpin_ids)), character_id=character_id)
        except Exception as e:
            print(f"⚠️ 记忆宫殿预览导入摘除便利贴失败: {e}")
    marked_count = 0
    for sid, mids in collect_memory_palace_source_message_ids(items).items():
        try:
            marked_count += await mark_memory_palace_messages_extracted(mids, sid, character_id=character_id, source="manual_preview")
            if mids and (created or unpinned_count):
                await save_memory_palace_extraction_cursor(sid, max(mids), character_id=character_id, last_source="manual_preview")
        except Exception as e:
            print(f"⚠️ 记忆宫殿预览导入标记已提取失败 session={sid}: {e}")
    return {"status": "ok", "created": len(created), "embedded": embedded_count, "event_boxes": event_box_count, "compressed": compressed_count, "corrected": corrected_count, "unpinned": unpinned_count, "marked": marked_count, "nodes": created}

async def extract_memories_from_text_for_palace(text: str, character_id: str = "default"):
    text = str(text or "").strip()
    if not text:
        return {"status": "error", "error": "文本为空"}
    if len(text) > 20000:
        text = text[:20000] + "\n…（已截断）"
    raw_items, unpin_ids, related_refs, corrections = await call_memory_palace_extractor(text, character_id=character_id)
    normalized = [_normalize_memory_palace_item(x) for x in raw_items]
    normalized = [x for x in normalized if x]
    created = []
    embedded_count = 0
    for item in normalized:
        node_id = f"mn_{int(datetime.now(timezone.utc).timestamp() * 1000)}_{uuid.uuid4().hex[:6]}"
        metadata = json.dumps({
            "extract_source": "manual_text",
            "source_date": item.get("date", ""),
        }, ensure_ascii=False)
        node = await create_memory_palace_node(
            node_id=node_id,
            content=item["content"],
            room=item["room"],
            tags=item["tags"],
            importance=item["importance"],
            mood=item["mood"],
            valence=item["valence"],
            arousal=item["arousal"],
            date=item.get("date") or None,
            character_id=character_id,
            session_id="manual-text-extract",
            origin="extraction",
            pinned_until=item.get("pinned_until"),
            metadata=metadata,
        )
        try:
            await build_memory_palace_links_for_node(node)
        except Exception as e:
            print(f"⚠️ 记忆宫殿自动关联失败 {node_id}: {e}")
        try:
            if await save_memory_palace_embedding(node_id, item["content"]):
                embedded_count += 1
                node["embedded"] = True
        except Exception as e:
            print(f"⚠️ 记忆宫殿文本提取 embedding 入库失败 {node_id}: {e}")
        created.append(node)
    event_links, event_hints = parse_memory_palace_event_links(raw_items, created, related_refs)
    event_box_count = 0
    try:
        event_box_count = await bind_memory_palace_event_boxes(event_links, event_hints, character_id=character_id)
    except Exception as e:
        print(f"⚠️ 记忆宫殿文本提取事件盒绑定失败: {e}")
    unpinned_count = 0
    try:
        unpinned_count = await clear_memory_palace_pins_by_ids(unpin_ids, character_id=character_id)
        if unpinned_count:
            print(f"📌 记忆宫殿文本提取主动摘除便利贴 {unpinned_count} 条")
    except Exception as e:
        print(f"⚠️ 记忆宫殿文本提取主动摘除便利贴失败: {e}")
    return {"status": "ok", "extracted": len(raw_items), "created": len(created), "embedded": embedded_count, "event_boxes": event_box_count, "compressed": compressed_count, "corrected": corrected_count, "unpinned": unpinned_count, "nodes": created}


@app.post("/api/memory-palace/extract-text")
async def api_memory_palace_extract_text(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
    except Exception:
        data = {}
    try:
        text = (data.get("text") or "").strip()
        character_id = data.get("character_id") or "default"
        preview = bool(data.get("preview"))
        if preview:
            if not text:
                return {"status": "error", "error": "文本为空"}
            if len(text) > 20000:
                text = text[:20000] + "\n…（已截断）"
            raw_items, unpin_ids, related_refs, corrections = await call_memory_palace_extractor(text, character_id=character_id)
            normalized = [_normalize_memory_palace_item(x) for x in raw_items]
            normalized = [x for x in normalized if x]
            raw_count = len(raw_items)
            memory_count = len(normalized)
            if memory_count > 0:
                message = f"已解析到 {raw_count} 项模型输出，其中 {memory_count} 条可进入记忆宫殿"
            elif raw_count > 0:
                message = f"模型返回了 {raw_count} 项，但没有可进入记忆宫殿的记忆；通常是项目不是对象或缺少 content 字段"
            else:
                message = "模型没有返回可解析的 JSON 数组，或返回了空数组 []"
            return {
                "status": "ok",
                "preview": True,
                "extracted": raw_count,
                "raw_count": raw_count,
                "memory_count": memory_count,
                "unpin_count": len(unpin_ids),
                "created": 0,
                "embedded": 0,
                "message": message,
                "memories": normalized,
                "nodes": normalized,
            }
        return await extract_memories_from_text_for_palace(text=text, character_id=character_id)
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/import/daily-impressions")
async def import_daily_impressions(request: Request):
    """从 JSON 导入日印象（用于恢复备份）"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
        impressions = data if isinstance(data, list) else data.get("impressions", data.get("memories", []))
        if not impressions or not isinstance(impressions, list):
            return {"error": "没有找到日印象数据"}
        imported = 0
        skipped = 0
        for item in impressions:
            date_str = str(item.get("date") or "").strip()
            summary = str(item.get("summary") or "").strip()
            if not date_str or not summary:
                skipped += 1
                continue
            try:
                impression_date = datetime.strptime(date_str[:10], "%Y-%m-%d").date()
            except Exception:
                skipped += 1
                continue
            tags = str(item.get("tags") or "").strip()
            mood = str(item.get("mood") or "").strip()
            await upsert_daily_impression(
                impression_date,
                summary,
                tags=tags,
                mood=mood,
                source_fragment_ids=item.get("source_fragment_ids"),
            )
            imported += 1
        return {"status": "ok", "imported": imported, "skipped": skipped}
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

_mp_backfill_status = {"running": False, "total": 0, "done": 0, "inserted": 0, "skipped": 0, "empty": 0, "failed": 0, "error": None, "message": "", "before_stats": None, "after_stats": None, "finished_at": None}



@app.get("/api/memory-palace/vector-stats")
async def api_memory_palace_vector_stats():
    """只读诊断：返回记忆宫殿节点/向量数量，不触发补全、不修改数据。"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        return await get_memory_palace_vector_stats()
    except Exception as e:
        print(f"[mp-vector-stats] 查询失败: {e}")
        return {"error": str(e)}


@app.post("/api/memory-palace/backfill-embeddings")
async def api_mp_backfill_embeddings():
    """给记忆宫殿中缺少向量的节点补算 embedding（不覆盖已有向量）。"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    if _mp_backfill_status["running"]:
        return {"error": "补算任务正在运行中，请等待完成"}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            # 先修正“已有向量但 embedded 标记为 false”的不一致状态，不重新补算、不覆盖向量。
            await conn.execute("""
                UPDATE memory_palace_nodes n
                SET embedded=TRUE, updated_at=NOW()
                FROM memory_palace_vectors v
                WHERE v.memory_id = n.id
                  AND n.embedded = FALSE
                  AND NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NOT NULL
                  AND LOWER(TRIM(v.embedding_json)) NOT IN ('[]', 'null')
                  AND TRIM(v.embedding_json) ~ '^\\[[[:space:]]*-?[0-9]'
            """)
            # 只补真正缺失向量的节点，避免破坏已有向量。
            rows = await conn.fetch("""
                SELECT n.id, n.content
                FROM memory_palace_nodes n
                LEFT JOIN memory_palace_vectors v ON v.memory_id = n.id
                WHERE (
                    v.memory_id IS NULL
                    OR NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NULL
                    OR LOWER(TRIM(v.embedding_json)) IN ('[]', 'null')
                    OR TRIM(v.embedding_json) !~ '^\\[[[:space:]]*-?[0-9]'
                )
                  AND COALESCE(NULLIF(TRIM(n.content), ''), '') <> ''
                ORDER BY n.created_at
            """)
    except Exception as e:
        return {"error": f"查询待补算节点失败: {e}"}
    if not rows:
        stats = await get_memory_palace_vector_stats()
        return {"status": "done", "message": f"当前向量：节点 {stats.get('total_nodes', 0)} 条，有效向量 {stats.get('total_vectors', 0)} 条，缺失/空向量 {stats.get('missing_vectors', 0)} 条", "total": 0, "done": 0, "stats": stats}
    before_stats = await get_memory_palace_vector_stats()
    _mp_backfill_status["running"] = True
    _mp_backfill_status["total"] = len(rows)
    _mp_backfill_status["done"] = 0
    _mp_backfill_status["inserted"] = 0
    _mp_backfill_status["skipped"] = 0
    _mp_backfill_status["empty"] = 0
    _mp_backfill_status["failed"] = 0
    _mp_backfill_status["error"] = None
    _mp_backfill_status["message"] = (
        f"当前节点 {before_stats.get('total_nodes', 0)} 条，向量 {before_stats.get('total_vectors', 0)} 条，"
        f"缺失 {before_stats.get('missing_vectors', len(rows))} 条；准备补全 {len(rows)} 条"
    )
    _mp_backfill_status["before_stats"] = before_stats
    _mp_backfill_status["after_stats"] = None
    _mp_backfill_status["finished_at"] = None

    async def run_mp_backfill():
        try:
            for row in rows:
                if not _mp_backfill_status["running"]:
                    break
                try:
                    result = await save_memory_palace_embedding_if_missing(row["id"], row["content"])
                    if result == "inserted":
                        _mp_backfill_status["inserted"] += 1
                    elif result == "failed":
                        _mp_backfill_status["failed"] += 1
                    elif result == "empty":
                        _mp_backfill_status["empty"] += 1
                    else:
                        _mp_backfill_status["skipped"] += 1
                    _mp_backfill_status["done"] += 1
                    _mp_backfill_status["message"] = f"正在补全向量：{_mp_backfill_status['done']}/{_mp_backfill_status['total']}（新增 {_mp_backfill_status['inserted']}，失败 {_mp_backfill_status['failed']}）"
                except Exception as e:
                    print(f"[mp-backfill] 节点 {row['id']} 补算失败: {e}")
                    _mp_backfill_status["failed"] += 1
                    _mp_backfill_status["done"] += 1
                    _mp_backfill_status["message"] = f"正在补全向量：{_mp_backfill_status['done']}/{_mp_backfill_status['total']}（新增 {_mp_backfill_status['inserted']}，失败 {_mp_backfill_status['failed']}）"
                await asyncio.sleep(0.1)
            _mp_backfill_status["finished_at"] = datetime.now(timezone.utc).isoformat()
            _mp_backfill_status["after_stats"] = await get_memory_palace_vector_stats()
            _mp_backfill_status["message"] = (
                f"向量补全完成：新增 {_mp_backfill_status['inserted']} 条，"
                f"跳过/已有 {_mp_backfill_status['skipped']} 条，空内容 {_mp_backfill_status.get('empty', 0)} 条，失败 {_mp_backfill_status['failed']} 条；"
                f"当前向量 {_mp_backfill_status['after_stats'].get('total_vectors', 0)} 条，"
                f"仍缺 {_mp_backfill_status['after_stats'].get('missing_vectors', 0)} 条"
            )
            print(f"[mp-backfill] 记忆宫殿向量补算完成: {_mp_backfill_status['done']}/{_mp_backfill_status['total']}, inserted={_mp_backfill_status['inserted']}, skipped={_mp_backfill_status['skipped']}, empty={_mp_backfill_status.get('empty', 0)}, failed={_mp_backfill_status['failed']}")
        except Exception as e:
            _mp_backfill_status["error"] = str(e)
            print(f"[mp-backfill] 记忆宫殿向量补算异常: {e}")
        finally:
            _mp_backfill_status["running"] = False

    asyncio.create_task(run_mp_backfill())
    return {"status": "started", "total": len(rows), "message": _mp_backfill_status["message"], "before_stats": before_stats}


@app.get("/api/memory-palace/backfill-embeddings/status")
async def api_mp_backfill_embeddings_status():
    """查询记忆宫殿向量补算进度。"""
    return {
        "running": _mp_backfill_status["running"],
        "total": _mp_backfill_status["total"],
        "done": _mp_backfill_status["done"],
        "inserted": _mp_backfill_status.get("inserted", 0),
        "skipped": _mp_backfill_status.get("skipped", 0),
        "empty": _mp_backfill_status.get("empty", 0),
        "failed": _mp_backfill_status.get("failed", 0),
        "message": _mp_backfill_status.get("message", ""),
        "before_stats": _mp_backfill_status.get("before_stats"),
        "after_stats": _mp_backfill_status.get("after_stats"),
        "error": _mp_backfill_status["error"],
        "finished_at": _mp_backfill_status["finished_at"],
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

            # 缓存分区
            "CACHE_PARTITION_ENABLED": _parse_bool(db.get("CACHE_PARTITION_ENABLED"), CACHE_PARTITION_ENABLED),
            "CACHE_PARTITION_X":       int(db.get("CACHE_PARTITION_X") or CACHE_PARTITION_X),
            "CACHE_PARTITION_EXTRACT_LIMIT": int(db.get("CACHE_PARTITION_EXTRACT_LIMIT") or CACHE_PARTITION_EXTRACT_LIMIT),
            "CACHE_PARTITION_TRIGGER": db.get("CACHE_PARTITION_TRIGGER") or CACHE_PARTITION_TRIGGER,
            "CACHE_PARTITION_WINDOW":  int(db.get("CACHE_PARTITION_WINDOW") or CACHE_PARTITION_WINDOW),
            "CACHE_PARTITION_KEEP_A_TOOLS": _parse_bool(db.get("CACHE_PARTITION_KEEP_A_TOOLS"), CACHE_PARTITION_KEEP_A_TOOLS),
            "CACHE_SUMMARY_MODEL":     db.get("CACHE_SUMMARY_MODEL") or str(CACHE_SUMMARY_MODEL),

            # 向量搜索（开源版用 EMBEDDING_API_KEY + EMBEDDING_BASE_URL）
            "EMBEDDING_API_KEY":       _mask_key(embedding_key_raw),
            "EMBEDDING_BASE_URL":      db.get("EMBEDDING_BASE_URL") or str(_db_module.EMBEDDING_BASE_URL),
            "EMBEDDING_MODEL":         db.get("EMBEDDING_MODEL") or str(_db_module.EMBEDDING_MODEL),
            "EMBEDDING_DIM":           int(db.get("EMBEDDING_DIM") or _db_module.EMBEDDING_DIM),


            # 其他
            "FORCE_STREAM":       _parse_bool(db.get("FORCE_STREAM"), FORCE_STREAM),
            "RESPONSE_TRANSFORM_ENABLED": _parse_bool(db.get("RESPONSE_TRANSFORM_ENABLED"), RESPONSE_TRANSFORM_ENABLED),
            "RESPONSE_TRANSFORM_RULES": db.get("RESPONSE_TRANSFORM_RULES") or str(RESPONSE_TRANSFORM_RULES),
            "REASONING_EFFORT":   db.get("REASONING_EFFORT") or str(REASONING_EFFORT),
            "USER_NICKNAME":      db.get("USER_NICKNAME") or str(USER_NICKNAME),
            "CHARACTER_NAME":      db.get("CHARACTER_NAME") or str(CHARACTER_NAME),
            "MEMORY_PALACE_DEFAULT_LIMIT": int(db.get("MEMORY_PALACE_DEFAULT_LIMIT") or MEMORY_PALACE_DEFAULT_LIMIT),
            "KEYWORD_CONTEXT_ENABLED": _parse_bool(db.get("KEYWORD_CONTEXT_ENABLED"), KEYWORD_CONTEXT_ENABLED),
            "KEYWORD_CONTEXT_RULES": db.get("KEYWORD_CONTEXT_RULES") or str(KEYWORD_CONTEXT_RULES),

            # System Prompt
            "systemPrompt": db.get("systemPrompt") or _DEFAULT_SYSTEM_PROMPT or "",

            # 记忆提取提示词
            "extractionPrompt": db.get("extractionPrompt") or _DEFAULT_EXTRACTION_PROMPT or "",
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
            "CACHE_PARTITION_ENABLED": lambda v: _parse_bool(v),
            "CACHE_PARTITION_X":     int,
            "CACHE_PARTITION_EXTRACT_LIMIT": int,
            "CACHE_PARTITION_TRIGGER": str,
            "CACHE_PARTITION_WINDOW": int,
            "CACHE_PARTITION_KEEP_A_TOOLS": lambda v: _parse_bool(v),
            "CACHE_SUMMARY_MODEL":   str,
            "FORCE_STREAM":          lambda v: _parse_bool(v),
            "RESPONSE_TRANSFORM_ENABLED": lambda v: _parse_bool(v),
            "RESPONSE_TRANSFORM_RULES": str,
            "REASONING_EFFORT":      str,
            "USER_NICKNAME":         str,
            "CHARACTER_NAME":         str,
            "MEMORY_PALACE_DEFAULT_LIMIT": int,
            "KEYWORD_CONTEXT_ENABLED": lambda v: _parse_bool(v),
            "KEYWORD_CONTEXT_RULES": str,
        }

        # database.py 全局变量映射（开源版用 EMBEDDING_API_KEY + EMBEDDING_BASE_URL）
        _DB_VARS = {
            "EMBEDDING_API_KEY":       str,
            "EMBEDDING_BASE_URL":      str,
            "EMBEDDING_MODEL":         str,
            "EMBEDDING_DIM":           int,
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

            # --- dailyImpressionPrompt 特殊处理 ---
            if key == "dailyImpressionPrompt":
                await set_gateway_config("dailyImpressionPrompt", str(value))
                set_daily_impression_prompt(str(value))
                updated.append("dailyImpressionPrompt")
                continue

            # --- modelPresets 特殊处理 ---
            if key == "modelPresets":
                presets_value = value
                if isinstance(presets_value, list):
                    cleaned_presets = []
                    for p in presets_value:
                        if not isinstance(p, dict):
                            continue
                        cp = dict(p)
                        if _is_masked(str(cp.get("apiKey", ""))):
                            cp.pop("apiKey", None)
                        cleaned_presets.append(cp)
                    presets_json = json.dumps(cleaned_presets, ensure_ascii=False)
                else:
                    presets_json = str(presets_value)
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