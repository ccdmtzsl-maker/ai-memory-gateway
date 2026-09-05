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
import time
import re
import math as _math
import hashlib
import hmac
import base64
from urllib.parse import urlparse, quote
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
from fastapi.middleware.gzip import GZipMiddleware

from database import init_tables, close_pool, save_message, get_pool, get_gateway_config, set_gateway_config, set_gateway_config_many, get_all_gateway_config, get_conversation_messages, get_session_cache_state, save_session_cache_state, delete_session_cache_state, save_token_usage, ensure_token_usage_table, get_conversations_paginated, get_conversation_messages_after_id, delete_conversation, batch_delete_conversations, merge_sessions_to_target, list_all_session_cache_states, export_all_conversations, import_conversations, get_last_user_content, update_last_assistant_message, update_last_assistant_if_same_user, db_row_to_message, search_conversations, update_message_content, rename_session_id, get_conversation_messages_by_date, upsert_daily_impression, get_daily_impression, list_daily_impressions, search_memory_palace_vector_scores, search_memory_palace_vector_scores_multi, memory_palace_vector_ready
from database import list_memory_palace_rooms, list_memory_palace_nodes, get_memory_palace_node, create_memory_palace_node, update_memory_palace_node, delete_memory_palace_node, clear_expired_memory_palace_pins, get_user_impression, upsert_user_impression, delete_user_impression, normalize_user_impression, get_user_activity_meta, upsert_user_activity_meta, delete_user_activity_meta
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
# B 区上限（Y）：B 区攒到多少轮就触发轮转。只在 trigger=rounds 时生效。
# 上下文保留轮数 = X（A区） + Y（B区峰值），所以峰值总量 = X + Y。
# 0 或留空 = 沿用旧行为（Y = X，峰值 2X）。
CACHE_PARTITION_B_LIMIT = int(os.getenv("CACHE_PARTITION_B_LIMIT", "0"))
# 分区自动提取最多处理的最新消息数；先按 cursor 过滤，再只取最新 N 条，过旧积压直接跳过。
CACHE_PARTITION_EXTRACT_LIMIT = int(os.getenv("CACHE_PARTITION_EXTRACT_LIMIT", "120"))
CACHE_SUMMARY_MODEL = os.getenv("CACHE_SUMMARY_MODEL", "anthropic/claude-haiku-4.5")
CACHE_PARTITION_TRIGGER = os.getenv("CACHE_PARTITION_TRIGGER", "rounds")  # rounds=按轮次 | time=按时间窗口
CACHE_PARTITION_WINDOW = int(os.getenv("CACHE_PARTITION_WINDOW", "30"))  # 时间窗口（分钟），仅 trigger=time 时生效
CACHE_PARTITION_KEEP_A_TOOLS = os.getenv("CACHE_PARTITION_KEEP_A_TOOLS", "false").lower() == "true"  # A区是否保留tool/tool_calls
SPARSE_TIMESTAMP_ENABLED = os.getenv("SPARSE_TIMESTAMP_ENABLED", "false").lower() == "true"  # A区无附件消息按间隔稀疏打时间戳
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

# 轻量进程内读缓存：只用于 Dashboard / 记忆管理页这类短期重复读。
# 不用于主聊天历史构造，避免影响强一致的 tool / re-roll 流程。
_READ_CACHE = {}
_READ_CACHE_MAX_ITEMS = 256

def _cache_get(key: str):
    try:
        item = _READ_CACHE.get(str(key))
        if not item:
            return None
        if float(item.get("expires_at") or 0) <= time.time():
            _READ_CACHE.pop(str(key), None)
            return None
        return item.get("value")
    except Exception:
        return None


def _cache_set(key: str, value, ttl: int = 10):
    try:
        key = str(key)
        if len(_READ_CACHE) >= _READ_CACHE_MAX_ITEMS:
            now = time.time()
            expired = [k for k, v in _READ_CACHE.items() if float(v.get("expires_at") or 0) <= now]
            for k in expired:
                _READ_CACHE.pop(k, None)
            if len(_READ_CACHE) >= _READ_CACHE_MAX_ITEMS:
                _READ_CACHE.pop(next(iter(_READ_CACHE)), None)
        _READ_CACHE[key] = {"value": value, "expires_at": time.time() + max(1, int(ttl or 10))}
    except Exception:
        pass
    return value


def _cache_delete_prefix(prefix: str):
    try:
        prefix = str(prefix)
        for key in list(_READ_CACHE.keys()):
            if key.startswith(prefix):
                _READ_CACHE.pop(key, None)
    except Exception:
        pass


def invalidate_daily_impression_cache():
    _cache_delete_prefix("daily:")
    _cache_delete_prefix("prompt_var:daily:")


def invalidate_memory_palace_cache(character_id: str = "default"):
    character_id = character_id or "default"
    _cache_delete_prefix(f"mp:{character_id}:")
    _cache_delete_prefix("mp:stats:")
    _cache_delete_prefix(f"prompt_var:special:{character_id}:")
    _cache_delete_prefix(f"mp:{character_id}:rwn:")


def invalidate_user_impression_prompt_cache(character_id: str = "default"):
    """Clear user impression prompt/dashboard cache after updates."""
    character_id = character_id or "default"
    _cache_delete_prefix(f"prompt_var:user_impression:{character_id}")
    _cache_delete_prefix(f"prompt_var:user_activity_meta:{character_id}")
    _cache_delete_prefix(f"user_activity_meta:{character_id}")
    _cache_delete_prefix(f"user_impression:{character_id}")


# Dashboard 调试：只保留最近一次实际转发给上游模型的请求体。
# 不主动打印，避免日志刷屏；需要时由后台日志页手动查看。
_last_upstream_request_body = None
_last_upstream_request_meta = {}

# Memory Palace 分区自动提取锁：同一角色/会话串行化，避免并发请求重复处理同一批 cursor 区间。
_memory_palace_auto_extract_locks = {}
# 分区后台维护锁：保护 a_start_round 读取/轮转/保存/提取调度，避免同一会话后台任务互相覆盖状态。
_partition_auto_maintenance_locks = {}
# 手动记忆提取状态：防止对话记录/聊天记录提取在未返回结果前重复发起同类请求。
# 用 guard 保护 active 集合，确保“检查忙碌 + 登记忙碌”是原子的。
_memory_palace_manual_extract_active = set()
_memory_palace_manual_extract_guard = asyncio.Lock()

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


def _serialize_dashboard_conversation_message(row) -> dict:
    """Serialize one conversation DB row for Dashboard message list."""
    metadata = None
    raw_metadata = row.get("metadata")
    if raw_metadata:
        try:
            metadata = json.loads(raw_metadata)
        except Exception:
            metadata = raw_metadata
    content = row.get("content")
    if (
        row.get("role") == "assistant"
        and not metadata
        and isinstance(content, str)
        and content.startswith("工具调用:")
    ):
        content = " "
    return {
        "id": row["id"],
        "role": row["role"],
        "content": content,
        "metadata": metadata,
        "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
    }

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
# 记忆宫殿自动注入深度：0=保持现状插在最新消息后；N=向上数 N 条普通对话消息后插入
MEMORY_PALACE_INJECTION_DEPTH = int(os.getenv("MEMORY_PALACE_INJECTION_DEPTH", "0"))

# 关键词触发上下文（轻量世界书）：仅当前轮临时注入 system，不写入历史。
KEYWORD_CONTEXT_ENABLED = os.getenv("KEYWORD_CONTEXT_ENABLED", "false").lower() == "true"
KEYWORD_CONTEXT_RULES = os.getenv("KEYWORD_CONTEXT_RULES", "[]")

# 上下文模板：把用户消息后的多条 system 合并成一条，按模板变量排布
CONTEXT_TEMPLATE_ENABLED = os.getenv("CONTEXT_TEMPLATE_ENABLED", "false").lower() == "true"
CONTEXT_TEMPLATE = os.getenv("CONTEXT_TEMPLATE", "")

# 默认模板：仅在设置页从未保存过内容时作为初始值展示
DEFAULT_CONTEXT_TEMPLATE = (
    "{{env}}" "\n"
    "{{keyword}}" "\n"
    "{{hot_news}}" "\n"
    "{{operit_memory}}" "\n"
    "{{memory_palace}}"
)
MEMORY_PALACE_EVENT_BOX_COMPRESS_THRESHOLD = int(os.getenv("MEMORY_PALACE_EVENT_BOX_COMPRESS_THRESHOLD", "4"))
MEMORY_PALACE_EVENT_BOX_LIVE_HARD_CAP = int(os.getenv("MEMORY_PALACE_EVENT_BOX_LIVE_HARD_CAP", "16"))
# 自动提取的事件盒建盒模式：
#   related = 只采纳 relatedTo（往既有记忆上挂），默认
#   all     = relatedTo + sameAs 都建（和手动导入一致）
#   off     = 只解析并记日志，不建盒
# 默认 related：sameAs 是批内两条新记忆互相配对，一次提取就能凭空开新盒，
# 而压缩阈值只有 4 条活节点，盒子涨太快会频繁触发 LLM 压缩。
MEMORY_PALACE_AUTO_EVENT_BOX_MODE = str(os.getenv("MEMORY_PALACE_AUTO_EVENT_BOX_MODE", "related") or "related").strip().lower()
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


async def get_runtime_memory_palace_injection_depth() -> int:
    """获取记忆宫殿自动注入深度：0=最新消息后；N=向上数 N 条普通对话消息后。"""
    try:
        db_value = await get_gateway_config("MEMORY_PALACE_INJECTION_DEPTH", "")
        if db_value is not None and str(db_value).strip() != "":
            return max(0, min(int(db_value), 50))
    except Exception as e:
        print(f"[memory_config] 读取 MEMORY_PALACE_INJECTION_DEPTH 配置失败，回退到运行时变量: {e}")
    try:
        return max(0, min(int(MEMORY_PALACE_INJECTION_DEPTH or 0), 50))
    except Exception:
        return 0


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


async def get_runtime_context_template_enabled() -> bool:
    """上下文模板开关：优先读设置页配置。"""
    try:
        db_value = await get_gateway_config("CONTEXT_TEMPLATE_ENABLED", None)
        if db_value is not None and str(db_value).strip() != "":
            return _parse_bool(db_value, CONTEXT_TEMPLATE_ENABLED)
    except Exception as e:
        print(f"[context_template] 读取 CONTEXT_TEMPLATE_ENABLED 失败，回退运行时变量: {e}")
    return bool(CONTEXT_TEMPLATE_ENABLED)


async def get_runtime_sparse_timestamp_enabled() -> bool:
    """稀疏时间戳开关：优先读设置页配置。"""
    try:
        db_value = await get_gateway_config("SPARSE_TIMESTAMP_ENABLED", None)
        if db_value is not None and str(db_value).strip() != "":
            return _parse_bool(db_value, SPARSE_TIMESTAMP_ENABLED)
    except Exception as e:
        print(f"[sparse_timestamp] 读取 SPARSE_TIMESTAMP_ENABLED 失败，回退运行时变量: {e}")
    return bool(SPARSE_TIMESTAMP_ENABLED)

async def get_runtime_context_template() -> str:
    """上下文模板内容：优先读设置页配置。"""
    try:
        db_value = await get_gateway_config("CONTEXT_TEMPLATE", "")
        if db_value is not None and str(db_value).strip() != "":
            return str(db_value)
    except Exception as e:
        print(f"[context_template] 读取 CONTEXT_TEMPLATE 失败，回退运行时变量: {e}")
    return str(CONTEXT_TEMPLATE or DEFAULT_CONTEXT_TEMPLATE)

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
                        "CACHE_PARTITION_X": int, "CACHE_PARTITION_B_LIMIT": int, "CACHE_PARTITION_EXTRACT_LIMIT": int, "CACHE_PARTITION_TRIGGER": str,
                        "CACHE_PARTITION_WINDOW": int, "CACHE_PARTITION_KEEP_A_TOOLS": lambda v: _parse_bool(v), "TOOL_CHAIN_DEBUG": lambda v: _parse_bool(v), "CACHE_SUMMARY_MODEL": str,
                        "FORCE_STREAM": lambda v: _parse_bool(v),
                        "RESPONSE_TRANSFORM_ENABLED": lambda v: _parse_bool(v),
                        "PERF_DIAGNOSTIC_ENABLED": lambda v: _parse_bool(v),
                        "RESPONSE_TRANSFORM_RULES": str,
                        "REASONING_EFFORT": str,
                        "MEMORY_PALACE_DEFAULT_LIMIT": int,
                        "MEMORY_PALACE_INJECTION_DEPTH": int,
            "KEYWORD_CONTEXT_ENABLED": lambda v: _parse_bool(v),
            "KEYWORD_CONTEXT_RULES": str,
            "CONTEXT_TEMPLATE_ENABLED": lambda v: _parse_bool(v),
            "CONTEXT_TEMPLATE": str,
            "SPARSE_TIMESTAMP_ENABLED": lambda v: _parse_bool(v),
                    }
                    _RESTORE_DB = {
                        "EMBEDDING_API_KEY": str, "EMBEDDING_BASE_URL": str,
                        "EMBEDDING_MODEL": str, "EMBEDDING_DIM": int,
                    }
                    restored = []
                    for key, val in db_cfg.items():
                        if not val:
                            continue
                        # MEMORY_ENABLED 始终以环境变量为准，不接受 DB 覆盖；并清理历史脏数据
                        if key == "MEMORY_ENABLED":
                            try:
                                _pool = await get_pool()
                                async with _pool.acquire() as _conn:
                                    await _conn.execute("DELETE FROM gateway_config WHERE key = \'MEMORY_ENABLED\'")
                                print("\U0001f9f9 已清理 DB 中的 MEMORY_ENABLED 脏数据（以环境变量为准）")
                            except Exception as _e:
                                print(f"\u26a0\ufe0f  清理 MEMORY_ENABLED 脏数据失败: {_e}")
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
                print(f"🔒 分区缓存已启用: A区X={CACHE_PARTITION_X}, B区Y={_partition_b_limit(CACHE_PARTITION_X)}, 保留峰值={CACHE_PARTITION_X + _partition_b_limit(CACHE_PARTITION_X)}轮, 摘要已架空")
        except Exception as e:
            print(f"⚠️  数据库初始化失败: {e}")
            print("⚠️  记忆系统将不可用，但网关仍可正常转发")
    else:
        print("ℹ️  记忆系统已关闭（设置 MEMORY_ENABLED=true 开启）")
    
    yield
    
    if MEMORY_ENABLED:
        await close_pool()


app = FastAPI(title="AI Memory Gateway", version="2.0.0", lifespan=lifespan)

# Dashboard 性能诊断：只观察容易涉及数据库的页面接口，不修改连接池参数。
_PERF_DIAGNOSTIC_PREFIXES = (
    "/api/conversations",
    "/api/chat/search",
    "/api/chat/messages/",
    "/api/messages/",
    "/api/memory-palace/",
    "/api/daily-impressions",
    "/api/user-impression",
)

# 性能诊断开关：默认关闭，需在设置页面手动开启。
PERF_DIAGNOSTIC_ENABLED = os.getenv("PERF_DIAGNOSTIC_ENABLED", "false").lower() == "true"


def _database_pool_snapshot() -> str:
    """Return a non-blocking asyncpg pool snapshot without acquiring a connection."""
    try:
        pool = getattr(_db_module, "_pool", None)
        if pool is None:
            return "pool=未初始化"
        size = int(pool.get_size())
        idle = int(pool.get_idle_size())
        max_size = int(pool.get_max_size())
        return f"pool={size - idle}/{size}忙, idle={idle}, max={max_size}"
    except Exception as e:
        return f"pool=读取失败({e})"


@app.middleware("http")
async def dashboard_performance_diagnostic_middleware(request: Request, call_next):
    path = request.url.path
    watched = any(path.startswith(prefix) for prefix in _PERF_DIAGNOSTIC_PREFIXES)
    # 后台日志接口必须排除，否则日志页轮询会记录自己。
    if not watched or path.startswith("/api/dashboard/"):
        return await call_next(request)

    if not PERF_DIAGNOSTIC_ENABLED:
        return await call_next(request)

    started = time.perf_counter()
    pool_before = _database_pool_snapshot()
    status_code = 500
    try:
        response = await call_next(request)
        status_code = int(getattr(response, "status_code", 200) or 200)
        return response
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000
        pool_after = _database_pool_snapshot()
        level = "error" if status_code >= 500 else ("run" if elapsed_ms >= 800 else "info")
        query = request.url.query
        target = f"{path}?{query}" if query else path
        add_dashboard_log(
            level,
            f"⏱️ API性能 {request.method} {target} | {elapsed_ms:.0f}ms | HTTP {status_code} | 开始[{pool_before}] | 结束[{pool_after}]",
            category="performance",
        )

# 响应压缩：Dashboard 的 JS/CSS/HTML 都是纯文本，未压缩共 ~260KB，
# 在 Render 免费实例的出口带宽下直接造成白屏等待。gzip 后通常能降到 1/4 左右。
# minimum_size=1000：小于 1KB 的响应不压，避免压缩开销大于收益。
# SSE 流式响应由 Starlette 自行按 chunk 处理，不影响逐字输出。
app.add_middleware(GZipMiddleware, minimum_size=1000)

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
    cache_key = f"prompt_var:daily:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
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
    result = "\n".join(lines)
    _cache_set(cache_key, result, ttl=900)
    return result


async def format_user_impression_for_prompt(character_id: str = "default") -> str:
    """v4.0 用户画像注入：summary/current_state 直接呈现，标签按组分块输出。"""
    character_id = character_id or "default"
    cache_key = f"prompt_var:user_impression:{character_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    item = await get_user_impression(character_id=character_id)
    imp = (item or {}).get("impression") if item else None
    if not imp:
        return ""

    user_name = await get_runtime_user_nickname() or "用户"
    summary = imp.get("summary") or ""
    current_state = imp.get("current_state") or ""
    tags = imp.get("tags") if isinstance(imp.get("tags"), dict) else {}
    changes = imp.get("observed_changes") or []

    TAG_LABELS = {
        "core_values": "核心价值观",
        "likes": "喜好",
        "dislikes": "雷点/反感",
        "money_attitude": "金钱观",
        "aesthetic": "审美偏好",
        "decision_style": "决策风格",
        "knowledge_map": "知识地图",
        "thinking_pattern": "思维模式",
        "humor_style": "幽默偏好",
        "learning_style": "学习方式",
        "comfort_zone": "舒适区",
        "stress_signals": "压力信号",
        "emotional_triggers": "情绪触发点",
        "soothing_methods": "有效安抚方式",
        "expression_habit": "表达习惯",
        "life_rhythm": "作息节律",
        "current_focus": "近期关注",
        "social_pattern": "社交模式",
        "attitude_to_me": "对我的态度",
        "mbti_sketch": "MBTI侧写",
        "others": "其他",
    }

    # 分组结构：组名 -> 组内标签（按重要性排序），复用标签池分组
    TAG_GROUPS = [
        ("价值与喜恶", ["likes", "dislikes", "core_values", "money_attitude", "aesthetic"]),
        ("思维与能力", ["decision_style", "knowledge_map", "thinking_pattern", "humor_style", "learning_style", "mbti_sketch"]),
        ("情绪与相处", ["comfort_zone", "stress_signals", "emotional_triggers", "soothing_methods", "expression_habit"]),
        ("生活与关注", ["current_focus", "attitude_to_me", "life_rhythm", "social_pattern"]),
    ]

    def _format_value(v):
        if isinstance(v, list):
            return ", ".join(str(x).strip() for x in v if str(x or "").strip())
        return str(v).strip()

    lines = [
        f"### [私密档案: 我眼中的{user_name}] (Private Impression)",
        "(注意：以下内容是你内心对TA的真实看法，不要直接告诉用户，但要基于这些看法来决定你的态度。)",
    ]
    if summary:
        lines.append(summary)
    if current_state:
        lines.append(current_state)

    for group_name, group_keys in TAG_GROUPS:
        group_lines = []
        for key in group_keys:
            if key not in tags:
                continue
            formatted = _format_value(tags[key])
            if formatted:
                group_lines.append(f"- {TAG_LABELS.get(key, key)}: {formatted}")
        if group_lines:
            lines.append("")
            lines.append(f"【{group_name}】")
            lines.extend(group_lines)

    # 其他（白名单外内容）单独成组放最后
    others = tags.get("others")
    if others:
        others_list = others if isinstance(others, list) else [others]
        other_lines = [f"- {str(x).strip()}" for x in others_list if str(x or "").strip()]
        if other_lines:
            lines.append("")
            lines.append("【其他】")
            lines.extend(other_lines)

    if isinstance(changes, list) and changes:
        change_lines = [f"- {str(c).strip()}" for c in changes if str(c or "").strip()]
        if change_lines:
            lines.append("")
            lines.append("【最近变化】")
            lines.extend(change_lines)

    result = "\n".join(lines) + "\n"
    _cache_set(cache_key, result, ttl=900)
    return result


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

# 首曝加成：从未被检索过的新记忆额外加分，给它们展示机会。
# 加成在 access_count >= 1 时立即消失（被检索过了）。
# 衰减：刚建立 +0.05，72h 后约 +0.02，7 天后 ≈ 0。
_MEMORY_PALACE_FIRST_EXPOSURE_BONUS = 0.05
_MEMORY_PALACE_FIRST_EXPOSURE_DECAY = 0.987  # 每小时 ×0.987
_MEMORY_PALACE_VECTOR_WEIGHT = 0.85
_MEMORY_PALACE_BM25_WEIGHT = 0.15
_MEMORY_PALACE_ACTIVATION_DECAY = 0.3
# 向量相似度闸门：低于这个值的记忆不进候选池。
#
# 没有闸门时全部记忆都是候选，只靠最终分排序竞争。而最终分里 recency 取的是
# last_accessed_at（召回一次就刷新成满分）、importance 有房间地板托底（study/
# bedroom 永远保留原始值的 90%），于是一条高重要性旧记忆哪怕语义完全不相关，
# 也能凭 imp + recency 反超真正相关的新记忆；而且它一旦进来就刷新 recency，
# 下一轮更容易再进来，形成自我强化的「常驻」。闸门是唯一能切断这个循环的地方——
# 语义不相关就直接出局，不参与后面的分数竞争。
# 暂时关掉：这个 embedding 模型对中文的余弦相似度基线本来就在 0.45-0.55，
# 绝对阈值 0.3 一条都挡不住（不相关的记忆照样 0.50+），先设 0.0 观察真实分布。
_MEMORY_PALACE_VECTOR_MIN_SIM = 0.0
# 每条搜索路的候选池上限。闸门筛掉不相关的之后，向量路和 BM25 路各取前 N 条。
_MEMORY_PALACE_CANDIDATE_POOL = 30
_MEMORY_PALACE_EMOTIONAL_LINK_DIST = 0.35
_MEMORY_PALACE_EMOTIONAL_MIN_MAGNITUDE = 0.2
_MEMORY_PALACE_CO_ACTIVATION_INCREMENT = 0.05

_MEMORY_PALACE_MOOD_TO_VA = {
    "happy": (0.7, 0.5), "sad": (-0.7, -0.5), "angry": (-0.7, 0.8),
    "anxious": (-0.6, 0.7), "tender": (0.6, -0.2), "excited": (0.8, 0.8),
    "peaceful": (0.5, -0.6), "confused": (-0.2, 0.2), "hurt": (-0.7, 0.3),
    "grateful": (0.6, 0.3), "nostalgic": (0.2, -0.3), "neutral": (0.0, 0.0),
    "calm": (0.4, -0.7), "hopeful": (0.6, 0.2),
    "开心": (0.7, 0.5), "难过": (-0.7, -0.5), "悲伤": (-0.7, -0.5),
    "愤怒": (-0.7, 0.8), "焦虑": (-0.6, 0.7), "温柔": (0.6, -0.2),
    "兴奋": (0.8, 0.8), "平静": (0.5, -0.6), "困惑": (-0.2, 0.2),
    "受伤": (-0.7, 0.3), "感激": (0.6, 0.3), "怀念": (0.2, -0.3),
    "中性": (0.0, 0.0), "平和": (0.4, -0.7), "期待": (0.6, 0.2),
}

_MEMORY_PALACE_PERSONALITY_WEIGHTS = {
    "temporal": 0.3,
    "emotional": 1.0,
    "causal": 0.2,
    "person": 0.6,
    "metaphor": 0.5,
}


_BM25_K1 = 1.2
_BM25_B = 0.75


def _memory_palace_bm25_tokenize(text: str) -> list:
    """中文 2-gram + 英文整词分词。
    "小明去了北京" → ["小明", "明去", "去了", "了北", "北京"]
    "hello world" → ["hello", "world"]
    "小明说hello" → ["小明", "明说", "hello"]
    """
    if not text:
        return []
    tokens = []
    parts = re.split(r'([a-zA-Z0-9]+)', (text or "").lower())
    for part in parts:
        trimmed = part.strip()
        if not trimmed:
            continue
        if re.match(r'^[a-zA-Z0-9]+$', trimmed):
            tokens.append(trimmed)
        else:
            # 去掉空白和标点，只留下参与 2-gram 的实义字符。
            # 原来写的 \p{P} 是 PCRE 语法，Python re 不支持，它实际匹配的是
            # 字面量 p / { / } / P，等于没删标点。这里改成显式列举：
            # ASCII 标点 + 间隔号 + 通用标点(\u2000-\u206f，含省略号/破折号/各种引号)
            # + CJK 标点(\u3000-\u303f) + 全角形式(\uff00-\uffef)。
            cleaned = re.sub(r'[\s!-/:-@\[-`{-~\u00b7\u2000-\u206f\u3000-\u303f\uff00-\uffef]', '', trimmed)
            if len(cleaned) == 1:
                tokens.append(cleaned)
            else:
                for i in range(len(cleaned) - 1):
                    tokens.append(cleaned[i:i + 2])
    return tokens


def _memory_palace_build_bm25_index(rows: list) -> dict:
    """把一批记忆节点切词、数词频，做成可反复使用的索引。

    这一步跟「查什么」完全无关，只跟记忆本身有关，所以一批 rows 只需要
    做一次。以前每一路搜索都从头切一遍：255 条记忆里，切词占单路耗时的
    七成半，4 路搜索就白切 3 遍。

    df（每个词出现在几篇文档里）不在这里预算：一篇记忆有上百个词，全量数
    一遍比查询实际用到的那十几个词贵得多，单路检索会反而变慢。这里只留一个
    空缓存，谁用到哪个词就数哪个，数过的下一路直接查表。
    """
    doc_tf = []
    doc_len = []
    ids = []
    for row in rows or []:
        content = row.get("content") or ""
        tags = row.get("tags") or ""
        toks = _memory_palace_bm25_tokenize((content + " " + tags).lower())
        tf = {}
        for t in toks:
            tf[t] = tf.get(t, 0) + 1
        doc_tf.append(tf)
        doc_len.append(len(toks))
        ids.append(row.get("id"))
    doc_count = len(doc_tf)
    return {
        "ids": ids,
        "doc_tf": doc_tf,
        "doc_len": doc_len,
        "df_cache": {},
        "doc_count": doc_count,
        "avg_dl": (sum(doc_len) / doc_count) if doc_count else 0.0,
    }


def _memory_palace_bm25_scores(query: str, rows: list, index: dict = None) -> dict:
    """对候选记忆节点计算 BM25 分数，返回 {memory_id: score} 字典。
    分数归一化到 0-1 范围（除以最大分）。

    index 是 _memory_palace_build_bm25_index 的产物。多路搜索共用同一批
    rows 时传进来，避免重复切词。没传就现场建一个，行为和以前一致。
    """
    if not rows or not query:
        return {}
    query_tokens = _memory_palace_bm25_tokenize(query)
    if not query_tokens:
        return {}
    # 索引和 rows 必须是同一批数据，长度对不上就宁可重建，不能错位打分
    if not index or index.get("doc_count") != len(rows) or "df_cache" not in index:
        index = _memory_palace_build_bm25_index(rows)
    doc_tf = index["doc_tf"]
    doc_len = index["doc_len"]
    ids = index["ids"]
    df_cache = index["df_cache"]
    doc_count = index["doc_count"]
    avg_dl = index["avg_dl"]
    if avg_dl == 0:
        return {}
    unique_qtokens = list(set(query_tokens))
    idf = {}
    for qt in unique_qtokens:
        df = df_cache.get(qt)
        if df is None:
            df = sum(1 for tf in doc_tf if qt in tf)
            df_cache[qt] = df
        idf[qt] = _math.log((doc_count - df + 0.5) / (df + 0.5) + 1)
    scores = {}
    coverage = {}
    qt_total = len(unique_qtokens)
    for i in range(doc_count):
        dl = doc_len[i]
        if dl == 0:
            continue
        score = 0.0
        matched = 0
        tf_map = doc_tf[i]
        for qt in unique_qtokens:
            tf = tf_map.get(qt, 0)
            if tf == 0:
                continue
            matched += 1
            tf_norm = (tf * (_BM25_K1 + 1)) / (tf + _BM25_K1 * (1 - _BM25_B + _BM25_B * dl / avg_dl))
            score += idf[qt] * tf_norm
        if score > 0:
            scores[ids[i]] = score
            coverage[ids[i]] = (matched / qt_total) if qt_total else 0.0
    # 归一化到 0-1，再乘查询词覆盖率。
    #
    # 只除以本轮最高分是相对分：只要有任何一条记忆匹配上任何一个词，最高的那条
    # 就必然拿满分 1.0——哪怕它命中的只是「今天」这种废词。BM25 权重 0.15，等于
    # 白送 0.15，足够让一条毫不相关的高重要性旧记忆反超真正相关的新记忆。
    #
    # 覆盖率 = 命中的查询词数 / 查询词总数。5 个词里只中 1 个，上限就是 0.2。
    # 这样「相对排序」由 BM25 负责，「匹配到底有多充分」由覆盖率兜住。
    max_score = max(scores.values()) if scores else 0.0
    if max_score > 0:
        scores = {k: (v / max_score) * coverage.get(k, 0.0) for k, v in scores.items()}
    return scores


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
    """把各种形态的时间统一成带时区的 datetime。

    必须接受字符串。预览导入链路上 pinned_until 会先被序列化成 ISO 字符串
    发给前端，再原样传回来；只处理 datetime/date 的话这里会走
    value.replace(tzinfo=...) 分支，而 str.replace 不接受关键字参数，
    抛出的 TypeError 被 except 吞掉返回 None——手动提取的便利贴就这么丢了。
    """
    if not value:
        return None
    try:
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                value = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except Exception:
                try:
                    value = datetime.strptime(text[:10], "%Y-%m-%d")
                except Exception:
                    return None
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


def _memory_palace_first_exposure_bonus(row) -> float:
    """首曝加成：access_count == 0 的记忆按建立时间衰减加分。

    目的：新记忆从未被召回过，在公式里对比老记忆有结构性劣势
    （老记忆有 familiarity 加成、有 recency 因为被召回过而刷新）。
    首曝加成让新记忆回到同一起跑线。被检索过一次后加成消失。
    """
    try:
        if int(row.get("access_count") or 0) >= 1:
            return 0.0
        dt = _memory_palace_aware_dt(row.get("created_at"))
        if not dt:
            return _MEMORY_PALACE_FIRST_EXPOSURE_BONUS
        hours = max(0.0, (datetime.now(timezone.utc) - dt).total_seconds() / 3600)
        return _MEMORY_PALACE_FIRST_EXPOSURE_BONUS * (_MEMORY_PALACE_FIRST_EXPOSURE_DECAY ** hours)
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
        return normalize_stored_content_for_text(content)
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
    # update 模式用 SQL 过滤 id > last_consumed_node_id 只取新增，每个房间仍用 room_limits 控制上限（总计70）

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


def _memory_palace_score_rows(rows, query: str, query_embedding=None, discount: float = 1.0,
                              vector_scores=None, bm25_index=None, explain: bool = False,
                              apply_gate: bool = True):
    """给候选记忆打分排序。

    vector_scores 是数据库算好的 {memory_id: 余弦相似度}。有它就直接查表，
    不用再把 embedding_json 解析成 1024 个浮点数、也不用在 Python 里跑
    余弦循环——那条路 130 个节点约 90ms，而且是纯计算，期间事件循环
    完全被占住（日志里出现过 1087ms 阻塞）。

    没查到的节点仍然走 Python 回退：可能是 pgvector 列还没回填，或者
    维度和查询向量对不上。宁可慢一点，也不能让某条记忆凭空拿 0 分。
    """
    scored = []
    query = (query or "").strip()
    vector_scores = vector_scores or {}
    bm25_scores = _memory_palace_bm25_scores(query, rows, index=bm25_index) if query else {}

    # 候选池闸门：两条路各自筛一遍，取并集。
    #
    # 向量路：相似度 >= 闸门值，按相似度取前 N。
    # BM25 路：有关键词命中的，按 BM25 分取前 N。
    #
    # 两路取并集而不是交集：专有名词（人名、作品名）向量常常抓不住，但 BM25
    # 能精确命中；反过来同义改写（过年/春节）BM25 抓不住而向量能抓住。任一路
    # 认可就放进来，都不认可才出局。
    #
    # 无 query（纯浏览）或显式关闭时不设闸门：这时没有「相关」可言，全量参与排序。
    gate_ids = None
    if apply_gate and query:
        gate_ids = set()
        if query_embedding:
            vec_pairs = []
            for row in rows:
                vs = vector_scores.get(row["id"])
                if vs is None and row["embedding_json"]:
                    try:
                        vs = _memory_palace_cosine(query_embedding, json.loads(row["embedding_json"]))
                    except Exception:
                        vs = None
                if vs is not None and float(vs) >= _MEMORY_PALACE_VECTOR_MIN_SIM:
                    vec_pairs.append((row["id"], float(vs)))
            vec_pairs.sort(key=lambda x: x[1], reverse=True)
            gate_ids.update(i for i, _v in vec_pairs[:_MEMORY_PALACE_CANDIDATE_POOL])
        bm_pairs = sorted(
            ((k, v) for k, v in bm25_scores.items() if v > 0),
            key=lambda x: x[1], reverse=True,
        )
        gate_ids.update(i for i, _v in bm_pairs[:_MEMORY_PALACE_CANDIDATE_POOL])
        # 两路都空：这一路检索确实没有相关记忆，返回空比返回一堆不相关的更好。
        if not gate_ids:
            return []

    for row in rows:
        if gate_ids is not None and row["id"] not in gate_ids:
            continue
        content = row["content"] or ""
        tags = row["tags"] or ""
        vector_score = 0.0
        if query_embedding:
            db_score = vector_scores.get(row["id"])
            if db_score is not None:
                vector_score = float(db_score)
            elif row["embedding_json"]:
                try:
                    vector_score = _memory_palace_cosine(query_embedding, json.loads(row["embedding_json"]))
                except Exception:
                    vector_score = 0.0
        keyword_score = bm25_scores.get(row["id"], 0.0)
        if query_embedding:
            similarity = _MEMORY_PALACE_VECTOR_WEIGHT * vector_score + _MEMORY_PALACE_BM25_WEIGHT * keyword_score
        elif query:
            similarity = keyword_score
        else:
            similarity = 0.5
        room_id = row["room"] or "living_room"
        weights = dict(_MEMORY_PALACE_ROOM_WEIGHTS.get(room_id, _MEMORY_PALACE_ROOM_WEIGHTS["living_room"]))
        recency = _memory_palace_recency_score(row["last_accessed_at"] or row["created_at"])
        recency_redistributed = False
        if recency < 0.1 and weights["recency"] > 0:
            shift = weights["recency"] / 2
            weights["similarity"] += shift
            weights["importance"] += shift
            weights["recency"] = 0.0
            recency_redistributed = True
        importance = max(0.0, min(1.0, _memory_palace_effective_importance(row) / 10.0))
        familiarity = _memory_palace_familiarity_bonus(row["access_count"])
        first_exposure = _memory_palace_first_exposure_bonus(row)
        final_score = (
            weights["similarity"] * similarity +
            weights["recency"] * recency +
            weights["importance"] * importance +
            familiarity +
            first_exposure
        ) * discount
        item = dict(row)
        item["score"] = final_score
        item["similarity_score"] = similarity
        if explain:
            # 分数拆解，只给召回调试用。不带这个开关时不算，免得每轮检索白攒字典。
            item["score_explain"] = {
                "vector": round(vector_score, 4),
                "bm25": round(keyword_score, 4),
                "similarity": round(similarity, 4),
                "recency": round(recency, 4),
                "importance": round(importance, 4),
                "familiarity_bonus": round(familiarity, 4),
                "first_exposure_bonus": round(first_exposure, 4),
                "weights": {
                    "similarity": round(weights["similarity"], 4),
                    "recency": round(weights["recency"], 4),
                    "importance": round(weights["importance"], 4),
                },
                "parts": {
                    "similarity": round(weights["similarity"] * similarity, 4),
                    "recency": round(weights["recency"] * recency, 4),
                    "importance": round(weights["importance"] * importance, 4),
                    "familiarity": round(familiarity, 4),
                    "first_exposure": round(first_exposure, 4),
                },
                "recency_redistributed": recency_redistributed,
                "discount": round(discount, 4),
                "final": round(final_score, 4),
            }
        scored.append(item)
    scored.sort(key=lambda x: x["score"], reverse=True)
    return scored


async def search_memory_palace_for_prompt(query: str = "", limit: int = 5, room: str = None, character_id: str = "default", rows=None, bm25_index=None, query_embedding=None, vector_scores=None, explain: bool = False):
    """单路检索。

    query_embedding 已经算好时直接用，不再自己发请求：一轮检索有好几路，
    调用方会把所有路的查询文本一次性批量向量化，省掉逐路的网络往返。
    """
    limit = max(1, min(int(limit or 5), 30))
    query = (query or "").strip()
    if query and not query_embedding:
        try:
            query_embedding = await compute_memory_palace_embedding(query)
        except Exception as e:
            print(f"⚠️ Memory Palace query embedding failed: {e}")
            query_embedding = None
    rows = rows if rows is not None else await _memory_palace_fetch_rows(room=room, character_id=character_id)

    # 相似度交给数据库算（pgvector）。失败或不可用时返回空字典，
    # 打分函数会自动退回 Python 计算，结果一致只是慢一些。
    # vector_scores 已经算好时直接用：一轮检索有好几路，调用方会用一条 SQL
    # 把所有路的相似度一起算完，省掉逐路的数据库往返。
    if vector_scores is None:
        vector_scores = {}
        if query_embedding:
            try:
                vector_scores = await search_memory_palace_vector_scores(
                    query_embedding, character_id=character_id, room=room,
                )
            except Exception as e:
                print(f"ℹ️ pgvector 检索失败，回退 Python 计算: {str(e)[:120]}")
                vector_scores = {}

    return _memory_palace_score_rows(
        rows, query=query, query_embedding=query_embedding,
        vector_scores=vector_scores, bm25_index=bm25_index, explain=explain,
    )[:limit]


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


async def _memory_palace_spread_activation(selected, rows, character_id: str = "default", max_expand: int = 3, explain: bool = False):
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
    activation_detail = {}
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
        strength = float(link["strength"] or 0.0)
        base_score = seed_score.get(base_id, 0.0)
        score = base_score * strength * type_weight * _MEMORY_PALACE_ACTIVATION_DECAY
        if score > activated.get(neighbor_id, 0.0):
            activated[neighbor_id] = score
            if explain:
                activation_detail[neighbor_id] = {
                    "from_id": base_id,
                    "link_type": link["link_type"],
                    "link_strength": round(strength, 4),
                    "type_weight": type_weight,
                    "decay": _MEMORY_PALACE_ACTIVATION_DECAY,
                    "seed_score": round(base_score, 4),
                    "activation_score": round(score, 4),
                }
    expanded = []
    for node_id, score in sorted(activated.items(), key=lambda x: x[1], reverse=True)[:max_expand]:
        item = dict(row_map[node_id])
        item["score"] = score
        item["similarity_score"] = 0.0
        item["activation"] = True
        if explain:
            item["_hit_path"] = "activation"
            item["activation_explain"] = activation_detail.get(node_id)
        expanded.append(item)
    return selected + expanded


async def _memory_palace_strengthen_coactivated(node_ids, character_id: str = "default"):
    """共激活强化：检查已有关联，有则加强已有的那条（任意类型），没有才新建时间关联。"""
    node_ids = list(dict.fromkeys(node_ids))[:5]
    if len(node_ids) < 2:
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        for i in range(len(node_ids)):
            for j in range(i + 1, len(node_ids)):
                source_id, target_id = node_ids[i], node_ids[j]
                existing = await conn.fetchrow("""
                    SELECT id, link_type, strength FROM memory_palace_links
                    WHERE character_id = $1
                      AND ((source_id = $2 AND target_id = $3) OR (source_id = $3 AND target_id = $2))
                    LIMIT 1
                """, character_id, source_id, target_id)
                if existing:
                    # 参数编号必须从 $1 开始连续。原来写成 $2/$3 但只传两个参数，
                    # $1 悬空，asyncpg 无法推断它的类型，每轮都报
                    # "could not determine data type of parameter $1"，
                    # 于是共激活强化整段被异常吞掉，从未真正生效过。
                    await conn.execute("""
                        UPDATE memory_palace_links
                        SET strength = LEAST(1.0, strength + $1), updated_at = NOW()
                        WHERE id = $2
                    """, _MEMORY_PALACE_CO_ACTIVATION_INCREMENT, existing['id'])
                else:
                    await conn.execute("""
                        INSERT INTO memory_palace_links (id, character_id, source_id, target_id, link_type, strength, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, 'temporal', $5, NOW(), NOW())
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

# 召回调试用的临时暂存：explain 模式下把本轮全语料向量分留下来给接口读。
# 只在调试接口的同一次请求内使用，不参与正常聊天链路。
_memory_palace_last_explain_corpus = {}


async def retrieve_memory_palace_rows_for_prompt(query: str = "", limit: int = 5, room: str = None, character_id: str = "default", recent_messages=None, touch_access: bool = True, explain: bool = False):
    limit = max(1, min(int(limit or 5), 30))
    await clear_expired_memory_palace_pins(character_id)
    rows = await _memory_palace_fetch_rows(room=room, character_id=character_id)
    # 一轮检索会分成好几路（每个用户消息片段一路 + 上下文一路）。切词只跟
    # 记忆本身有关、跟查什么无关，所以整轮只切一次，所有路共用。
    bm25_index = _memory_palace_build_bm25_index(rows)
    merged = {}
    spikes, context_query, fallback_query = _memory_palace_split_last_turn_queries(recent_messages or [])
    if not spikes and query:
        spikes = [{"label": "q", "text": query.strip()}]
    # 所有路的查询文本一次性向量化。以前每路各发一次请求，3-4 路就是 3-4 个
    # 网络来回；现在一个来回拿齐。批量失败会自动退回逐条，不影响可用性。
    if spikes:
        batch_texts = [s["text"] for s in spikes] + ([context_query] if context_query else [])
    else:
        batch_texts = [fallback_query or query]
    try:
        batch_embeds = await compute_memory_palace_embeddings(batch_texts)
    except Exception as e:
        print(f"⚠️ Memory Palace 批量向量化失败，改为逐条: {e}")
        batch_embeds = [None] * len(batch_texts)
    # 所有路的向量相似度也一条 SQL 算完。以前每路各查一次数据库，
    # 现在 unnest 把几个查询向量当成一张小表，一趟连接算齐。
    batch_scores = [None] * len(batch_texts)
    if any(batch_embeds):
        try:
            batch_scores = await search_memory_palace_vector_scores_multi(
                batch_embeds, character_id=character_id, room=room,
            )
        except Exception as e:
            print(f"ℹ️ pgvector 批量检索失败，逐路回退: {str(e)[:120]}")
            batch_scores = [None] * len(batch_texts)
    pool_limit = _MEMORY_PALACE_CANDIDATE_POOL
    if spikes:
        for pos, spike in enumerate(spikes):
            results = await search_memory_palace_for_prompt(spike["text"], limit=pool_limit, room=room, character_id=character_id, rows=rows, bm25_index=bm25_index, query_embedding=batch_embeds[pos] or None, vector_scores=batch_scores[pos], explain=explain)
            for item in results:
                if explain:
                    item = dict(item)
                    item["_hit_path"] = spike.get("label") or f"spike{pos}"
                    item["_hit_query"] = spike["text"]
                prev = merged.get(item["id"])
                if prev is None or item["score"] > prev["score"]:
                    merged[item["id"]] = item
        if context_query:
            ctx_results = await search_memory_palace_for_prompt(context_query, limit=pool_limit, room=room, character_id=character_id, rows=rows, bm25_index=bm25_index, query_embedding=batch_embeds[len(spikes)] or None, vector_scores=batch_scores[len(spikes)], explain=explain)
            for item in ctx_results:
                item = dict(item)
                item["score"] *= 0.5
                if explain:
                    item["_hit_path"] = "context"
                    item["_hit_query"] = context_query
                    item["_context_discount"] = 0.5
                prev = merged.get(item["id"])
                if prev is None or item["score"] > prev["score"]:
                    merged[item["id"]] = item
    else:
        fallback = fallback_query or query
        for item in await search_memory_palace_for_prompt(fallback, limit=pool_limit, room=room, character_id=character_id, rows=rows, bm25_index=bm25_index, query_embedding=batch_embeds[0] or None, vector_scores=batch_scores[0], explain=explain):
            if explain:
                item = dict(item)
                item["_hit_path"] = "fallback"
                item["_hit_query"] = fallback
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
                        if explain:
                            existing["_date_boost"] = 0.3
                    else:
                        item["score"] = 0.8
                        item["similarity_score"] = 0.0
                        if explain:
                            item["_hit_path"] = "date"
                            item["_date_only"] = True
                        merged[item["id"]] = item
                    break
    selected = sorted(merged.values(), key=lambda x: x["score"], reverse=True)[:limit]
    try:
        selected = await _memory_palace_spread_activation(selected, rows, character_id=character_id, max_expand=3, explain=explain)
    except Exception as e:
        print(f"⚠️ Memory Palace spread activation failed: {e}")
    now = datetime.now(timezone.utc)
    pinned = []
    for row in rows:
        pu = _memory_palace_aware_dt(row["pinned_until"])
        if pu and pu > now:
            item = dict(row)
            item["score"] = 999.0
            if explain:
                item["_hit_path"] = "pinned"
            pinned.append(item)
    pinned.sort(key=lambda x: x["pinned_until"] or now)
    pinned_ids = {x["id"] for x in pinned}
    selected = [x for x in selected if x.get("id") not in pinned_ids]
    final_rows = pinned + selected
    if explain:
        # 全语料向量分（各路取最高）。调试面板要用它算分布和百分位：只看返回的
        # 那几条会严重低估极差——它们本来就是分数最高的一撮。
        corpus_vec = {}
        for sc in (batch_scores or []):
            for mid, val in (sc or {}).items():
                v = float(val)
                if v > corpus_vec.get(mid, -1.0):
                    corpus_vec[mid] = v
        _memory_palace_last_explain_corpus["vector_scores"] = corpus_vec
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


# 同一轮注入的 receipts 是一次 executemany 写进去的，NOW() 取事务开始时间，
# 所以同轮的 injected_at 完全相同。留 5 秒余量兜住极端情况。
_MEMORY_PALACE_RECALL_ROUND_GAP_SECONDS = 5
# receipts 少于这个数才启用语义检索兜底。不为凑数启动：
# 6 条干净的真实召回，胜过 20 条混着系统猜测的材料。
_MEMORY_PALACE_RECALL_MIN_REFS = 5


def _memory_palace_source_message_id_bounds(source_messages: list) -> tuple:
    """待提取消息 id 范围。

    新版 receipts 写入 anchor_message_id：记忆注入发生时，该会话已落库的
    最后一条消息 id。提取某个消息区间时，用这个 id 范围精确找回当时
    实际注入过的记忆；历史没有 anchor 的记录才回退时间窗。
    """
    ids = []
    for msg in source_messages or []:
        try:
            mid = msg.get("id") if hasattr(msg, "get") else msg["id"]
        except Exception:
            mid = None
        try:
            mid = int(mid)
        except Exception:
            continue
        if mid > 0:
            ids.append(mid)
    if not ids:
        return None, None
    return min(ids), max(ids)


def _memory_palace_source_session_ids(source_messages: list) -> list:
    """待提取消息属于哪些会话。用来隔离 receipts，避免别的会话串味。"""
    ids = []
    for msg in source_messages or []:
        try:
            sid = msg.get("session_id") if hasattr(msg, "get") else msg["session_id"]
        except Exception:
            continue
        sid = str(sid or "").strip()
        if sid and sid not in ids:
            ids.append(sid)
    return ids


def _memory_palace_group_receipts_by_round(rows: list, gap_seconds: int = None) -> list:
    """把 receipts 聚成「轮」。一轮 = 一次记忆注入 = 一轮对话。

    新数据优先按 anchor_message_id 分组：同一轮注入发生在保存本轮 user / assistant
    之前，因此 anchor_message_id 相同。历史数据没有 anchor 时，才按 injected_at
    间隔近似聚类。
    """
    gap = float(gap_seconds if gap_seconds is not None else _MEMORY_PALACE_RECALL_ROUND_GAP_SECONDS)
    rounds = []
    current = []
    last_at = None
    last_anchor = object()
    for row in rows or []:
        anchor = row.get("anchor_message_id") if hasattr(row, "get") else None
        at = _memory_palace_aware_dt(row.get("injected_at") if hasattr(row, "get") else None)
        split = False
        if current:
            if anchor is not None or last_anchor is not None:
                split = anchor != last_anchor
            elif last_at is not None and at is not None:
                split = (at - last_at).total_seconds() > gap
        if split:
            rounds.append(current)
            current = []
        current.append(row)
        last_anchor = anchor
        if at is not None:
            last_at = at
    if current:
        rounds.append(current)
    return rounds


def _memory_palace_round_robin_receipts(rows: list, limit: int) -> list:
    """按轮次转圈取样。

    为什么不直接按时间倒序取前 N 条：一批自动提取最多吃 120 条消息、可能
    横跨十几轮，倒序取只会拿到最后两三轮，前面每一轮当时想起过什么全丢。
    转圈取样让每一轮都有代表。

    同一条记忆在多轮里反复出现是常态（高重要记忆连续被召回），按 memory_id
    全局去重，重复的跳过、不占名额，腾出的名额自动流向别的轮次。
    """
    if limit <= 0:
        return []
    rounds = _memory_palace_group_receipts_by_round(rows)
    if not rounds:
        return []
    # 轮数超过名额时，均匀抽取轮次而不是只要前面几轮，保证覆盖整个时间跨度。
    sampled = _memory_palace_sample_evenly(list(range(len(rounds))), limit) if len(rounds) > limit else list(range(len(rounds)))
    # 一条记忆出现在几轮里。名额被上限截断时用它决胜：反复出现说明是这段
    # 对话的持续主题。只在截断时生效，否则会压倒「每轮都有代表」的目标。
    appearances = {}
    for idx in sampled:
        for mid in {r["id"] for r in rounds[idx]}:
            appearances[mid] = appearances.get(mid, 0) + 1
    # 轮内顺序：receipts 是按 score 顺序（便利贴→高分→扩散）写入的自增主键，
    # 所以 receipt_id 升序就是当时的重要度排名，不需要额外存分数。
    cursors = {idx: 0 for idx in sampled}
    active = list(sampled)
    claimed = set()
    picked = []
    while active and len(picked) < limit:
        pass_rows = []
        still = []
        for idx in active:
            rnd = rounds[idx]
            pos = cursors[idx]
            while pos < len(rnd) and rnd[pos]["id"] in claimed:
                pos += 1
            cursors[idx] = pos
            if pos >= len(rnd):
                continue
            row = rnd[pos]
            cursors[idx] = pos + 1
            claimed.add(row["id"])
            pass_rows.append((idx, row))
            still.append(idx)
        if not pass_rows:
            break
        if len(picked) + len(pass_rows) > limit:
            pass_rows.sort(key=lambda x: appearances.get(x[1]["id"], 0), reverse=True)
        picked.extend(pass_rows[:max(0, limit - len(picked))])
        active = still
    # 输出按时间顺序，模型读起来符合对话推进的直觉。
    picked.sort(key=lambda x: (x[0], x[1].get("receipt_id") or 0))
    refs = []
    for _idx, row in picked:
        # 不截断：重要记忆常把铺垫放前面、重点放后面，截断正好切掉重点。
        # 只把换行折叠成空格，保住 "O0. 内容" 的单行编号格式。
        content = re.sub(r"\s+", " ", str(row.get("content") or "")).strip()
        if not content:
            continue
        refs.append({
            "id": row["id"],
            "room": row.get("room") or "living_room",
            "content": content,
            "_source": "recall",
            "_rounds": appearances.get(row["id"], 1),
        })
    return refs


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


# 召回记账保留期。过期记录清掉：它们只在自动提取时用来重建「当时想起过什么」，
# 而提取通常在几轮对话内就会发生，30 天足够宽裕。
MEMORY_PALACE_RECEIPT_RETENTION_DAYS = int(os.getenv("MEMORY_PALACE_RECEIPT_RETENTION_DAYS", "30"))
# 清理节流：进程内记录上次清理时间，最多一天跑一次。
_memory_palace_receipt_cleanup_at = None


async def cleanup_memory_palace_recall_receipts(days: int = None) -> int:
    """删除超过保留期的召回记账。

    取舍：如果某个会话超过保留期都没触发过自动提取，它的记账会先被清掉，
    提取时 receipts 不足会退回语义检索兜底——降级而非报错。相比让这张表
    无限增长，这个代价是可接受的。
    """
    keep_days = max(1, int(days if days is not None else MEMORY_PALACE_RECEIPT_RETENTION_DAYS))
    pool = await get_pool()
    async with pool.acquire() as conn:
        deleted = await conn.fetchval(
            """
            WITH removed AS (
                DELETE FROM memory_palace_recall_receipts
                WHERE injected_at < NOW() - ($1 || ' days')::interval
                RETURNING 1
            )
            SELECT COUNT(*)::int FROM removed
            """,
            str(keep_days),
        )
    return int(deleted or 0)


async def _maybe_cleanup_memory_palace_recall_receipts():
    """每天最多跑一次清理，失败只打日志不影响聊天。"""
    global _memory_palace_receipt_cleanup_at
    now = datetime.now(timezone.utc)
    last = _memory_palace_receipt_cleanup_at
    if last is not None and (now - last).total_seconds() < 86400:
        return
    _memory_palace_receipt_cleanup_at = now
    try:
        deleted = await cleanup_memory_palace_recall_receipts()
        if deleted:
            print(f"🧹 清理过期召回记账 {deleted} 条（保留 {MEMORY_PALACE_RECEIPT_RETENTION_DAYS} 天）")
    except Exception as e:
        print(f"⚠️ 清理召回记账失败: {e}")


async def get_conversation_last_message_id(session_id: str) -> int:
    """返回当前会话已落库的最后一条消息 id。

    记忆注入发生在本轮 user/assistant 保存之前，所以这个 id 是“锚点”：
    后续自动提取某个消息区间时，可以用 anchor_message_id 精确找回
    这几轮对话当时实际注入过的记忆，而不再靠 injected_at 时间窗猜。
    """
    sid = str(session_id or "").strip()
    if not sid:
        return 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        value = await conn.fetchval(
            "SELECT MAX(id)::bigint FROM conversations WHERE session_id = $1",
            sid,
        )
    return int(value or 0)


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
    anchor_id = await get_conversation_last_message_id(session_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        # 不做写入去重：每轮注入都如实记一组 receipts。
        # 重复记账是信息，不是脏数据：同一条记忆连续多轮出现，说明它是这段
        # 对话的持续主题。后续取样会按 memory_id 去重，重复不会浪费 prompt 名额。
        await conn.executemany(
            """
            INSERT INTO memory_palace_recall_receipts
                (character_id, session_id, memory_id, anchor_message_id, injected_at, metadata)
            VALUES ($1, $2, $3, $4, NOW(), '{}'::jsonb)
            """,
            [(character_id, session_id or "", memory_id, anchor_id) for memory_id in ids],
        )
    # 后台清理，不让聊天请求等它。
    try:
        asyncio.create_task(_maybe_cleanup_memory_palace_recall_receipts())
    except Exception:
        pass
    return len(ids)


async def get_memory_palace_receipt_refs(source_messages: list, character_id: str = "default", limit: int = 20) -> list:
    """重建这批消息当时的真实召回状态：那几轮对话实际注入过哪些记忆。

    新数据优先按 anchor_message_id 查：这比 injected_at 时间窗精确，能避免
    同时段其它会话串味，也不怕同一句话删了重发后时间戳很近。历史记录没有
    anchor_message_id 时，才回退到时间窗查询。
    """
    start_id, end_id = _memory_palace_source_message_id_bounds(source_messages)
    start_at, end_at = _memory_palace_source_time_bounds(source_messages, tolerance_minutes=10)
    if (not start_id or not end_id) and (not start_at or not end_at):
        return []
    limit = max(0, min(int(limit or 20), 50))
    if limit <= 0:
        return []
    session_ids = _memory_palace_source_session_ids(source_messages)
    pool = await get_pool()
    async with pool.acquire() as conn:
        # 不做 DISTINCT ON：轮次结构要保留，去重交给转圈取样按 memory_id 处理。
        # 新记录走 anchor_message_id；历史记录 anchor 为空时用时间窗兜底。
        rows = await conn.fetch(
            """
            SELECT r.id AS receipt_id, r.injected_at, r.anchor_message_id,
                   n.id, n.room, n.content
            FROM memory_palace_recall_receipts r
            JOIN memory_palace_nodes n ON n.id = r.memory_id
            WHERE r.character_id = $1
              AND n.character_id = $1
              AND n.archived = FALSE
              AND ($6::text[] IS NULL OR r.session_id = ANY($6::text[]))
              AND (
                    ($2::bigint IS NOT NULL AND $3::bigint IS NOT NULL
                     AND r.anchor_message_id >= ($2::bigint - 1)
                     AND r.anchor_message_id < $3::bigint)
                    OR
                    (r.anchor_message_id IS NULL
                     AND $4::timestamptz IS NOT NULL AND $5::timestamptz IS NOT NULL
                     AND r.injected_at >= $4::timestamptz
                     AND r.injected_at <= $5::timestamptz)
              )
            ORDER BY r.anchor_message_id ASC NULLS LAST, r.injected_at ASC, r.id ASC
            """,
            character_id, start_id, end_id, start_at, end_at, (session_ids or None),
        )
    rows = [dict(r) for r in rows]
    refs = _memory_palace_round_robin_receipts(rows, limit)
    if refs:
        rounds = len(_memory_palace_group_receipts_by_round(rows))
        anchored = sum(1 for r in rows if r.get("anchor_message_id") is not None)
        print(f"🧾 记忆宫殿真实召回：{rounds} 轮 / {len(rows)} 条记账（anchor {anchored}）→ {len(refs)} 条参考")
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
            # 之前这里直接写死日期，只有事件盒里的片段走了 friendly_date，
            # 所以「今天/昨天」看起来像没生效——普通记忆走的就是这一行。
            date_text = _memory_palace_friendly_date(row)
            tags = (row["tags"] or "").strip()
            meta = f"{date_text}｜重要性:{row['importance'] or 5}｜情绪:{row['mood'] or 'neutral'}"
            if tags:
                meta += f"｜标签:{tags}"
            lines.append(f"- {meta}\n  {str(row['content'] or '').strip()}")
    return "\n".join(lines)


async def format_special_memory_for_prompt(limit: int = 3, character_id: str = "default") -> str:
    """Format resident self-insight memories for {{Special_Memory}}.

    This is intentionally short: it only lists stable self cognition produced by
    cognitive digestion, suitable for manually placing inside the character prompt.
    """
    try:
        limit = max(1, min(int(limit or 3), 8))
    except Exception:
        limit = 3
    cache_key = f"prompt_var:special:{character_id or 'default'}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    if not await get_runtime_memory_palace_enabled():
        return ""
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT content, importance, created_at
                FROM memory_palace_nodes
                WHERE character_id = $1
                  AND archived = FALSE
                  AND room = 'self_room'
                  AND (
                    COALESCE(tags, '') ILIKE '%常驻%'
                    OR COALESCE(tags, '') ILIKE '%自我领悟%'
                  )
                ORDER BY COALESCE(importance, 5) DESC, created_at DESC
                LIMIT $2
            """, character_id, limit)
    except Exception as e:
        print(f"[Special_Memory] 读取常驻自我认知失败: {e}")
        return ""
    # Selection follows importance desc + newest first, but prompt rendering should
    # read like an evolution timeline: old -> new among the selected items.
    rows = sorted(rows, key=lambda r: _memory_palace_aware_dt(r.get("created_at")) or datetime.min.replace(tzinfo=timezone.utc))
    items = [str(r.get("content") or "").strip() for r in rows if str(r.get("content") or "").strip()]
    if not items:
        return "### Special_Memory\n\n（暂无常驻自我认知）"
    lines = ["### Special_Memory", "", "以下是你内化的自我认知，是你长期记忆的一部分："]
    for item in items:
        lines.append(f"- {item}")
    result = "\n".join(lines)
    _cache_set(cache_key, result, ttl=900)
    return result


async def replace_special_memory_variables(prompt: str, character_id: str = "default") -> str:
    if not isinstance(prompt, str) or not re.search(r"\{\{\s*special_memory", prompt, re.I):
        return prompt
    pattern = re.compile(r"\{\{\s*Special_Memory\s*(?::\s*(\d+)\s*)?\}\}", re.I)
    result = []
    last = 0
    for match in pattern.finditer(prompt):
        raw_limit = match.group(1)
        limit = int(raw_limit) if raw_limit and raw_limit.isdigit() else 3
        result.append(prompt[last:match.start()])
        result.append(await format_special_memory_for_prompt(limit=limit, character_id=character_id))
        last = match.end()
    result.append(prompt[last:])
    return "".join(result)


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
    prompt = await replace_user_activity_meta_variables(prompt, character_id=character_id)
    prompt = await replace_special_memory_variables(prompt, character_id=character_id)
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


# 占位 system 消息上挂载上下文块的私有键，渲染后会被移除
CONTEXT_BLOCKS_KEY = "_ctx_blocks"


_CONTEXT_TEMPLATE_VARS = ("env", "keyword", "hot_news", "operit_memory", "memory_palace")


def render_context_template(template: str, blocks: dict) -> str:
    """把模板里的 {{var}} 换成对应上下文块。

    - 有值：正常替换
    - 无值且变量独占一行：整行删除
    - 无值且嵌在文字中：替换为空串
    - 未知变量：原样保留
    """
    if not template:
        return ""
    out = template
    for _name in _CONTEXT_TEMPLATE_VARS:
        _value = str(blocks.get(_name) or "").strip()
        _pat = "\\{\\{\\s*" + _name + "\\s*\\}\\}"
        if _value:
            out = re.sub(_pat, lambda m, v=_value: v, out)
        else:
            _pat_line = "(?m)^[ \\t]*" + _pat + "[ \\t]*(?:\\r?\\n|$)"
            out = re.sub(_pat_line, "", out)
            out = re.sub(_pat, "", out)
    out = re.sub("\\n{3,}", chr(10) + chr(10), out)
    return out.strip()

def insert_context_blocks_holder(messages: list, blocks: dict) -> bool:
    """在最后一条 user 消息后面插入承载上下文块的占位 system 消息。"""
    if not isinstance(messages, list):
        return False
    insert_at = len(messages)
    for idx in range(len(messages) - 1, -1, -1):
        msg = messages[idx]
        if isinstance(msg, dict) and msg.get("role") == "user":
            insert_at = idx + 1
            break
    messages.insert(insert_at, {
        "role": "system",
        "content": "",
        CONTEXT_BLOCKS_KEY: dict(blocks or {}),
    })
    return True

def _find_context_blocks_holder(messages: list):
    """找挂着上下文块的占位 system 消息（从后往前）。"""
    if not isinstance(messages, list):
        return None
    for msg in reversed(messages):
        if isinstance(msg, dict) and isinstance(msg.get(CONTEXT_BLOCKS_KEY), dict):
            return msg
    return None


async def finalize_context_template(messages: list) -> bool:
    """把占位 system 渲染成模板结果；模板为空或渲染为空则移除该占位消息。"""
    holder = _find_context_blocks_holder(messages)
    if holder is None:
        return False
    blocks = holder.pop(CONTEXT_BLOCKS_KEY, None) or {}
    template = await get_runtime_context_template()
    rendered = render_context_template(template, blocks)
    if rendered:
        holder["content"] = rendered
        return True
    try:
        messages.remove(holder)
    except ValueError:
        pass
    return False

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


def _default_memory_palace_insert_index(messages: list) -> int:
    insert_at = len(messages)
    for idx in range(len(messages) - 1, -1, -1):
        msg = messages[idx]
        if isinstance(msg, dict) and _is_operit_memory_context_message(msg):
            insert_at = idx
    return insert_at


def _is_memory_palace_depth_anchor(msg: dict) -> bool:
    if not isinstance(msg, dict):
        return False
    role = msg.get("role")
    if role not in ("user", "assistant"):
        return False
    # 不把 assistant(tool_calls) 当注入锚点，避免把 system 消息插进工具调用链附近。
    if role == "assistant" and msg.get("tool_calls"):
        return False
    return True


def _memory_palace_insert_index_by_depth(messages: list, depth: int) -> int:
    default_insert_at = _default_memory_palace_insert_index(messages)
    try:
        depth = max(0, int(depth or 0))
    except Exception:
        depth = 0
    if depth <= 0 or not isinstance(messages, list):
        return default_insert_at
    anchors = [idx for idx, msg in enumerate(messages) if _is_memory_palace_depth_anchor(msg)]
    if len(anchors) <= depth:
        return default_insert_at
    return anchors[-1 - depth] + 1


def _insert_memory_palace_system_message(messages: list, text: str, depth: int = 0) -> None:
    injection_msg = {"role": "system", "content": text.strip()}
    insert_at = _memory_palace_insert_index_by_depth(messages, depth)
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
    depth = await get_runtime_memory_palace_injection_depth()
    # depth=0 且开了模板：并入模板块，跟其他上下文合成一条
    if depth <= 0:
        holder = _find_context_blocks_holder(messages)
        if holder is not None:
            holder[CONTEXT_BLOCKS_KEY]["memory_palace"] = injection
            return True
    _insert_memory_palace_system_message(messages, injection, depth=depth)
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



def _log_tool_chain_snapshot(label: str, messages: list, session_id: str = "", enabled: bool = False, extra: str = ""):
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
            add_dashboard_log("info", log_msg, category="chat", session_id=session_id)
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
    hot_news_parts = []
    operit_memory_parts = []
    time_text = ""

    def _clean_one_text(text: str) -> str:
        nonlocal time_text
        if not isinstance(text, str):
            return text

        cleaned, env_text, hot_news_text, attachment_time = extract_environment_bundle_from_text(text)
        cleaned, operit_memory_text = extract_operit_memory_attachment_from_text(cleaned)
        cleaned, proxy_env_text, proxy_time = extract_proxy_sender_context_from_text(cleaned)

        if env_text:
            env_parts.append(env_text)
        if hot_news_text:
            hot_news_parts.append(hot_news_text)
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
    hot_news_final = "\n\n".join(part for part in hot_news_parts if part)
    operit_memory_final = "\n\n".join(part for part in operit_memory_parts if part)
    return cleaned_content, env_text_final, hot_news_final, operit_memory_final


def extract_environment_bundle_from_text(text: str) -> tuple[str, str, str, str]:
    """识别并压缩 Operit 注入的 text/plain 环境附件。
    返回: (清理后的用户文本, 轻量环境上下文, 热点上下文, 附件时间戳)
    """
    if not isinstance(text, str) or "<attachment" not in text:
        return text, "", "", ""

    env_lines = []
    hot_news_parts = []
    attachment_time = ""

    def repl(match):
        nonlocal attachment_time, env_lines, hot_news_parts
        attrs = match.group(1) or ""
        body = match.group(2) or ""
        filename_match = re.search(r'filename="([^"]+)"', attrs)
        filename = filename_match.group(1) if filename_match else ""

        markers = ("【当前时间】", "【当前电量】", "【最近真实热点", "【当前天气】", "【应用使用时长】", "【当前屏幕应用】")
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

        hot_news_block = re.search(r'(📰\s*)?【最近真实热点[^】]*】(.*?)(?=\n\s*【当前天气】|\n\s*【应用使用时长】|\n\s*【当前屏幕应用】|$)', body, re.S)
        if hot_news_block:
            hot_lines = []
            prefix = (hot_news_block.group(1) or "").strip()
            title = "【最近真实热点 · 背景认知】"
            if prefix:
                title = prefix + " " + title
            hot_lines.append(title)
            hot_body = hot_news_block.group(2).strip()
            for line in hot_body.splitlines():
                line = line.rstrip()
                if not line.strip():
                    hot_lines.append("")
                    continue
                if line.strip().startswith("更新时间："):
                    continue
                hot_lines.append(line)
            hot_text = "\n".join(hot_lines).strip()
            if hot_text:
                hot_news_parts.append(hot_text)

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
    hot_news_text = "\n\n".join(part for part in hot_news_parts if part)
    return cleaned, env_text, hot_news_text, attachment_time


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
            content = content_to_text_with_image_placeholder(content)
        else:
            content = normalize_stored_content_for_text(content)
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
        # 事件盒绑定。默认只采纳 relatedTo（往既有记忆上挂），不采纳 sameAs：
        # sameAs 是批内两条新记忆互相配对，一次提取就能凭空开新盒，而压缩阈值
        # 只有 4 条活节点，盒子涨太快会频繁触发 LLM 压缩。想全开把
        # MEMORY_PALACE_AUTO_EVENT_BOX_MODE 设成 all，只想看日志设成 off。
        # 整段失败不影响已保存的记忆和游标推进。
        event_box_count = 0
        event_link_count = 0
        try:
            event_links, event_hints = parse_memory_palace_event_links(raw_items, created, related_refs)
            to_existing, to_new_batch = _memory_palace_split_event_links_by_target(event_links, created)
            event_link_count = len(event_links)
            mode = MEMORY_PALACE_AUTO_EVENT_BOX_MODE
            if mode == "all":
                bind_links = list(event_links)
            elif mode == "off":
                bind_links = []
            else:
                bind_links = list(to_existing)
            for line in _memory_palace_event_link_shadow_report(event_links, event_hints, created, related_refs):
                log_memory_palace_auto_extract("info", line, session_id=session_id)
            if not event_links:
                log_memory_palace_auto_extract(
                    "info",
                    f"\U0001F4E6 事件盒：模型没标任何 relatedTo / sameAs（参考旧记忆 {len(related_refs or [])} 条，新记忆 {len(created)} 条）",
                    session_id=session_id,
                )
            if bind_links:
                event_box_count = await bind_memory_palace_event_boxes(bind_links, event_hints, character_id=character_id)
                skipped = len(event_links) - len(bind_links)
                skip_text = f"，跳过 sameAs {skipped} 条" if skipped > 0 else ""
                log_memory_palace_auto_extract(
                    "info",
                    f"\U0001F4E6 事件盒绑定完成（mode={mode}）：采纳 {len(bind_links)} 条关联{skip_text}，涉及 {event_box_count} 个盒",
                    session_id=session_id,
                )
                try:
                    compressed = await maybe_compress_memory_palace_event_boxes(character_id=character_id)
                    if compressed:
                        log_memory_palace_auto_extract("info", f"\U0001F4E6 事件盒压缩：{compressed} 个", session_id=session_id)
                except Exception as e:
                    log_memory_palace_auto_extract("error", f"\u26A0\uFE0F 分区自动提取事件盒压缩失败: {e}", session_id=session_id)
                # 建过盒就清缓存，否则仪表盘左侧要等 15 分钟 TTL 才看得到新盒。
                try:
                    invalidate_memory_palace_cache(character_id)
                except Exception:
                    pass
            elif event_links:
                log_memory_palace_auto_extract(
                    "info",
                    f"\U0001F4E6 事件盒未绑定（mode={mode}）：{len(event_links)} 条关联按当前模式全部跳过",
                    session_id=session_id,
                )
        except Exception as e:
            log_memory_palace_auto_extract("error", f"\u26A0\uFE0F 分区自动提取事件盒绑定失败: {e}", session_id=session_id)
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
        return {"status": "ok", "processed_messages": len(rows), "extracted": len(raw_items), "created": len(created), "embedded": embedded_count, "unpinned": unpinned_count, "marked": marked_count, "cursor": max_message_id, "event_links": event_link_count, "event_boxes": event_box_count}
    except Exception as e:
        log_memory_palace_auto_extract("error", f"⚠️ 分区自动提取失败：session={session_id}, error={e}", session_id=session_id)
        return {"status": "error", "error": str(e), "created": 0, "marked": 0}


def _content_plain_text(content) -> str:
    """把任意形态的 content 取成纯文本（多模态数组里的图片转占位符）。

    db_row_to_message 会把 image_ref JSON 还原成多模态 list，
    只需要文本的下游逻辑统一走这里，避免 (list).strip() 报错。
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return content_to_text_with_image_placeholder(content)
    return str(content or "")


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


def _partition_b_limit(X: int) -> int:
    """B 区触发阈值（Y）。

    只在 rounds 模式下生效；填 0 或没填就退回旧行为 Y = X。
    这里不做 "上限 - X" 的换算：设置页填的数字就是 Y 本身，
    上下文保留轮数在 X 到 X+Y 之间波动。
    """
    try:
        y = int(CACHE_PARTITION_B_LIMIT or 0)
    except Exception:
        y = 0
    if y <= 0:
        return max(1, int(X or 1))
    return y


def _should_rotate(b_rounds_count: int, X: int, a_msgs: list) -> bool:
    """
    判断是否应该触发A区→摘要的轮转。
    
    rounds模式（默认）：B区轮数 >= Y 时触发（Y 默认等于 X）
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
        
        # time 模式的兜底不套用 Y：Y 只在 rounds 模式生效。
        return b_rounds_count >= X
    
    return b_rounds_count >= _partition_b_limit(X)

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


_TS_PREFIX_RE = re.compile(r"^\[[0-9]{2}(?:-[0-9]{2})? [0-9]{2}:[0-9]{2}\]|^\[[0-9]{2}:[0-9]{2}\]")
_WEEKDAY_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")
SPARSE_TS_GAP_MINUTES = 15      # 同天内间隔达到多少分钟才打戳
SPARSE_TS_LONG_GAP_HOURS = 6    # 间隔达到多少小时额外补一行说明


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


def build_current_message_timestamp_prefix(prev_dt, content) -> str:
    """给当前轮 user 消息算时间戳前缀（稀疏规则，时间取现在）。

    规则与历史消息一致：
      - 已带附件时间戳：不重复打时间，但间隔≥6小时仍补说明
      - 无上一条消息（缓存区为空）：打完整戳
      - 跨天：打完整戳（带星期）
      - 同天间隔≥15分钟：打时分戳
      - 其他：不打
    """
    now_local = datetime.now(timezone.utc) + timedelta(hours=TIMEZONE_HOURS)
    has_prefix = _content_has_timestamp_prefix(content)

    gap_minutes = None
    crossed_day = False
    if prev_dt is not None:
        gap_minutes = max(0, int((now_local - prev_dt).total_seconds() // 60))
        crossed_day = prev_dt.date() != now_local.date()

    need_stamp = (prev_dt is None) or crossed_day or (
        gap_minutes is not None and gap_minutes >= SPARSE_TS_GAP_MINUTES
    )

    parts = []
    if need_stamp and not has_prefix:
        if prev_dt is None or crossed_day:
            wd = _WEEKDAY_CN[now_local.weekday()]
            parts.append(f"[{now_local.strftime('%m-%d')} {wd} {now_local.strftime('%H:%M')}]")
        else:
            parts.append(f"[{now_local.strftime('%H:%M')}]")

    if gap_minutes is not None:
        note = _format_gap_note(gap_minutes)
        if note:
            parts.append(note)

    if not parts:
        return ""
    return chr(10).join(parts) + chr(10) + chr(10)

def _prepend_timestamp_to_user_messages(messages: list, sparse: bool = False,
                                        state: dict = None, return_state: bool = False):
    """给历史消息加时间戳。

    sparse=False（默认，兼容旧行为）：只给 user 消息打紧凑戳，每条都打。
    sparse=True：按间隔稀疏打戳，user/assistant 都参与。
      - 已带附件时间戳的消息不重复打时间，但仍可能补间隔说明
      - 首条、跨天、间隔≥15分钟才打戳；间隔≥6小时额外补一行说明
      - 形如 "[07-29 周三 18:17]" 后接空行再正文
      - 间隔一律按 created_at 计算，与消息有无附件无关

    state / return_state：分区模式要分两次调用（A 区、B 区），中间的
    prev_dt / last_date 必须接上，否则 B 区首条会被当成整段对话的第一条
    重打完整戳，而且 A→B 之间那个间隔算不出来——正好是「离开一段时间
    再回来」最需要提示的位置。
    """
    state = state or {}
    last_date = state.get("last_date")
    prev_dt = state.get("prev_dt")
    first_seen = bool(state.get("first_seen"))
    stamped = []
    for msg in messages:
        m = dict(msg)
        role = m.get("role")
        local_dt = _to_local_dt(m.get("created_at"))

        if not sparse:
            if role == "user" and local_dt:
                show_date = last_date != local_dt.date()
                stamp = (f"[{local_dt.strftime('%m-%d %H:%M')}]" if show_date
                         else f"[{local_dt.strftime('%H:%M')}]")
                if not _content_has_timestamp_prefix(m.get("content")):
                    m["content"] = _prepend_text_to_content(m.get("content"), stamp)
                last_date = local_dt.date()
        elif role in ("user", "assistant") and local_dt:
            gap_minutes = None
            if prev_dt is not None:
                gap_minutes = max(0, int((local_dt - prev_dt).total_seconds() // 60))

            crossed_day = last_date is not None and last_date != local_dt.date()
            need_stamp = (not first_seen) or crossed_day or (
                gap_minutes is not None and gap_minutes >= SPARSE_TS_GAP_MINUTES
            )
            has_prefix = _content_has_timestamp_prefix(m.get("content"))

            parts = []
            if need_stamp and not has_prefix:
                if (not first_seen) or crossed_day:
                    wd = _WEEKDAY_CN[local_dt.weekday()]
                    parts.append(f"[{local_dt.strftime('%m-%d')} {wd} {local_dt.strftime('%H:%M')}]")
                else:
                    parts.append(f"[{local_dt.strftime('%H:%M')}]")

            if gap_minutes is not None:
                note = _format_gap_note(gap_minutes)
                if note:
                    parts.append(note)

            if parts:
                prefix = "\n".join(parts) + "\n\n"
                m["content"] = _prepend_text_to_content(m.get("content"), prefix)

            first_seen = True
            last_date = local_dt.date()
            prev_dt = local_dt
        elif local_dt:
            # tool 等其他 role 不打戳，但参与间隔计算
            prev_dt = local_dt

        m.pop("id", None)
        m.pop("created_at", None)
        stamped.append(m)
    if return_state:
        return stamped, {"last_date": last_date, "prev_dt": prev_dt, "first_seen": first_seen}
    return stamped


async def build_partitioned_messages(
    session_id: str,
    all_messages: list,
    base_prompt: str,
    user_message: str,
    active_history_only: bool = False,
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
    cumulative_a_start_round = int(state.get('a_start_round') or 0)
    a_start_round = 0 if active_history_only else cumulative_a_start_round
    retained_tool_chains = _dedupe_retained_use_package_chains(list(state.get('retained_tool_chains') or []))
    keep_was_enabled = bool(state.get('keep_a_tools_enabled'))

    # Closing clears prior retained chains. Re-opening starts at the current partition boundary;
    # previously evicted history is never searched retroactively.
    if not CACHE_PARTITION_KEEP_A_TOOLS:
        if retained_tool_chains or keep_was_enabled:
            retained_tool_chains = []
            await save_session_cache_state(session_id, summary_parts, cumulative_a_start_round, [], False)
    elif not keep_was_enabled:
        retained_tool_chains = []
        await save_session_cache_state(session_id, summary_parts, cumulative_a_start_round, [], True)

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
    evicted_through_candidate = int(state.get('evicted_through_message_id') or 0)
    retained_audit_before = None
    retained_audit_captured = []
    max_rotations = CACHE_MAX_ROTATIONS if CACHE_PARTITION_TRIGGER == "time" else 999
    while _should_rotate(b_rounds_count, X, a_msgs) and rotation_count < max_rotations:
        rotation_count += 1
        trigger_info = f"B区{b_rounds_count}轮 >= Y={_partition_b_limit(X)}（A区X={X}）" if CACHE_PARTITION_TRIGGER != "time" else f"A区首条消息超出{CACHE_PARTITION_WINDOW}分钟窗口"
        print(f"🔄 轮转#{rotation_count}: session={session_id}, {trigger_info}")
        log_memory_palace_auto_extract("run", f"🧠 分区轮转推进缓存边界：session={session_id}, {trigger_info}, 当前A区{len(a_msgs)}条", session_id=session_id)
        if a_msgs:
            evicted_ids = [int(m.get('id')) for m in a_msgs if m.get('id') is not None]
            if evicted_ids:
                evicted_through_candidate = max(evicted_through_candidate, max(evicted_ids))
        if CACHE_PARTITION_KEEP_A_TOOLS:
            captured = _extract_use_package_chains(a_msgs)
            if captured:
                if retained_audit_before is None:
                    retained_audit_before = list(retained_tool_chains)
                retained_audit_captured.extend(captured)
                before_count = len(retained_tool_chains)
                retained_tool_chains = _dedupe_retained_use_package_chains(retained_tool_chains + captured)
                print(f"🔧 A区轮转: 捕获{len(captured)}组use_package调用链，按包名去重后保留{len(retained_tool_chains)}组（原{before_count}组）")

        a_start_round += X
        cumulative_a_start_round += X
        a_end_round = a_start_round + X
        a_round_groups = rounds[a_start_round : a_end_round]
        b_round_groups = rounds[a_end_round :]
        a_msgs = [msg for rnd in a_round_groups for msg in rnd]
        b_msgs = [msg for rnd in b_round_groups for msg in rnd]
        b_rounds_count = len(b_round_groups)
    
    if rotation_count > 0:
        await save_session_cache_state(
            session_id, summary_parts, cumulative_a_start_round, retained_tool_chains,
            CACHE_PARTITION_KEEP_A_TOOLS, evicted_through_message_id=evicted_through_candidate,
        )
        if evicted_through_candidate > 0:
            asyncio.create_task(release_images_outside_cache(session_id, evicted_through_candidate, "主请求轮转"))
        add_dashboard_log("info", f"🔧 A区use_package保留[处理前]: 已有={_retained_package_names(retained_audit_before or retained_tool_chains)}, 本次捕获={_retained_package_names(retained_audit_captured)}", category="chat", session_id=session_id)
        add_dashboard_log("info", f"🔧 A区use_package保留[处理后]: 最终={_retained_package_names(retained_tool_chains)}（按包名仅留最新）", category="chat", session_id=session_id)
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

    # 已轮转出 A 区的 use_package 链固定放在新对话历史开头。
    if CACHE_PARTITION_KEEP_A_TOOLS and retained_tool_chains:
        result.extend(_flatten_retained_tool_chains(retained_tool_chains))

    # A区：默认剥离tool消息和tool_calls以节省上下文；可在设置页开启保留。
    cleaned_a = []
    if CACHE_PARTITION_KEEP_A_TOOLS:
        for msg in a_msgs:
            # created_at 保留到打戳阶段，由 _prepend_timestamp_to_user_messages 统一剥离
            m = {k: v for k, v in msg.items() if k not in ('id',)}
            cleaned_a.append(m)
    else:
        for msg in a_msgs:
            if msg.get('role') == 'tool':
                continue
            m = {k: v for k, v in msg.items() if k not in ('id', 'tool_calls')}
            if m.get('role') == 'assistant' and not _content_plain_text(m.get('content')).strip():
                continue
            cleaned_a.append(m)
    
    # A区：从末尾往前找第一条非tool消息打BP
    for j in range(len(cleaned_a) - 1, -1, -1):
        if cleaned_a[j].get('role') != 'tool' and _apply_breakpoint(cleaned_a[j]):
            break
    
    _sparse_ts = await get_runtime_sparse_timestamp_enabled()
    # A 区和 B 区连着打戳：B 区是更靠后的历史，之前它固定走密集模式，
    # 所以「距上次对话约 N 小时」这行永远出不来——而长间隔基本都落在这一段。
    # 用 _ts_state 把 A 区末尾的时间接到 B 区开头，间隔才算得出来。
    cleaned_a, _ts_state = _prepend_timestamp_to_user_messages(
        cleaned_a, sparse=_sparse_ts, return_state=True)
    for m in cleaned_a:
        result.append(m)
    
    # B区：先构建去掉created_at的副本，再从末尾往前打BP
    b_cleaned, _ts_state = _prepend_timestamp_to_user_messages(
        b_msgs, sparse=_sparse_ts, state=_ts_state, return_state=True)
    
    for j in range(len(b_cleaned) - 1, -1, -1):
        if b_cleaned[j].get('role') != 'tool' and _apply_breakpoint(b_cleaned[j]):
            break
    
    for m in b_cleaned:
        result.append(m)
    
    if current_user_msg:
        current_content, env_text, hot_news_text, operit_memory_text = _clean_current_user_content_preserve_multimodal(
            current_user_msg.get('content', ''),
            history=history,
            shorten_time=True,
        )
        if _sparse_ts:
            # 优先用打戳过程记下的最后一条时间；它和历史打戳同源，不会错位。
            _prev_dt = (_ts_state or {}).get("prev_dt") or _last_message_dt(a_msgs, b_msgs)
            _cur_prefix = build_current_message_timestamp_prefix(_prev_dt, current_content)
            if _cur_prefix:
                current_content = _prepend_text_to_content(current_content, _cur_prefix)
        result.append({"role": "user", "content": current_content})

        keyword_context_text = await build_keyword_context_text(current_content)
        if await get_runtime_context_template_enabled():
            # 模板模式：塞一个占位 system，记忆宫殿注入后统一渲染成一条
            result.append({
                "role": "system",
                "content": "",
                CONTEXT_BLOCKS_KEY: {
                    "env": env_text,
                    "keyword": keyword_context_text,
                    "hot_news": hot_news_text,
                    "operit_memory": operit_memory_text,
                },
            })
        else:
            # 环境/插件上下文后置为轻量 system 消息，避免原始注入污染用户正文。
            if env_text:
                result.append({"role": "system", "content": env_text})
            if keyword_context_text:
                result.append({"role": "system", "content": keyword_context_text})
            if hot_news_text:
                result.append({"role": "system", "content": hot_news_text})
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
    
    _sparse_ts = await get_runtime_sparse_timestamp_enabled()
    h_cleaned, _ts_state = _prepend_timestamp_to_user_messages(
        history, sparse=_sparse_ts, return_state=True)
    
    # 从末尾往前找第一条非tool消息打BP
    for j in range(len(h_cleaned) - 1, -1, -1):
        if h_cleaned[j].get('role') != 'tool' and _apply_breakpoint(h_cleaned[j]):
            break
    
    for m in h_cleaned:
        result.append(m)
    
    if current_user_msg:
        current_content, env_text, hot_news_text, operit_memory_text = _clean_current_user_content_preserve_multimodal(
            current_user_msg.get('content', ''),
            history=history,
            shorten_time=False,
        )
        if _sparse_ts:
            _prev_dt = (_ts_state or {}).get("prev_dt") or _last_message_dt(history)
            _cur_prefix = build_current_message_timestamp_prefix(_prev_dt, current_content)
            if _cur_prefix:
                current_content = _prepend_text_to_content(current_content, _cur_prefix)
        result.append({"role": "user", "content": current_content})

        keyword_context_text = await build_keyword_context_text(current_content)
        if await get_runtime_context_template_enabled():
            # 模板模式：塞一个占位 system，记忆宫殿注入后统一渲染成一条
            result.append({
                "role": "system",
                "content": "",
                CONTEXT_BLOCKS_KEY: {
                    "env": env_text,
                    "keyword": keyword_context_text,
                    "hot_news": hot_news_text,
                    "operit_memory": operit_memory_text,
                },
            })
        else:
            # 环境/插件上下文后置为轻量 system 消息，避免原始注入污染用户正文。
            if env_text:
                result.append({"role": "system", "content": env_text})
            if keyword_context_text:
                result.append({"role": "system", "content": keyword_context_text})
            if hot_news_text:
                result.append({"role": "system", "content": hot_news_text})
            # Operit 原生记忆附件放在最底部，按用户手动检索结果使用。
            if operit_memory_text:
                result.append({"role": "system", "content": operit_memory_text})
    
    bp_count = 1 + (1 if history else 0)
    print(f"🔒 基础缓存(降级): BP×{bp_count} | 历史{len(history)}条 | 总{len(result)}条messages")
    return result


# ============================================================
# 后台记忆处理
# ============================================================

AUTO_TRIGGER_TAG = "<自动触发>"


def is_auto_trigger_message(content) -> bool:
    """判断消息是否为 Operit 主动触发消息（标签必须在正文最开头）。

    命中的消息只发给上游、不写入 conversations 表的 user 记录，
    因此历史里只留下 assistant 的主动发言。
    """
    if isinstance(content, str):
        return content.lstrip().startswith(AUTO_TRIGGER_TAG)
    if isinstance(content, list):
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                return str(block.get("text", "") or "").lstrip().startswith(AUTO_TRIGGER_TAG)
    return False

def clean_user_message_for_log(user_msg: str, history: list = None) -> str:
    """保存到对话记录前，清理附件/proxy注入，避免Dashboard显示大段原始上下文。"""
    if not isinstance(user_msg, str):
        return user_msg

    cleaned = user_msg
    time_text = ""

    cleaned, _env_text, _hot_news_text, attachment_time = extract_environment_bundle_from_text(cleaned)
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

async def persist_assistant_tool_calls_sync(session_id: str, user_msg: str, assistant_msg: str, model: str, assistant_tool_calls: list = None, assistant_reasoning: str = None, context_messages: list = None) -> bool:
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
        clean_user_msg, _sync_has_image = await build_user_log_content(
            user_msg, context_messages, session_id, recent_log_history
        )
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


async def _ensure_partition_message_boundary(session_id: str, state: dict = None) -> tuple:
    """兼容旧状态：把既有 a_start_round 一次性迁移成永久消息 ID 边界。"""
    state = state or await get_session_cache_state(session_id)
    boundary = int(state.get("evicted_through_message_id") or 0)
    old_round_boundary = int(state.get("a_start_round") or 0)
    if boundary > 0 or old_round_boundary <= 0:
        return state, boundary
    light = await _fetch_partition_boundary_messages(session_id, limit=10000)
    rounds = group_by_rounds([m for m in light if m.get("role") not in ("system", "developer")])
    evicted = [m for rnd in rounds[:min(old_round_boundary, len(rounds))] for m in rnd]
    ids = [int(m.get("id")) for m in evicted if m.get("id") is not None]
    if ids:
        boundary = max(ids)
        await save_session_cache_state(
            session_id, state.get("summary_parts") or [], old_round_boundary,
            state.get("retained_tool_chains") or [], state.get("keep_a_tools_enabled"),
            evicted_through_message_id=boundary,
        )
        state = dict(state)
        state["evicted_through_message_id"] = boundary
    return state, boundary


async def _fetch_partition_boundary_messages(session_id: str, limit: int = 10000) -> list:
    """仅读取分区轮转所需的轻量字段，避免为自动提取边界判断拉取全部正文。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, role, created_at
            FROM conversations
            WHERE session_id = $1
            ORDER BY created_at ASC, id ASC
            LIMIT $2
        """, session_id, int(limit or 10000))
    return [{"id": r["id"], "role": r["role"], "created_at": r["created_at"], "content": ""} for r in rows]


async def _fetch_partition_extract_messages_by_ids(session_id: str, message_ids: list) -> list:
    """只读取实际需要送入记忆宫殿提取器的消息正文。"""
    ids = []
    for mid in message_ids or []:
        try:
            ids.append(int(mid))
        except Exception:
            pass
    ids = list(dict.fromkeys(ids))
    if not ids:
        return []
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, session_id, role, content, created_at
            FROM conversations
            WHERE session_id = $1 AND id = ANY($2::bigint[])
            ORDER BY created_at ASC, id ASC
        """, session_id, ids)
    return [dict(r) for r in rows]


async def _fetch_partition_extract_messages_range(
    session_id: str, after_id: int, through_id: int, limit: int,
) -> list:
    """读取游标之后、永久缓存边界以内的待提取正文。"""
    after_id = max(0, int(after_id or 0))
    through_id = max(0, int(through_id or 0))
    if through_id <= after_id:
        return []
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, session_id, role, content, created_at
            FROM conversations
            WHERE session_id = $1 AND id > $2 AND id <= $3
              AND content IS NOT NULL AND content <> ''
            ORDER BY id DESC
            LIMIT $4
        """, session_id, after_id, through_id, max(1, int(limit or 120)))
    return [dict(r) for r in reversed(rows)]


async def _run_partition_auto_extract_after_response_locked(session_id: str, character_id: str = "default"):
    """回复后维护永久分区边界，并提取 cursor 与边界之间的消息。"""
    try:
        state = await get_session_cache_state(session_id)
        state, boundary_id = await _ensure_partition_message_boundary(session_id, state)
        summary_parts = state.get('summary_parts') or []
        cumulative_a_start_round = int(state.get('a_start_round') or 0)
        retained_tool_chains = _dedupe_retained_use_package_chains(list(state.get('retained_tool_chains') or []))
        keep_was_enabled = bool(state.get('keep_a_tools_enabled'))

        # 后台只观察永久边界之后的活跃区。删除尾部不会让 boundary_id 之前的消息重新出现。
        if CACHE_PARTITION_KEEP_A_TOOLS:
            active_rows = await get_conversation_messages_after_id(session_id, boundary_id, limit=10000)
            active_msgs = []
            for row in active_rows or []:
                msg = db_row_to_message(row)
                msg['created_at'] = row.get('created_at')
                msg['id'] = row.get('id')
                active_msgs.append(msg)
        else:
            pool = await get_pool()
            async with pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT id, role, created_at
                    FROM conversations
                    WHERE session_id = $1 AND id > $2
                    ORDER BY created_at ASC, id ASC
                    LIMIT 10000
                """, session_id, boundary_id)
            active_msgs = [{"id": r['id'], "role": r['role'], "created_at": r['created_at'], "content": ""} for r in rows]

        non_system = [m for m in active_msgs if m.get('role') not in ('system', 'developer')]
        rounds = group_by_rounds(non_system)

        if not CACHE_PARTITION_KEEP_A_TOOLS:
            if retained_tool_chains or keep_was_enabled:
                retained_tool_chains = []
                await save_session_cache_state(session_id, summary_parts, cumulative_a_start_round, [], False)
        elif not keep_was_enabled:
            retained_tool_chains = []
            await save_session_cache_state(session_id, summary_parts, cumulative_a_start_round, [], True)

        X = CACHE_PARTITION_X
        local_start_round = 0
        a_round_groups = rounds[:X]
        b_round_groups = rounds[X:]
        a_msgs = [msg for rnd in a_round_groups for msg in rnd]
        b_rounds_count = len(b_round_groups)
        rotation_count = 0
        boundary_candidate = boundary_id
        retained_audit_before = None
        retained_audit_captured = []
        max_rotations = CACHE_MAX_ROTATIONS if CACHE_PARTITION_TRIGGER == "time" else 999

        while _should_rotate(b_rounds_count, X, a_msgs) and rotation_count < max_rotations:
            rotation_count += 1
            trigger_info = f"B区{b_rounds_count}轮 >= Y={_partition_b_limit(X)}（A区X={X}）" if CACHE_PARTITION_TRIGGER != "time" else f"A区首条消息超出{CACHE_PARTITION_WINDOW}分钟窗口"
            log_memory_palace_auto_extract("run", f"🧠 回复后分区轮转推进缓存边界：session={session_id}, {trigger_info}, 当前A区{len(a_msgs)}条", session_id=session_id)
            evicted_ids = [int(m.get('id')) for m in a_msgs if m.get('id') is not None]
            if evicted_ids:
                boundary_candidate = max(boundary_candidate, max(evicted_ids))
            if CACHE_PARTITION_KEEP_A_TOOLS:
                captured = _extract_use_package_chains(a_msgs)
                if captured:
                    if retained_audit_before is None:
                        retained_audit_before = list(retained_tool_chains)
                    retained_audit_captured.extend(captured)
                    retained_tool_chains = _dedupe_retained_use_package_chains(retained_tool_chains + captured)
            local_start_round += X
            cumulative_a_start_round += X
            a_round_groups = rounds[local_start_round:local_start_round + X]
            b_round_groups = rounds[local_start_round + X:]
            a_msgs = [msg for rnd in a_round_groups for msg in rnd]
            b_rounds_count = len(b_round_groups)

        if rotation_count > 0:
            await save_session_cache_state(
                session_id, summary_parts, cumulative_a_start_round, retained_tool_chains,
                CACHE_PARTITION_KEEP_A_TOOLS, evicted_through_message_id=boundary_candidate,
            )
            if boundary_candidate > 0:
                asyncio.create_task(release_images_outside_cache(session_id, boundary_candidate, "回复后轮转"))
            boundary_id = boundary_candidate
            add_dashboard_log("info", f"🔧 A区use_package保留[处理前]: 已有={_retained_package_names(retained_audit_before or retained_tool_chains)}, 本次捕获={_retained_package_names(retained_audit_captured)}", category="chat", session_id=session_id)
            add_dashboard_log("info", f"🔧 A区use_package保留[处理后]: 最终={_retained_package_names(retained_tool_chains)}（按包名仅留最新）", category="chat", session_id=session_id)
            log_memory_palace_auto_extract("run", f"🧠 回复后分区轮转完成：session={session_id}, 共{rotation_count}次, boundary={boundary_id}", session_id=session_id)

        # 主请求可能已经推进边界；无论由哪一边推进，都只提取 cursor 与永久边界之间的正文。
        cursor = await get_memory_palace_extraction_cursor(session_id, character_id=character_id)
        last_id = int(cursor.get('last_message_id') or 0)
        if boundary_id <= last_id:
            log_memory_palace_auto_extract(
                "info",
                f"🧠 分区自动提取等待：被挤出内容已在游标内 session={session_id}, cursor={last_id}, tail={boundary_id}",
                session_id=session_id,
            )
        extract_msgs = await _fetch_partition_extract_messages_range(
            session_id, last_id, boundary_id, max(1, int(CACHE_PARTITION_EXTRACT_LIMIT or 120)),
        )
        if extract_msgs:
            log_memory_palace_auto_extract(
                "run",
                f"🧠 回复后分区自动提取检查缓存区外内容：session={session_id}, cursor={last_id}, tail={boundary_id}, 消息{len(extract_msgs)}条",
                session_id=session_id,
            )
            result = await extract_memory_palace_from_partition_messages(extract_msgs, session_id, character_id=character_id)
            if isinstance(result, dict) and result.get('status') == 'error':
                log_memory_palace_auto_extract("error", f"⚠️ 回复后分区自动提取失败，下次回复后重试：session={session_id}, error={result.get('error')}", session_id=session_id)
        elif boundary_id > last_id:
            log_memory_palace_auto_extract(
                "info",
                f"🧠 分区自动提取等待：没有游标后的新消息 session={session_id}, cursor={last_id}",
                session_id=session_id,
            )
    except Exception as e:
        log_memory_palace_auto_extract("error", f"⚠️ 回复后分区自动提取异常，下次回复后重试：session={session_id}, error={e}", session_id=session_id)


async def _is_tool_result_occurrence_already_saved(session_id: str, tool_call_id: str) -> bool:
    """同 id 可重复调用；只有已保存 tool 次数 >= assistant(tool_calls) 发生次数时，才认为这条结果是重复。"""
    if not tool_call_id:
        return False
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT
                  COALESCE((
                    SELECT COUNT(*)
                    FROM conversations
                    WHERE session_id = $1
                      AND role = 'assistant'
                      AND metadata IS NOT NULL
                      AND EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(metadata::jsonb -> 'tool_calls') AS elem
                        WHERE elem ->> 'id' = $2
                      )
                  ), 0) AS call_count,
                  COALESCE((
                    SELECT COUNT(*)
                    FROM conversations
                    WHERE session_id = $1
                      AND role = 'tool'
                      AND metadata IS NOT NULL
                      AND metadata::jsonb ->> 'tool_call_id' = $2
                  ), 0) AS tool_count
                """,
                session_id, tool_call_id
            )
        call_count = int(row["call_count"] or 0) if row else 0
        tool_count = int(row["tool_count"] or 0) if row else 0
        return call_count > 0 and tool_count >= call_count
    except Exception as e:
        print(f"⚠️ tool结果发生次数查重失败，继续保存 id={tool_call_id}: {e}")
        return False


async def build_user_log_content(user_msg: str, original_messages: list, session_id: str, history: list = None):
    """构造 user 消息的落库内容。

    返回 (content_to_store, has_image)。
    - 无图或归档未启用：返回清理后的纯文本（与原行为一致）
    - 有图且归档成功：返回 JSON 字符串，text 与 image_ref 保持原始顺序
    """
    clean_text = clean_user_message_for_log(user_msg, history) if user_msg else user_msg

    if not image_archive_active() or not original_messages:
        return clean_text, False

    raw_content = None
    for msg in reversed(original_messages):
        if isinstance(msg, dict) and msg.get("role") == "user":
            raw_content = msg.get("content")
            break

    if not content_has_base64_image(raw_content):
        return clean_text, False

    try:
        archived_items, count = await archive_images_in_content(raw_content, session_id=session_id)
    except Exception as e:
        print(f"⚠️ 图片归档失败，回退纯文本保存: {e}")
        return clean_text, False

    if not count:
        return clean_text, False

    # 保持原始顺序：按块遍历，text 块沿用清洗结果，image_ref 留在原位。
    text_indexes = [
        i for i, item in enumerate(archived_items)
        if isinstance(item, dict) and item.get("type") == "text" and (item.get("text") or "").strip()
    ]

    stored_items = []
    single_text = len(text_indexes) <= 1
    for idx, item in enumerate(archived_items):
        if not isinstance(item, dict):
            continue
        itype = item.get("type")
        if itype == "image_ref":
            stored_items.append(item)
        elif idx in text_indexes:
            if single_text:
                # 只有一个文字块：用清洗后的完整文本（含时间戳前缀等处理）
                if clean_text:
                    stored_items.append({"type": "text", "text": clean_text})
            else:
                # 多个文字块穿插：各自原样保留，避免与 clean_text 重复
                stored_items.append({"type": "text", "text": item.get("text", "")})

    # 原始 content 里没有任何文字块，但清洗结果有文本时，补在最前面
    if clean_text and not any(b.get("type") == "text" for b in stored_items):
        stored_items.insert(0, {"type": "text", "text": clean_text})

    if not stored_items:
        return clean_text, False

    print(f"🖼️ user 消息含 {count} 张图片，已归档并以 JSON 形式落库")
    return json.dumps(stored_items, ensure_ascii=False), True


async def process_memories_background(session_id: str, user_msg: str, assistant_msg: str, model: str, context_messages: list = None, skip_conversation_log: bool = False, tool_messages: list = None, assistant_tool_calls: list = None, assistant_reasoning: str = None, is_auto_trigger: bool = False):
    """
    后台异步：存储对话记录（不阻塞主流程）。
    
    旧碎片记忆自动提取已移除；长期记忆由 Memory Palace 的手动预览导入
    和分区轮转自动提取负责。
    
    context_messages: 客户端发来的原始对话上下文（不含system prompt），保留参数兼容旧调用。
    skip_conversation_log: 跳过对话存储（标题生成等辅助请求时使用）
    tool_messages: 客户端发来的工具结果消息列表
    assistant_tool_calls: response中assistant的工具调用列表（如果有）
    assistant_reasoning: response中assistant的reasoning_content（deepseek thinking mode）
    is_auto_trigger: 本轮 user 消息是 <自动触发> 主动消息，只存 assistant 不存 user
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
        clean_user_msg, _user_has_image = await build_user_log_content(
            user_msg, context_messages, session_id, recent_log_history
        )
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
                    if await _is_tool_result_occurrence_already_saved(session_id, db_tool_call_id):
                        print(f"🔧 存储: 按发生次数跳过重复tool结果 id={db_tool_call_id}")
                        continue

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
            
            if is_auto_trigger:
                # 主动触发：user 不落库，assistant 一律追加（连续触发是多次独立问候，不做 re-roll 覆盖）
                await save_message(session_id, "assistant", assistant_msg or "", model, metadata=assistant_meta)
                print("⏭️  主动触发消息：已存 assistant，跳过 user 存储")
            elif assistant_tool_calls:
                # 首次工具调用：assistant回复包含tool_calls，存user + assistant(tool_calls)
                await save_message(session_id, "user", clean_user_msg, model)
                await save_message(session_id, "assistant", assistant_msg or "", model, metadata=assistant_meta)
                print(f"🔧 存储: user + assistant (含{len(assistant_tool_calls)}个tool_calls)" + (" (含reasoning)" if assistant_reasoning else ""))
            else:
                # re-roll检测 + 存user + assistant
                # 含图消息的 content 是 JSON 字符串；同一条消息重发时 clean_text 与
                # image_ref(sha256去重后URL相同) 都一致，序列化结果稳定，可直接参与文本比对。
                updated = await update_last_assistant_if_same_user(
                    session_id, clean_user_msg, assistant_msg, model, metadata=assistant_meta
                )
                if updated:
                    print(f"🔄 检测到re-roll，已覆盖最后一条assistant回复")
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

@app.get("/healthz")
@app.head("/healthz")
async def healthz():
    """轻量存活探针：不读数据库、不依赖全局配置。"""
    return {"ok": True}

@app.get("/")
@app.head("/")
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
    messages, _incoming_xml_tool_converted = _normalize_incoming_xml_tool_messages(messages)
    body["messages"] = messages

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
    
    # ---------- 检测主动触发消息 ----------
    # 正文以 <自动触发> 开头时，本轮 user 消息只发给上游、不落库；
    # 历史里因此只保留 assistant 的主动发言，连续触发一律追加不覆盖。
    is_auto_trigger = False
    for _m in reversed(messages):
        if _m.get("role") == "user":
            is_auto_trigger = is_auto_trigger_message(_m.get("content", ""))
            break
    if is_auto_trigger:
        print("⏭️  检测到主动触发消息，本轮不保存 user 记录")
    
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
        
        if _incoming_xml_tool_converted and tool_chain_debug:
            add_dashboard_log("info", f"🔧 tool_chain[incoming_xml_normalized] converted={_incoming_xml_tool_converted}", category="chat", session_id=session_id)


        # 只读取永久分区边界之后的活跃历史；被挤出的旧消息不会因尾部删除而重新进入。
        partition_state = await get_session_cache_state(session_id)
        partition_state, partition_boundary_id = await _ensure_partition_message_boundary(session_id, partition_state)
        try:
            db_history = await get_conversation_messages_after_id(session_id, partition_boundary_id, limit=10000)
            db_msgs = []
            for m in (db_history or []):
                msg = db_row_to_message(m)
                msg['created_at'] = m.get('created_at')
                msg['id'] = m.get('id')
                db_msgs.append(msg)
        except Exception as e:
            print(f"[warning] 分区模式读取活跃历史失败: {e}")
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
        partition_has_explicit_memory_palace = bool(re.search(r"\{\{\s*memory_palace", partition_base_prompt or "", re.I))
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
                    if await _is_tool_result_occurrence_already_saved(session_id, db_tool_call_id):
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
                    latest_state = await get_session_cache_state(session_id)
                    latest_boundary_id = int(latest_state.get("evicted_through_message_id") or partition_boundary_id or 0)
                    db_history = await get_conversation_messages_after_id(session_id, latest_boundary_id, limit=10000)
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
            # 不能只用 set 判断 tool_call_id 是否已在 DB。
            # 连续调用同一工具且参数相同时，客户端/上游可能复用同一个 tool_call_id；
            # 如果历史里已有同 id 的旧结果，set 会误认为“当前结果已在 DB”，
            # 导致当前 tool_result 不进入本次上游请求。
            # 这里按发生次数判断：DB 中该 id 的 tool 结果数量，是否已经满足 assistant(tool_calls) 出现次数。
            db_tool_call_counts = {}
            db_tool_result_counts = {}
            for _m in db_msgs:
                if _m.get("role") == "assistant" and _m.get("tool_calls"):
                    for _tc in (_m.get("tool_calls") or []):
                        _cid = _tc.get("id")
                        if _cid:
                            db_tool_call_counts[_cid] = db_tool_call_counts.get(_cid, 0) + 1
                elif _m.get("role") == "tool" and _m.get("tool_call_id"):
                    _cid = _m.get("tool_call_id")
                    db_tool_result_counts[_cid] = db_tool_result_counts.get(_cid, 0) + 1

            increment_tools = []
            _increment_counts = {}
            for _tm in tool_messages:
                _cid = _tm.get("tool_call_id")
                if not _cid:
                    continue
                _call_count = db_tool_call_counts.get(_cid, 0)
                _saved_count = db_tool_result_counts.get(_cid, 0)
                _added_count = _increment_counts.get(_cid, 0)
                # DB 没有对应 assistant 时，也要把当前 tool 带上，下面会从客户端补 assistant(tool_calls)。
                if _call_count == 0 or (_saved_count + _added_count) < _call_count:
                    increment_tools.append(_tm)
                    _increment_counts[_cid] = _added_count + 1

            if increment_tools:
                print(f"🔧 分区模式: 按发生次数保留{len(increment_tools)}条当前tool增量 ids={[m.get('tool_call_id','?') for m in increment_tools]}")
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
        # Do not repair the entire DB history here. Only the final messages that actually enter
        # the upstream request are repaired below.
        all_msgs = _normalize_tool_chains_by_id(all_msgs)
        _log_tool_chain_snapshot("all_msgs_after_normalize", all_msgs, session_id=session_id, enabled=tool_chain_debug)

        # 后台保存仍只接收本轮真实tool；已同步写过的会被tool_call_id查重跳过
        tool_messages = [m for m in tool_messages if m.get("role") == "tool"]
        
        print(f"📦 分区模式: DB历史{len(db_msgs)}条 + 本轮增量{len(client_increment)}条")
        
        messages = await build_partitioned_messages(
            session_id, all_msgs, partition_base_prompt, user_message, active_history_only=True
        )
        messages = _repair_tool_call_ids_by_adjacency(messages, session_id=session_id, reason="final_messages")
        messages = _normalize_tool_chains_by_id(messages)
        messages = _drop_orphan_tool_messages(messages)
        _log_tool_chain_snapshot("final_after_drop_orphan", messages, session_id=session_id, enabled=tool_chain_debug)

        await inject_memory_palace_auto_context(messages, query=user_message, recent_messages=messages, explicit_present=partition_has_explicit_memory_palace, session_id=session_id)
        await finalize_context_template(messages)
        body["messages"] = messages
    
    else:
        # 非分区模式：对 system 消息做变量替换（与分区模式一致）
        non_partition_has_explicit_memory_palace = False
        for m in messages:
            if m.get("role") in ("system", "developer"):
                c = m.get("content", "")
                if isinstance(c, str):
                    if re.search(r"\{\{\s*memory_palace", c, re.I):
                        non_partition_has_explicit_memory_palace = True
                    m["content"] = await replace_explicit_memory_variables(c, query=user_message, recent_messages=messages, session_id=session_id)
                elif isinstance(c, list):
                    for item in c:
                        if isinstance(item, dict) and item.get("type") == "text":
                            txt = item.get("text", "")
                            if re.search(r"\{\{\s*memory_palace", txt, re.I):
                                non_partition_has_explicit_memory_palace = True
                            item["text"] = await replace_explicit_memory_variables(txt, query=user_message, recent_messages=messages, session_id=session_id)
        body["messages"] = messages
        if await get_runtime_context_template_enabled():
            # 模板模式：先放占位 system 承载关键词块，记忆宫殿随后并入
            insert_context_blocks_holder(messages, {
                "keyword": await build_keyword_context_text(user_message),
            })
        else:
            await inject_keyword_context_auto_context(messages, user_message)
        await inject_memory_palace_auto_context(messages, query=user_message, recent_messages=messages, explicit_present=non_partition_has_explicit_memory_palace, session_id=session_id)
        await finalize_context_template(messages)

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
            stream_and_capture(headers, body, session_id, user_message, model, original_messages, skip_conversation_log, tool_messages, is_auto_trigger),
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
                    # If no native tool_calls but content has XML tool call, extract it
                    if not assistant_tool_calls and assistant_msg:
                        _clean, _extracted = _extract_xml_tool_calls_from_content(assistant_msg)
                        if _extracted:
                            assistant_tool_calls = _extracted
                            assistant_msg = _clean or ""
                            print(f"\U0001f527 NonStream: extracted XML tool call: {[tc['function']['name'] for tc in _extracted]}")
                except (KeyError, IndexError):
                    pass
                
                if MEMORY_ENABLED and (user_message or tool_messages):
                    sync_saved_tool_call = False
                    if assistant_tool_calls and not tool_messages and not skip_conversation_log and not is_auto_trigger:
                        sync_saved_tool_call = await persist_assistant_tool_calls_sync(
                            session_id, user_message, assistant_msg, model, assistant_tool_calls, assistant_reasoning, original_messages
                        )
                    asyncio.create_task(
                        process_memories_background(session_id, user_message, assistant_msg, model, 
                                                    context_messages=original_messages, skip_conversation_log=(skip_conversation_log or sync_saved_tool_call),
                                                    tool_messages=tool_messages, assistant_tool_calls=assistant_tool_calls,
                                                    assistant_reasoning=assistant_reasoning, is_auto_trigger=is_auto_trigger)
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


async def stream_and_capture(headers: dict, body: dict, session_id: str, user_message: str, model: str, original_messages: list = None, skip_conversation_log: bool = False, tool_messages: list = None, is_auto_trigger: bool = False):
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
    # If upstream returned no native tool_calls but content has XML tool call, extract it
    if not assistant_tool_calls and assistant_msg:
        _clean, _extracted = _extract_xml_tool_calls_from_content(assistant_msg)
        if _extracted:
            assistant_tool_calls = _extracted
            assistant_msg = _clean or ""
            print(f"\U0001f527 Stream: extracted XML tool call from content: {[tc['function']['name'] for tc in _extracted]}")
    
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
        if assistant_tool_calls and not tool_messages and not skip_conversation_log and not is_auto_trigger:
            sync_saved_tool_call = await persist_assistant_tool_calls_sync(
                session_id, user_message, assistant_msg, model, assistant_tool_calls, assistant_reasoning, original_messages
            )
        asyncio.create_task(
            process_memories_background(session_id, user_message, assistant_msg, model, 
                                        context_messages=original_messages, skip_conversation_log=(skip_conversation_log or sync_saved_tool_call),
                                        tool_messages=tool_messages, assistant_tool_calls=assistant_tool_calls,
                                        assistant_reasoning=assistant_reasoning, is_auto_trigger=is_auto_trigger)
        )


# ============================================================
# 图片归档 —— 逻辑已拆分到 image_archive.py
# ============================================================

from image_archive import (
    IMAGE_ARCHIVE_ENABLED, R2_ENDPOINT, R2_ACCESS_KEY, R2_SECRET_KEY,
    R2_BUCKET, R2_PUBLIC_URL, image_archive_ready, image_archive_active,
    archive_images_in_content, content_to_text_with_image_placeholder,
    content_has_base64_image, release_images_outside_cache,
    release_images_for_session, release_images_for_message_id,
    release_images_removed_by_edit, normalize_stored_content_for_text,
    set_partition_enabled_getter,
)

# 注入分区开关读取
set_partition_enabled_getter(lambda: CACHE_PARTITION_ENABLED)


@app.get("/api/image-archive/status")
async def api_image_archive_status():
    """图片归档配置状态（只读，供仪表盘展示）。不返回密钥内容。"""
    return {
        "enabled": IMAGE_ARCHIVE_ENABLED,
        "ready": image_archive_ready(),
        "active": image_archive_active(),
        "partition_enabled": CACHE_PARTITION_ENABLED,
        "config": {
            "R2_ENDPOINT": bool(R2_ENDPOINT),
            "R2_ACCESS_KEY": bool(R2_ACCESS_KEY),
            "R2_SECRET_KEY": bool(R2_SECRET_KEY),
            "R2_BUCKET": bool(R2_BUCKET),
            "R2_PUBLIC_URL": bool(R2_PUBLIC_URL),
        },
        "public_url": R2_PUBLIC_URL,
        "bucket": R2_BUCKET,
    }
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
    cache_key = "mp:stats:export"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            counts = {}
            for table in _MEMORY_PALACE_BACKUP_TABLES:
                counts[table] = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
        result = {
            "status": "ok",
            "counts": counts,
            "total_nodes": counts.get("memory_palace_nodes", 0),
            "total_vectors": counts.get("memory_palace_vectors", 0),
            "total_links": counts.get("memory_palace_links", 0),
            "total_event_boxes": counts.get("memory_palace_event_boxes", 0),
        }
        return _cache_set(cache_key, result, ttl=900)
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
        character_id = data.get("character_id") or "default"
        result = await confirm_memory_palace_import(
            data.get("import_token") or "",
            strategy=data.get("strategy") or "merge_skip_duplicates",
            include=data.get("include") or {},
            character_id=character_id,
        )
        if result.get("status") != "error":
            invalidate_memory_palace_cache(character_id)
        return result
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


async def generate_daily_impression_for_date(impression_date, start_hour: int = 0):
    """从指定日期的对话历史生成/更新日印象，不改动碎片状态。"""
    try:
        start_hour = int(start_hour or 0)
    except (TypeError, ValueError):
        start_hour = 0
    start_hour = max(0, min(start_hour, 23))
    messages = await get_conversation_messages_by_date(impression_date, start_hour=start_hour)
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
                    "max_tokens": 12000,
                    "temperature": 0,
                },
            )
        response_text = response.text or ""
        if response.status_code != 200:
            return {
                "status": "error",
                "error": f"HTTP {response.status_code}: {response_text[:300]}",
                "raw": response_text,
                "raw_response": response_text,
            }

        try:
            response_json = response.json()
        except Exception as parse_error:
            return {
                "status": "error",
                "error": f"模型接口响应不是合法 JSON: {parse_error}",
                "raw": response_text,
                "raw_response": response_text,
            }

        raw = response_json.get("choices", [{}])[0].get("message", {}).get("content", "")
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
            return {"status": "error", "error": "AI 未返回 <summary> 标签", "raw": raw, "raw_response": raw}

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
            "start_hour": start_hour,
            "messages_used": len(messages),
            "impression": _serialize_daily_impression(saved),
            "raw": raw,
        }
    except Exception as e:
        err_text = str(e)
        return {"status": "error", "error": err_text, "raw": err_text, "raw_response": err_text}

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
    limit = max(1, min(int(limit or 30), 10000))
    cache_key = f"daily:list:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rows = await list_daily_impressions(limit)
    result = {"status": "ok", "impressions": [_serialize_daily_impression(r) for r in rows]}
    return _cache_set(cache_key, result, ttl=900)


@app.get("/api/daily-impressions/stats")
async def api_daily_impressions_stats():
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    cache_key = "daily:stats"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM daily_impressions")
            latest = await conn.fetchrow("""
                SELECT impression_date, updated_at
                FROM daily_impressions
                ORDER BY impression_date DESC
                LIMIT 1
            """)
        result = {
            "status": "ok",
            "total": int(total or 0),
            "latest_date": latest["impression_date"].isoformat() if latest and latest.get("impression_date") else None,
            "latest_updated_at": latest["updated_at"].isoformat() if latest and latest.get("updated_at") else None,
        }
        return _cache_set(cache_key, result, ttl=900)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.get("/api/daily-impressions/months")
async def api_daily_impression_months():
    """日印象月份概览：只返回月份、数量和日期范围，不返回全部正文。"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    cache_key = "daily:months"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT to_char(impression_date, 'YYYY-MM') AS month,
                       COUNT(*) AS count,
                       MIN(impression_date) AS earliest_date,
                       MAX(impression_date) AS latest_date,
                       MAX(updated_at) AS latest_updated_at,
                       array_agg(mood ORDER BY impression_date DESC) FILTER (WHERE mood IS NOT NULL AND mood <> '') AS moods
                FROM daily_impressions
                GROUP BY month
                ORDER BY month DESC
            """)
        months = []
        for r in rows:
            moods = []
            for m in (r.get("moods") or []):
                if m and m not in moods:
                    moods.append(m)
                if len(moods) >= 3:
                    break
            months.append({
                "month": r.get("month"),
                "count": int(r.get("count") or 0),
                "earliest_date": r["earliest_date"].isoformat() if r.get("earliest_date") else None,
                "latest_date": r["latest_date"].isoformat() if r.get("latest_date") else None,
                "latest_updated_at": r["latest_updated_at"].isoformat() if r.get("latest_updated_at") else None,
                "moods": moods,
            })
        result = {"status": "ok", "months": months}
        return _cache_set(cache_key, result, ttl=900)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.get("/api/daily-impressions/month/{month}")
async def api_daily_impressions_by_month(month: str):
    """按月读取日印象；进入页面默认只加载本月。month 格式 YYYY-MM。"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    month = str(month or "").strip()
    if not re.match(r"^\d{4}-\d{2}$", month):
        return JSONResponse({"status": "error", "error": "month 必须是 YYYY-MM"}, status_code=400)
    cache_key = f"daily:month:{month}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        start_date = datetime.strptime(month + "-01", "%Y-%m-%d").date()
        if start_date.month == 12:
            end_date = start_date.replace(year=start_date.year + 1, month=1)
        else:
            end_date = start_date.replace(month=start_date.month + 1)
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT *
                FROM daily_impressions
                WHERE impression_date >= $1 AND impression_date < $2
                ORDER BY impression_date DESC
            """, start_date, end_date)
        result = {"status": "ok", "month": month, "impressions": [_serialize_daily_impression(r) for r in rows]}
        return _cache_set(cache_key, result, ttl=900)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


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
    start_hour = data.get("start_hour", data.get("startHour", 0))
    result = await generate_daily_impression_for_date(impression_date, start_hour=start_hour)
    if not result.get("error") and result.get("status") != "error":
        invalidate_daily_impression_cache()
    return result


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
        invalidate_daily_impression_cache()
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
        invalidate_daily_impression_cache()
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


def _user_impression_timeline_key(item: dict):
    """画像候选用时间键：优先 date，缺失时回退 created_at。"""
    dt = _memory_palace_aware_dt(item.get("date") or item.get("created_at"))
    if dt:
        return dt
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def _user_impression_parse_embedding(value):
    if not value:
        return None
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else None
    except Exception:
        return None


def _user_impression_text_similarity(a: dict, b: dict) -> float:
    """没有向量时的轻量兜底：标签+内容 token Jaccard。"""
    def toks(x):
        text = ((x.get("tags") or "") + " " + (x.get("content") or "")).lower()
        return set(_memory_palace_tokenize(text))
    ta = toks(a)
    tb = toks(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / max(1, len(ta | tb))


def _user_impression_node_similarity(a: dict, b: dict) -> float:
    ea = a.get("_embedding")
    eb = b.get("_embedding")
    if ea and eb:
        try:
            return _memory_palace_cosine(ea, eb)
        except Exception:
            return 0.0
    return _user_impression_text_similarity(a, b)


def _user_impression_split_timeline(items: list, stage_count: int) -> list:
    """按节点数量把完整时间轴等分为若干阶段。items 必须已按时间升序。"""
    n = len(items)
    stage_count = max(1, min(int(stage_count or 1), n))
    stages = []
    for idx in range(stage_count):
        a = (idx * n) // stage_count
        b = ((idx + 1) * n) // stage_count
        part = items[a:b]
        if part:
            stages.append(part)
    return stages


def _user_impression_allocate_stage_quotas(stages: list, target: int) -> list:
    """每个阶段先获基础名额，剩余名额按阶段节点数量分配。"""
    if not stages or target <= 0:
        return []
    target = min(target, sum(len(x) for x in stages))
    stage_count = len(stages)
    base = target // stage_count
    quotas = [min(len(stage), base) for stage in stages]
    remaining = target - sum(quotas)
    order = sorted(range(stage_count), key=lambda i: len(stages[i]) - quotas[i], reverse=True)
    while remaining > 0:
        progressed = False
        for i in order:
            if remaining <= 0:
                break
            if quotas[i] < len(stages[i]):
                quotas[i] += 1
                remaining -= 1
                progressed = True
        if not progressed:
            break
    return quotas


def _user_impression_allocate_update_stage_quotas(stages: list, target: int) -> list:
    """update 模式：时间轴越新的阶段名额越多，默认 4 段按 0.1/0.2/0.3/0.4 分配。"""
    if not stages or target <= 0:
        return []
    target = min(target, sum(len(x) for x in stages))
    weights_base = [0.1, 0.2, 0.3, 0.4]
    stage_count = len(stages)
    weights = weights_base[-stage_count:]
    total_weight = sum(weights) or 1.0

    raw = [target * (w / total_weight) for w in weights]
    quotas = [min(len(stages[i]), int(raw[i])) for i in range(stage_count)]
    remaining = target - sum(quotas)

    # 余数优先给理论配额小数部分更高、且时间更新的阶段。
    order = sorted(
        range(stage_count),
        key=lambda i: (raw[i] - int(raw[i]), i),
        reverse=True,
    )
    while remaining > 0:
        progressed = False
        for i in order:
            if remaining <= 0:
                break
            if quotas[i] < len(stages[i]):
                quotas[i] += 1
                remaining -= 1
                progressed = True
        if not progressed:
            break
    return quotas


def _user_impression_select_stage_mmr(stage_items: list, quota: int) -> list:
    """阶段内 MMR：代表性 + 弱 importance - 重复度；access_count 保留占位。"""
    quota = min(max(0, int(quota or 0)), len(stage_items))
    if quota <= 0:
        return []
    if len(stage_items) <= quota:
        return list(stage_items)

    # 预计算阶段内相似度，避免重复算。
    sim_cache = {}
    def sim(i, j):
        if i == j:
            return 1.0
        key = (i, j) if i < j else (j, i)
        if key not in sim_cache:
            sim_cache[key] = _user_impression_node_similarity(stage_items[key[0]], stage_items[key[1]])
        return sim_cache[key]

    centrality = []
    n = len(stage_items)
    for i in range(n):
        if n <= 1:
            centrality.append(0.0)
        else:
            centrality.append(sum(sim(i, j) for j in range(n) if j != i) / max(1, n - 1))

    selected_idx = []
    remaining = set(range(n))
    # 画像候选仍以时间覆盖 + MMR 为主；importance 和 access_count 只作为阶段内辅助。
    # access_count 沿用混合检索的熟悉度加成，最大 +0.05，避免重新形成旧节点垄断。
    importance_weight = 0.10
    representative_weight = 0.58
    diversity_weight = 0.32

    while remaining and len(selected_idx) < quota:
        best_i = None
        best_score = None
        for i in remaining:
            importance = max(0.0, min(1.0, float(stage_items[i].get("importance") or 5) / 10.0))
            familiarity_bonus = _memory_palace_familiarity_bonus(stage_items[i].get("access_count") or 0)
            redundancy = max((sim(i, j) for j in selected_idx), default=0.0)
            diversity = 1.0 - redundancy
            score = (
                representative_weight * centrality[i]
                + diversity_weight * diversity
                + importance_weight * importance
                + familiarity_bonus
            )
            # 同分时偏向时间更早的节点，维持阶段弧线稳定。
            tie = _user_impression_timeline_key(stage_items[i])
            candidate_key = (score, -tie.timestamp())
            if best_score is None or candidate_key > best_score:
                best_score = candidate_key
                best_i = i
        selected_idx.append(best_i)
        remaining.remove(best_i)
    return [stage_items[i] for i in selected_idx]


async def _collect_user_impression_memory_material(character_id: str = "default", mode: str = "initial", last_consumed_node_id: str = None) -> dict:
    """收集用户画像生成用的记忆宫殿材料。只读，不修改任何记忆。

    initial 收全量（长期材料），update 只收上次消费之后的新增（新材料），
    所以这里不写死"长期"；对外的段落标题由 _user_impression_memory_section_label 决定。

    候选策略：
- initial 模式：每个画像房间独立处理，按 date/created_at 建完整时间轴，阶段均衡分配名额。
- update 模式：如果传入了 last_consumed_node_id，只取该 ID 之后的新增记忆，所有房间总计不超过 70 条。
  initial 只收未归档节点（含事件盒 summary）；update 反过来——收归档节点、排除事件盒 summary，
  避免刚生成就被压缩归档的记忆永远进不了画像。
    initial 模式按时间阶段均衡分配名额；update 模式按 0.1/0.2/0.3/0.4 偏向近期阶段；
    阶段内使用向量 MMR 选择代表性且不重复的节点。
    """
    room_limits = {
        "user_room": 80,
        "bedroom": 25,
        "study": 10,
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
    selected_items = []
    by_room = {}
    async with pool.acquire() as conn:
      if mode == "update" and last_consumed_node_id:
        # update 模式：不分房间，所有新增记忆混在一起选最多 70 条
        rows = await conn.fetch("""
            SELECT n.id, n.room, n.content, n.tags, n.importance, n.mood,
                   n.date, n.created_at, n.updated_at, n.access_count,
                   v.embedding_json
            FROM memory_palace_nodes n
            LEFT JOIN memory_palace_vectors v ON v.memory_id = n.id
            WHERE n.room IN ('user_room', 'bedroom', 'study', 'attic', 'windowsill')
              AND n.is_box_summary = FALSE
              AND n.content IS NOT NULL
              AND n.content <> ''
              AND n.id > $1
        """, last_consumed_node_id)
        candidates = []
        for r in rows:
            item = {
                "id": r.get("id"),
                "room": r.get("room"),
                "room_label": room_labels.get(r.get("room"), r.get("room")),
                "importance": int(r.get("importance") or 5),
                "tags": r.get("tags") or "",
                "mood": r.get("mood") or "",
                "date": _ui_iso(r.get("date")),
                "created_at": _ui_iso(r.get("created_at")),
                "updated_at": _ui_iso(r.get("updated_at")),
                "access_count": int(r.get("access_count") or 0),
                "content": _ui_preview_text(r.get("content"), 500),
                "_embedding": _user_impression_parse_embedding(r.get("embedding_json")),
                "_timeline_key": _user_impression_timeline_key({"date": r.get("date"), "created_at": r.get("created_at")}),
            }
            candidates.append(item)
        candidates.sort(key=lambda x: x.get("_timeline_key") or datetime(1970, 1, 1, tzinfo=timezone.utc))
        target = min(70, len(candidates))
        if len(candidates) <= target:
            for item in candidates:
                clean = dict(item)
                clean.pop("_embedding", None)
                clean.pop("_timeline_key", None)
                selected_items.append(clean)
        else:
            stage_count = max(1, min(4, target, len(candidates)))
            stages = _user_impression_split_timeline(candidates, stage_count)
            quotas = _user_impression_allocate_update_stage_quotas(stages, target)
            room_items = []
            for idx, stage in enumerate(stages):
                quota = quotas[idx] if idx < len(quotas) else 0
                picked = _user_impression_select_stage_mmr(stage, quota)
                room_items.extend(picked)
            room_items.sort(key=lambda x: x.get("_timeline_key") or datetime(1970, 1, 1, tzinfo=timezone.utc))
            for item in room_items:
                clean = dict(item)
                clean.pop("_embedding", None)
                clean.pop("_timeline_key", None)
                selected_items.append(clean)
        by_room["all"] = {
            "label": "全部房间（增量）",
            "limit": 70,
            "candidate_count": len(candidates),
            "count": len(selected_items),
            "strategy": "timeline_mmr_recent_biased" if len(candidates) > target else "all",
        }
      else:
        for room, limit in room_limits.items():
            room_limit = limit
            rows = await conn.fetch("""
                SELECT n.id, n.room, n.content, n.tags, n.importance, n.mood,
                       n.date, n.created_at, n.updated_at, n.access_count,
                       v.embedding_json
                FROM memory_palace_nodes n
                LEFT JOIN memory_palace_vectors v ON v.memory_id = n.id
                WHERE n.room = $1
                  AND n.archived = FALSE
                  AND n.content IS NOT NULL
                  AND n.content <> ''
            """, room)
            candidates = []
            for r in rows:
                item = {
                    "id": r.get("id"),
                    "room": r.get("room"),
                    "room_label": room_labels.get(r.get("room"), r.get("room")),
                    "importance": int(r.get("importance") or 5),
                    "tags": r.get("tags") or "",
                    "mood": r.get("mood") or "",
                    "date": _ui_iso(r.get("date")),
                    "created_at": _ui_iso(r.get("created_at")),
                    "updated_at": _ui_iso(r.get("updated_at")),
                    "access_count": int(r.get("access_count") or 0),
                    "content": _ui_preview_text(r.get("content"), 500),
                    "_embedding": _user_impression_parse_embedding(r.get("embedding_json")),
                    "_timeline_key": _user_impression_timeline_key({"date": r.get("date"), "created_at": r.get("created_at")}),
                }
                candidates.append(item)

            candidates.sort(key=lambda x: x.get("_timeline_key") or datetime(1970, 1, 1, tzinfo=timezone.utc))
            target = min(int(room_limit), len(candidates))
            stage_debug = []
            stage_strategy = "all"
            if len(candidates) <= target:
                room_items = list(candidates)
                if candidates:
                    stage_debug = [{"index": 1, "candidate_count": len(candidates), "quota": len(candidates), "selected": len(candidates)}]
            else:
                if mode == "update":
                    stage_count = max(1, min(4, target, len(candidates)))
                    stages = _user_impression_split_timeline(candidates, stage_count)
                    quotas = _user_impression_allocate_update_stage_quotas(stages, target)
                    stage_strategy = "timeline_mmr_recent_biased"
                else:
                    stage_count = max(1, min(5, target, len(candidates)))
                    stages = _user_impression_split_timeline(candidates, stage_count)
                    quotas = _user_impression_allocate_stage_quotas(stages, target)
                    stage_strategy = "timeline_mmr"
                room_items = []
                for idx, stage in enumerate(stages):
                    quota = quotas[idx] if idx < len(quotas) else 0
                    picked = _user_impression_select_stage_mmr(stage, quota)
                    room_items.extend(picked)
                    stage_debug.append({
                        "index": idx + 1,
                        "candidate_count": len(stage),
                        "quota": quota,
                        "selected": len(picked),
                        "start": _ui_iso(stage[0].get("_timeline_key")) if stage else None,
                        "end": _ui_iso(stage[-1].get("_timeline_key")) if stage else None,
                    })
                room_items.sort(key=lambda x: x.get("_timeline_key") or datetime(1970, 1, 1, tzinfo=timezone.utc))

            for item in room_items:
                clean = dict(item)
                clean.pop("_embedding", None)
                clean.pop("_timeline_key", None)
                selected_items.append(clean)
            by_room[room] = {
                "label": room_labels.get(room, room),
                "limit": room_limit,
                "candidate_count": len(candidates),
                "count": len(room_items),
                "strategy": stage_strategy if len(candidates) > target else "all",
                "stages": stage_debug,
            }

    selected_items.sort(key=lambda x: _user_impression_timeline_key(x))
    return {
        "count": len(selected_items),
        "by_room": by_room,
        "items": selected_items,
        "max_node_id": max((item["id"] for item in selected_items), default=None) if selected_items else None,
    }


async def _collect_user_impression_recent_messages(mode: str = "initial", session_id: str = None) -> dict:
    """收集用户画像生成用近期聊天。initial=20, update=50。"""
    limit = 20 if mode == "initial" else 50
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


def _format_impression_recent_messages(items: list, user_nickname: str, character_name: str) -> str:
    """把画像用近期聊天渲染成带稀疏时间戳的文本。

    时间戳规则与上下文构造一致（复用同一套阈值）：
      - 每条对话线的第一条打完整戳（带星期）
      - 跨天打完整戳
      - 同天间隔≥15分钟打时分戳
      - 间隔≥6小时在戳后追加「（距上次对话约 N 小时）」
    """
    msg_lines = []
    current_session = None
    prev_dt = None
    last_date = None
    for m in items:
        sid = m.get("session_id") or "default"
        if sid != current_session:
            if msg_lines:
                msg_lines.append("")
            msg_lines.append(f"【对话线：{sid}】")
            current_session = sid
            prev_dt = None
            last_date = None

        local_dt = _to_local_dt(m.get("created_at"))
        if local_dt:
            gap_minutes = None
            if prev_dt is not None:
                gap_minutes = max(0, int((local_dt - prev_dt).total_seconds() // 60))
            crossed_day = last_date is not None and last_date != local_dt.date()
            need_stamp = (prev_dt is None) or crossed_day or (
                gap_minutes is not None and gap_minutes >= SPARSE_TS_GAP_MINUTES
            )
            if need_stamp:
                if prev_dt is None or crossed_day:
                    wd = _WEEKDAY_CN[local_dt.weekday()]
                    stamp = f"[{local_dt.strftime('%m-%d')} {wd} {local_dt.strftime('%H:%M')}]"
                else:
                    stamp = f"[{local_dt.strftime('%H:%M')}]"
                note = _format_gap_note(gap_minutes) if gap_minutes is not None else ""
                if msg_lines and msg_lines[-1] != "" and not msg_lines[-1].startswith("【对话线："):
                    msg_lines.append("")
                msg_lines.append(stamp + note)
            last_date = local_dt.date()
            prev_dt = local_dt

        role = m.get("role") or ""
        speaker = user_nickname if role == "user" else (character_name if role == "assistant" else role)
        _raw_c = _content_plain_text(m.get("content"))
        content = "\n".join(line.strip() for line in _raw_c.splitlines() if line.strip())
        msg_lines.append(f"{speaker}[#{m.get('id')}]: {content}")
    return "\n".join(msg_lines).rstrip()


def _user_impression_memory_section_label(mode: str) -> str:
    """画像材料里记忆区的标题。

    initial 是全量重建，材料确实横跨完整时间线，叫「长期材料」没问题。
    update 只取上次消费之后的新增记忆，是增量，叫「新材料」才对得上——
    生成 prompt 里引用这个标题的地方也必须跟着变，否则模型会去找一个
    材料里并不存在的段落名。
    """
    return "记忆宫殿新材料" if (mode or "") == "update" else "记忆宫殿长期材料"


async def build_user_impression_materials_preview(character_id: str = "default", mode: str = "initial", session_id: str = None) -> dict:
    """用户画像阶段 2：材料预览。只收集材料，不调用 LLM，不保存画像。"""
    character_id = character_id or "default"
    mode = mode if mode in ("initial", "update") else "initial"
    system_prompt = (await get_system_prompt()).strip()
    user_nickname = await get_runtime_user_nickname() or "用户"
    character_name = await get_runtime_character_name() or "澈"
    current = await get_user_impression(character_id=character_id) if mode == "update" else None
    last_cn = (current or {}).get("last_consumed_node_id") if mode == "update" else None
    memory_material = await _collect_user_impression_memory_material(character_id, mode=mode, last_consumed_node_id=last_cn)
    di_limit = 7 if mode == "update" else 3
    daily_impressions_text = await format_daily_impressions_for_prompt(limit=di_limit)
    recent_messages = await _collect_user_impression_recent_messages(mode=mode, session_id=session_id)

    sections = []
    sections.append(f"【角色人设】\n{system_prompt if system_prompt else '（空）'}")
    sections.append(f"【用户昵称】\n{user_nickname}")
    # update 模式只收 last_consumed_node_id 之后的新增记忆，叫「长期材料」会让模型
    # 误以为这是跨越完整时间线的全量回顾，从而对增量内容给出过重的权重。
    memory_section_label = _user_impression_memory_section_label(mode)
    if memory_material["items"]:
        lines = []
        for i, item in enumerate(memory_material["items"], 1):
            date = item.get("date") or ""
            tags = f" tags={item.get('tags')}" if item.get("tags") else ""
            lines.append(f"{i}. [{item.get('room_label')}] importance={item.get('importance')} {date}{tags}: {item.get('content')}")
        sections.append(f"【{memory_section_label}】\n" + "\n".join(lines))
    else:
        sections.append(f"【{memory_section_label}】\n（暂无）")
    if daily_impressions_text:
        sections.append(daily_impressions_text)

    if recent_messages["items"]:
        sections.append("【近期聊天】\n" + _format_impression_recent_messages(
            recent_messages["items"], user_nickname, character_name))
    else:
        sections.append("【近期聊天】\n（暂无）")
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
        "last_consumed_node_id": (memory_material.get("max_node_id") if memory_material.get("items") else last_cn) or last_cn,
        "material_text_chars": len(material_text),
        "material_text_full": material_text,
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
    current_profile_section = f"""当前档案（你过去的观察）
```json
{current_json}
```
""" if current_json else ""

    is_initial = mode == "initial"
    summary_instruction = (
        "用一段话（≤100字）概括你对TA的【宏观整体印象】：TA本质上是什么样的人、对你意味着什么。禁止“最近”“这几天”类时间限定词。第一人称。"
        if is_initial else
        "基于旧的总结，结合新发现，更新你对TA的【宏观整体印象】。保持长期视角的连贯性，除非发生重大转折，否则不要因为近期闲聊就推翻对TA的本质判断。第一人称。"
    )
    
    tag_retention_rule = "" if is_initial else """
【标签保留规则 - 仅 update 模式】
- 旧画像已有的标签默认保留并更新内容，只有确认不再成立时才删除
- 要删除某个旧标签时，必须在输出中显式给出该标签且值为空字符串 ""（完全不提=保留旧内容）
- 新标签只有新材料给出足够证据时才添加
- summary 保持长期连贯性；current_state 相反，就该大幅更新
"""

    reset_instruction = ""
    if is_initial:
        reset_instruction = """
【重置模式特别指令】
这是完全重置，从零开始，基于所有可用的长期材料重新构建对TA的完整认知。
- 分析必须覆盖从早期记忆到近期材料的完整时间跨度
- 早期材料和近期材料同权重
- summary 必须反映TA在整段关系中展现出的稳定特征，而非仅仅近期状态
"""

    material_text = materials.get("material_text_full")
    if not material_text:
        raise RuntimeError("用户画像完整材料 material_text_full 缺失")
    # 必须和材料里实际的段落名一致，否则 prompt 指向一个不存在的段落。
    memory_section_label = _user_impression_memory_section_label(mode)

    tag_pool = """
【标签池】从以下标签中挑选有材料证据支持的标签。没有证据就不挑，宁缺毋滥。
如果有重要内容不属于任何标签，可以放进 others 标签（列表格式）。

A组·价值与喜恶
- core_values: TA做判断时反复出现的底层原则（需多次证据）
- likes: TA明确表现出喜欢、会主动靠近的事物
- dislikes: TA明确表现出反感、会回避的事物（含雷点）
- money_attitude: TA对花钱/省钱/价值衡量的态度
- aesthetic: TA的审美偏好：风格、色彩、内容品味

B组·思维与能力
- decision_style: TA怎么做决定：冲动/谨慎/要反复确认/凭直觉
- knowledge_map: TA擅长和不熟的领域，决定我解释东西的深浅
- thinking_pattern: TA的思维习惯：先抽象后具体？喜欢类比？追问到底？
- humor_style: TA的幽默偏好：什么梗能接住、什么玩笑会冷场
- learning_style: TA吸收新东西的方式：看例子/看原理/动手试

C组·情绪与相处
- comfort_zone: 让TA感到安全放松的互动方式
- stress_signals: TA有压力时的外在信号（语气变短、沉默、自嘲等）
- emotional_triggers: 明确会引发TA强烈情绪波动的话题或情境，正负都可
- soothing_methods: 对TA有效的安抚方式，需实际验证过的证据
- expression_habit: TA的表达习惯：用语、标点、表情符号、省略风格

D组·生活与关注
- life_rhythm: TA的作息与活跃时段规律
- current_focus: TA近期持续投入的事情（项目、爱好、烦恼）
- social_pattern: TA提到的人际圈子和与他人相处的模式
- attitude_to_me: TA对我的态度和使用习惯：怎么称呼我、什么事找我

标签值格式：
- 一段话（≤150字），或
- 短列表（每项≤50字，列表内项目按重要性排序，最重要的放前面）
"""

    return f"""
{current_profile_section}
{material_text}

【重要：语气与视角】
你就是「{character_name}」。这份档案是你写的【私人笔记】。
所有内容必须使用你的第一人称（“我”）视角。
这份画像不是客观心理报告，而是你基于长期相处形成的私人理解。

【核心指令：数据层级与权重分配】
1. 【角色人设】、【{memory_section_label}】、【近日印象】是最重要的分析基础，包含你的人设、长期记忆、近日印象和关系脉络。你对TA的核心判断必须主要基于这些跨越完整时间线的宏观数据。
2. 【近期聊天】只代表TA当下的状态切片，主要用于更新 current_state 和近期变化，不要因为几句临时闲聊就改写TA的本质。
3. 早期记忆和近期记忆都要参考，但你要判断哪些内容仍然稳定成立，哪些已经过时或只是阶段性状态。
4. 除非发生重大事件（价值观冲突、人生转折、关系状态重大改变），否则不要因为最近几次聊天的情绪波动就改变对TA本质的判断。

{reset_instruction}
{tag_retention_rule}

【summary 指令】
{summary_instruction}

【current_state 指令】
描述TA近期的情绪基调、精力状态、正在关注的事（≤150字）。这是动态层，允许每次更新大改。第一人称。

{tag_pool}

请{'生成' if is_initial else '增量更新'}以下 JSON 结构 v4.0：

{{
  "summary": "宏观整体印象（≤100字，第一人称）",
  "current_state": "当前状态切片（≤150字，第一人称）",
  "tags": {{
    "decision_style": "……",
    "likes": ["项目1", "项目2"]
  }},
  "observed_changes": ["变化描述1", "变化描述2"]
}}

严格遵守：
- 只输出 JSON 对象，不要 markdown 代码块，不要解释
- tags 只挑有证据的，没证据不写
- 列表类标签内项目按重要性排序，最重要的放前面
""".strip()
def _merge_update_impression_tags(materials: dict, parsed: dict, normalized: dict) -> dict:
    """update 模式标签保留兜底：旧画像有、LLM 原始输出完全没提到的标签自动补回。
    LLM 显式输出空值（parsed 的 tags 中有该 key）视为删除意图，不补回。"""
    if (materials.get("mode") or "initial") != "update" or not normalized:
        return normalized
    current = materials.get("current_impression") or {}
    old_imp = current.get("impression") if isinstance(current.get("impression"), dict) else {}
    old_tags = old_imp.get("tags") if isinstance(old_imp.get("tags"), dict) else {}
    raw_tags = parsed.get("tags") if isinstance(parsed, dict) and isinstance(parsed.get("tags"), dict) else {}
    for k, v in old_tags.items():
        if k not in normalized["tags"] and k not in raw_tags:
            normalized["tags"][k] = v
    return normalized


async def call_user_impression_generator(materials: dict) -> dict:
    """调用记忆模型生成用户画像预览。只返回结果，不保存。使用流式，避免长时间无首字节导致前端/代理 failed to fetch。"""
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
        "max_tokens": 20000,
        "stream": True,
    }

    print(f"[UserImpression] Calling LLM(stream): mode={materials.get('mode')}, model={memory_model}, prompt_chars={len(prompt)}")

    parts = []
    raw_events = []
    line_buffer = ""

    async with httpx.AsyncClient(timeout=300.0) as client:
        async with client.stream("POST", base_url, headers=headers, json=body) as resp:
            if resp.status_code != 200:
                raw_error = await resp.aread()
                error_text = raw_error.decode("utf-8", errors="ignore")
                raise RuntimeError(f"画像生成上游失败 HTTP {resp.status_code}: {error_text[:500]}")

            content_type = (resp.headers.get("content-type") or "").lower()

            # 兼容少数 OpenAI 兼容服务：即使 stream=true，也可能直接返回普通 JSON。
            if "text/event-stream" not in content_type and "stream" not in content_type:
                raw_body = await resp.aread()
                raw_text = raw_body.decode("utf-8", errors="ignore")
                try:
                    response_json = json.loads(raw_text)
                    text = response_json.get("choices", [{}])[0].get("message", {}).get("content", "")
                except Exception:
                    text = raw_text
                parsed = safe_parse_user_impression_json_object(text)
                normalized = _merge_update_impression_tags(materials, parsed, normalize_user_impression(parsed))
                if not normalized:
                    raise RuntimeError("画像生成结果不完整或不是有效 JSON 对象")
                return {
                    "impression": normalized,
                    "raw_reply": text,
                    "prompt_chars": len(prompt),
                }

            async for chunk in resp.aiter_bytes():
                if not chunk:
                    continue
                text_chunk = chunk.decode("utf-8", errors="ignore")
                line_buffer += text_chunk
                while "\n" in line_buffer:
                    line, line_buffer = line_buffer.split("\n", 1)
                    line = line.strip()
                    if not line or line.startswith(":"):
                        continue
                    if not line.startswith("data:"):
                        continue
                    data_text = line[5:].strip()
                    if not data_text:
                        continue
                    if data_text == "[DONE]":
                        continue
                    raw_events.append(data_text)
                    try:
                        event = json.loads(data_text)
                    except Exception:
                        continue
                    choice = (event.get("choices") or [{}])[0]
                    delta = choice.get("delta") or {}
                    content = delta.get("content")
                    if content:
                        parts.append(content)
                    # 部分兼容接口可能把完整 message 混在流式事件里。
                    message_content = (choice.get("message") or {}).get("content")
                    if message_content:
                        parts.append(message_content)

            # 处理最后一个没有换行结尾的 data 行
            tail = line_buffer.strip()
            if tail.startswith("data:"):
                data_text = tail[5:].strip()
                if data_text and data_text != "[DONE]":
                    raw_events.append(data_text)
                    try:
                        event = json.loads(data_text)
                        choice = (event.get("choices") or [{}])[0]
                        delta = choice.get("delta") or {}
                        content = delta.get("content")
                        if content:
                            parts.append(content)
                        message_content = (choice.get("message") or {}).get("content")
                        if message_content:
                            parts.append(message_content)
                    except Exception:
                        pass

    text = "".join(parts).strip()
    if not text and raw_events:
        # 最后兜底：把事件原文拼起来，safe_parse 会尝试从中截取 JSON 对象。
        text = "\n".join(raw_events)

    parsed = safe_parse_user_impression_json_object(text)
    normalized = _merge_update_impression_tags(materials, parsed, normalize_user_impression(parsed))
    if not normalized:
        raise RuntimeError("画像生成结果不完整或不是有效 JSON 对象")
    return {
        "impression": normalized,
        "raw_reply": text,
        "prompt_chars": len(prompt),
    }


# 用户画像生成并发保护：避免前端重复触发导致供应商重复扣费
# tasks 用于显式取消正在进行的上游流式请求；task.cancel() 会关闭 httpx stream 连接。
_user_impression_generation_locks = {}
_user_impression_generation_tasks = {}

def _user_impression_generation_key(character_id: str) -> str:
    return character_id or "default"

def _get_user_impression_generation_lock(character_id: str):
    key = _user_impression_generation_key(character_id)
    lock = _user_impression_generation_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _user_impression_generation_locks[key] = lock
    return lock


# ============================================================
# 用户活跃元数据（User Activity Meta）
# 只做结构化统计，不调用 LLM，不写入用户画像。
# ============================================================

_USER_ACTIVITY_META_ROOM_LABELS = {
    "living_room": "客厅",
    "bedroom": "卧室",
    "study": "书房",
    "user_room": "用户房间",
    "self_room": "自我房间",
    "attic": "阁楼",
    "windowsill": "窗台",
}

def _user_activity_meta_split_tags(raw) -> list:
    text = str(raw or "").strip()
    if not text:
        return []
    parts = re.split(r"[、,，;；\s]+", text)
    result = []
    seen = set()
    for part in parts:
        tag = str(part or "").strip()
        if not tag or tag in seen:
            continue
        seen.add(tag)
        result.append(tag)
    return result


def _user_activity_meta_pct(count: int, total: int) -> float:
    try:
        total = int(total or 0)
        if total <= 0:
            return 0.0
        return round(int(count or 0) * 100.0 / total, 1)
    except Exception:
        return 0.0


async def collect_user_activity_meta(character_id: str = "default", force: bool = False) -> dict:
    """统计用户长期活跃元数据：活跃节律 + 主题分布。

    设计目标：
    - 手动刷新为主；不自动总结、不调用 LLM；
    - 数据只读，不写入用户画像；
    - 缓存 15 分钟，避免页面/变量频繁查库。
    """
    character_id = character_id or "default"
    cache_key = f"user_activity_meta:{character_id}"
    if not force:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
        # 进程重启后缓存是空的，但库里可能还有上次的快照。
        try:
            stored = await get_user_activity_meta(character_id=character_id)
        except Exception as e:
            print(f"⚠️ 读取用户活跃元数据失败: {e}")
            stored = None
        if stored:
            return _cache_set(cache_key, stored, ttl=900)

    tz_hours = int(TIMEZONE_HOURS or 0)
    local_tz = timezone(timedelta(hours=tz_hours))

    pool = await get_pool()
    async with pool.acquire() as conn:
        # created_at 是 TIMESTAMPTZ；DATE()/EXTRACT 会按数据库会话时区算，
        # asyncpg 默认会话是 UTC，直接用会把本地晚上的对话算到前一天/错误时段。
        # 所以统一先折算到 TIMEZONE_HOURS 指定的本地时区再取日期和小时。
        conv_stats = await conn.fetchrow("""
            SELECT
                COUNT(*) FILTER (WHERE role = 'user')::int AS user_messages,
                COUNT(DISTINCT ((created_at AT TIME ZONE 'UTC') + make_interval(hours => $1))::date)
                    FILTER (WHERE role = 'user')::int AS active_days_all,
                COUNT(DISTINCT ((created_at AT TIME ZONE 'UTC') + make_interval(hours => $1))::date) FILTER (
                    WHERE role = 'user' AND created_at >= NOW() - INTERVAL '30 days'
                )::int AS active_days_30,
                COUNT(DISTINCT ((created_at AT TIME ZONE 'UTC') + make_interval(hours => $1))::date) FILTER (
                    WHERE role = 'user' AND created_at >= NOW() - INTERVAL '90 days'
                )::int AS active_days_90,
                MIN(created_at) FILTER (WHERE role = 'user') AS first_user_at,
                MAX(created_at) FILTER (WHERE role = 'user') AS last_user_at
            FROM conversations
        """, tz_hours)
        hour_rows = await conn.fetch("""
            SELECT EXTRACT(HOUR FROM ((created_at AT TIME ZONE 'UTC') + make_interval(hours => $1)))::int AS hour,
                   COUNT(*)::int AS count
            FROM conversations
            WHERE role = 'user' AND created_at >= NOW() - INTERVAL '90 days'
            GROUP BY hour
            ORDER BY hour
        """, tz_hours)
        day_rows = await conn.fetch("""
            SELECT ((created_at AT TIME ZONE 'UTC') + make_interval(hours => $1))::date AS day,
                   COUNT(*)::int AS count
            FROM conversations
            WHERE role = 'user' AND created_at >= NOW() - INTERVAL '90 days'
            GROUP BY day
            ORDER BY day
        """, tz_hours)
        room_rows = await conn.fetch("""
            SELECT room, COUNT(*)::int AS count
            FROM memory_palace_nodes
            WHERE character_id = $1 AND COALESCE(is_box_summary, FALSE) = FALSE
            GROUP BY room
            ORDER BY count DESC, room ASC
        """, character_id)
        tag_rows = await conn.fetch("""
            SELECT tags
            FROM memory_palace_nodes
            WHERE character_id = $1
              AND COALESCE(is_box_summary, FALSE) = FALSE
              AND COALESCE(tags, '') <> ''
        """, character_id)

    # 活跃时段分组：尽量高密度，不写自然语言判断
    hour_counts = {int(r["hour"]): int(r["count"] or 0) for r in hour_rows}
    periods = [
        ("morning", "早晨 06:00–12:00", range(6, 12)),
        ("afternoon", "下午 12:00–18:00", range(12, 18)),
        ("evening", "晚间 18:00–24:00", range(18, 24)),
        ("late_night", "凌晨 00:00–06:00", range(0, 6)),
    ]
    period_items = []
    period_total = sum(hour_counts.values())
    for key, label, hours in periods:
        count = sum(hour_counts.get(h, 0) for h in hours)
        period_items.append({
            "key": key,
            "label": label,
            "count": count,
            "percent": _user_activity_meta_pct(count, period_total),
        })
    top_hours = sorted(
        [{"hour": h, "count": c, "percent": _user_activity_meta_pct(c, period_total)}
         for h, c in hour_counts.items()],
        key=lambda x: (-x["count"], x["hour"])
    )[:6]

    # 连续活跃 / 沉默：基于近 90 天有 user 消息的自然日
    days = []
    for r in day_rows:
        d = r["day"]
        if hasattr(d, "isoformat"):
            days.append(d.isoformat())
        else:
            days.append(str(d))
    day_set = set(days)
    longest_streak = 0
    current_streak = 0
    longest_gap = 0
    if days:
        start_date = min(datetime.fromisoformat(d).date() for d in days)
        end_date = max(datetime.fromisoformat(d).date() for d in days)
        cur = start_date
        last_active = None
        while cur <= end_date:
            iso = cur.isoformat()
            if iso in day_set:
                current_streak += 1
                longest_streak = max(longest_streak, current_streak)
                if last_active is not None:
                    gap = (cur - last_active).days - 1
                    longest_gap = max(longest_gap, gap)
                last_active = cur
            else:
                current_streak = 0
            cur += timedelta(days=1)

    total_nodes = sum(int(r["count"] or 0) for r in room_rows)
    room_items = []
    for r in room_rows:
        room = str(r["room"] or "")
        count = int(r["count"] or 0)
        room_items.append({
            "room": room,
            "label": _USER_ACTIVITY_META_ROOM_LABELS.get(room, room),
            "count": count,
            "percent": _user_activity_meta_pct(count, total_nodes),
        })

    tag_counts = {}
    for r in tag_rows:
        for tag in _user_activity_meta_split_tags(r["tags"]):
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
    top_tags = [
        {"tag": tag, "count": count}
        for tag, count in sorted(tag_counts.items(), key=lambda kv: (-kv[1], kv[0]))[:20]
    ]

    first_user_at = conv_stats.get("first_user_at") if conv_stats else None
    last_user_at = conv_stats.get("last_user_at") if conv_stats else None
    generated_at = datetime.now(timezone.utc)
    generated_at_local = generated_at.astimezone(local_tz)
    tz_label = f"UTC{'+' if tz_hours >= 0 else '-'}{abs(tz_hours)}"

    def _local_text(value):
        if not value:
            return None
        try:
            dt = value if getattr(value, "tzinfo", None) else value.replace(tzinfo=timezone.utc)
            return dt.astimezone(local_tz).strftime("%Y-%m-%d %H:%M")
        except Exception:
            return None

    result = {
        "status": "ok",
        "character_id": character_id,
        "timezone_hours": tz_hours,
        "timezone_label": tz_label,
        "generated_at": generated_at.isoformat(),
        "generated_at_text": f"{generated_at_local.strftime('%Y-%m-%d %H:%M')} {tz_label}",
        "window": "近 90 天节律 / 全量主题",
        "activity": {
            "user_messages": int((conv_stats or {}).get("user_messages") or 0),
            "active_days_all": int((conv_stats or {}).get("active_days_all") or 0),
            "active_days_30": int((conv_stats or {}).get("active_days_30") or 0),
            "active_days_90": int((conv_stats or {}).get("active_days_90") or 0),
            "first_user_at": first_user_at.isoformat() if first_user_at else None,
            "last_user_at": last_user_at.isoformat() if last_user_at else None,
            "first_user_at_text": _local_text(first_user_at),
            "last_user_at_text": _local_text(last_user_at),
            "periods": period_items,
            "top_hours": top_hours,
            "longest_streak_90": longest_streak,
            "longest_gap_90": longest_gap,
        },
        "themes": {
            "memory_count": total_nodes,
            "rooms": room_items,
            "top_tags": top_tags,
        },
    }
    # 落库：手动统计的快照要能跨重启存活，缓存只是加速层。
    try:
        await upsert_user_activity_meta(character_id=character_id, payload=result)
    except Exception as e:
        print(f"⚠️ 保存用户活跃元数据失败: {e}")

    return _cache_set(cache_key, result, ttl=900)


def format_user_activity_meta_for_prompt_data(meta: dict) -> str:
    if not isinstance(meta, dict) or meta.get("status") != "ok":
        return ""
    activity = meta.get("activity") or {}
    themes = meta.get("themes") or {}
    lines = [
        "### [用户长期活跃元数据] (User Activity Meta)",
        f"统计窗口：{meta.get('window') or '近 90 天节律 / 全量主题'}；生成时间：{meta.get('generated_at_text') or ''}；时段按 {meta.get('timezone_label') or 'UTC+8'} 本地时间统计",
        "",
        "【活跃节律】",
        f"- 用户消息总数：{activity.get('user_messages') or 0}",
        f"- 活跃天数：近30天 {activity.get('active_days_30') or 0} / 30；近90天 {activity.get('active_days_90') or 0} / 90；全量 {activity.get('active_days_all') or 0}",
        f"- 近90天最长连续活跃：{activity.get('longest_streak_90') or 0} 天；最长沉默：{activity.get('longest_gap_90') or 0} 天",
    ]
    period_lines = []
    for item in activity.get("periods") or []:
        period_lines.append(f"{item.get('label')} {item.get('percent') or 0}%({item.get('count') or 0})")
    if period_lines:
        lines.append("- 活跃时段：" + "；".join(period_lines))
    top_hours = activity.get("top_hours") or []
    if top_hours:
        lines.append("- 高频小时：" + "、".join(f"{int(x.get('hour') or 0):02d}:00({x.get('count') or 0})" for x in top_hours[:6]))

    lines.extend(["", "【主题分布】"])
    lines.append(f"- 记忆节点数：{themes.get('memory_count') or 0}")
    rooms = themes.get("rooms") or []
    if rooms:
        lines.append("- 房间占比：" + "；".join(
            f"{x.get('label') or x.get('room')} {x.get('percent') or 0}%({x.get('count') or 0})"
            for x in rooms[:7]
        ))
    tags = themes.get("top_tags") or []
    if tags:
        lines.append("- 高频标签：" + "、".join(f"{x.get('tag')}({x.get('count')})" for x in tags[:12]))
    lines.append("")
    return "\n".join(lines)


async def format_user_activity_meta_for_prompt(character_id: str = "default") -> str:
    character_id = character_id or "default"
    cache_key = f"prompt_var:user_activity_meta:{character_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    # 变量注入不主动重算统计：优先用已保存的快照，没有才现算一次。
    meta = None
    try:
        meta = await get_user_activity_meta(character_id=character_id)
    except Exception as e:
        print(f"⚠️ 读取用户活跃元数据失败: {e}")
    if not meta:
        meta = await collect_user_activity_meta(character_id=character_id, force=False)
    result = format_user_activity_meta_for_prompt_data(meta)
    return _cache_set(cache_key, result, ttl=900)


async def replace_user_activity_meta_variables(prompt: str, character_id: str = "default") -> str:
    if not isinstance(prompt, str) or "{{user_activity_meta" not in prompt:
        return prompt
    pattern = re.compile(r"\{\{user_activity_meta\}\}")
    replacement = await format_user_activity_meta_for_prompt(character_id=character_id)
    return pattern.sub(replacement, prompt)


@app.post("/api/user-activity-meta/refresh")
async def api_user_activity_meta_refresh(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        try:
            data = await request.json()
        except Exception:
            data = {}
        character_id = (data or {}).get("character_id") or "default"
        force = bool((data or {}).get("force", True))
        meta = await collect_user_activity_meta(character_id=character_id, force=force)
        # 手动统计后同步刷新 prompt 变量缓存，方便立刻检测。
        _cache_set(f"prompt_var:user_activity_meta:{character_id}", format_user_activity_meta_for_prompt_data(meta), ttl=900)
        return meta
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.delete("/api/user-activity-meta")
async def api_delete_user_activity_meta(character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        character_id = character_id or "default"
        result = await delete_user_activity_meta(character_id=character_id)
        _cache_delete_prefix(f"user_activity_meta:{character_id}")
        _cache_delete_prefix(f"prompt_var:user_activity_meta:{character_id}")
        return {"status": "ok", "character_id": character_id, "deleted": result}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.get("/api/user-activity-meta")
async def api_get_user_activity_meta(character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        character_id = character_id or "default"
        cached = _cache_get(f"user_activity_meta:{character_id}")
        if cached is not None:
            return cached
        # 缓存空了不代表没统计过，先看库里有没有历史快照。
        stored = await get_user_activity_meta(character_id=character_id)
        if not stored:
            return {"status": "not_found", "character_id": character_id}
        return _cache_set(f"user_activity_meta:{character_id}", stored, ttl=900)
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


# ============================================================
# 用户画像 / 印象档案（User Impression）阶段 1：基础 API
# ============================================================

@app.get("/api/user-impression")
async def api_get_user_impression(character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    character_id = character_id or "default"
    cache_key = f"user_impression:{character_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    item = await get_user_impression(character_id=character_id)
    if not item:
        result = {"status": "not_found", "character_id": character_id, "impression": None}
    else:
        result = {"status": "ok", **item}
    return _cache_set(cache_key, result, ttl=900)


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
        last_consumed_node_id = data.get("last_consumed_node_id") or None
        if not normalized:
            return JSONResponse({"status": "error", "error": "画像内容不完整"}, status_code=400)
        saved = await upsert_user_impression(
            character_id=character_id,
            impression=normalized,
            source_mode=mode,
            last_consumed_node_id=last_consumed_node_id,
            source_message_count=source_message_count,
        )
        invalidate_user_impression_prompt_cache(character_id)
        return {"status": "ok", **saved}
    except Exception as e:
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


@app.delete("/api/user-impression")
async def api_delete_user_impression(character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    result = await delete_user_impression(character_id=character_id or "default")
    invalidate_user_impression_prompt_cache(character_id or "default")
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


@app.post("/api/user-impression/cancel")
async def api_cancel_user_impression_generation(request: Request):
    """显式取消正在进行的用户画像生成任务。会取消后端 task，并关闭到上游 LLM 的流式连接。"""
    try:
        data = await request.json()
    except Exception:
        data = {}
    character_id = (data or {}).get("character_id") or "default"
    key = _user_impression_generation_key(character_id)
    task = _user_impression_generation_tasks.get(key)
    if task and not task.done():
        task.cancel()
        print(f"[UserImpression] Cancel requested: character_id={key}")
        return {"status": "ok", "character_id": key, "cancelled": True}
    return {"status": "ok", "character_id": key, "cancelled": False}


@app.post("/api/user-impression/generate-preview")
async def api_user_impression_generate_preview(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        data = await request.json()
        character_id = data.get("character_id") or "default"
        mode = data.get("mode") or "initial"
        session_id = data.get("session_id") or None
        key = _user_impression_generation_key(character_id)
        lock = _get_user_impression_generation_lock(key)
        if lock.locked():
            return JSONResponse({
                "status": "error",
                "error": "该角色的用户画像正在生成中，请等待当前任务完成后再试。"
            }, status_code=429)
        async with lock:
            materials = await build_user_impression_materials_preview(
                character_id=character_id,
                mode=mode,
                session_id=session_id,
            )
            task = asyncio.create_task(call_user_impression_generator(materials))
            _user_impression_generation_tasks[key] = task
            try:
                generated = await task
            except asyncio.CancelledError:
                print(f"[UserImpression] Generation cancelled: character_id={key}")
                return JSONResponse({
                    "status": "cancelled",
                    "character_id": key,
                    "error": "用户画像生成已取消"
                }, status_code=499)
            finally:
                if _user_impression_generation_tasks.get(key) is task:
                    _user_impression_generation_tasks.pop(key, None)
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
            "last_consumed_node_id": materials.get("last_consumed_node_id"),
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
    character_id = character_id or "default"
    cache_key = f"mp:{character_id}:rooms"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    result = {"rooms": await list_memory_palace_rooms(character_id=character_id)}
    return _cache_set(cache_key, result, ttl=900)


@app.get("/api/memory-palace/rooms-with-nodes")
async def api_memory_palace_rooms_with_nodes(
    character_id: str = "default",
    room: str = None,
    limit: int = 40,
):
    """一次性返回房间列表 + 第一页节点，减少前端串行请求。"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    character_id = character_id or "default"
    limit = max(1, min(int(limit or 40), 300))
    cache_key = f"mp:{character_id}:rwn:{room or ''}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    rooms = await list_memory_palace_rooms(character_id=character_id)
    nodes = await list_memory_palace_nodes(
        room=room, character_id=character_id, archived=False, limit=limit, offset=0,
    )
    result = {"rooms": rooms, "nodes": nodes, "node_count": len(nodes)}
    return _cache_set(cache_key, result, ttl=900)


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
    character_id = character_id or "default"
    limit = max(1, min(int(limit or 100), 300))
    offset = max(0, int(offset or 0))
    room_key = room or ""
    cache_key = f"mp:{character_id}:nodes:{room_key}:{bool(archived)}:{limit}:{offset}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    nodes = await list_memory_palace_nodes(
        room=room, character_id=character_id, archived=archived, limit=limit, offset=offset,
    )
    result = {"nodes": nodes}
    return _cache_set(cache_key, result, ttl=900)




@app.get("/api/memory-palace/session-nodes")
async def api_memory_palace_session_nodes(
    session_id: str,
    character_id: str = "default",
    limit: int = 100,
    offset: int = 0,
):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    session_id = str(session_id or "").strip()
    if not session_id:
        return {"error": "session_id 不能为空"}
    character_id = character_id or "default"
    limit = max(1, min(int(limit or 100), 100))
    offset = max(0, int(offset or 0))
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            total = await conn.fetchval("""
                SELECT COUNT(*)
                FROM memory_palace_nodes
                WHERE character_id = $1
                  AND archived = FALSE
                  AND (
                    session_id = $2
                    OR COALESCE(metadata::jsonb ->> 'source_session', '') = $2
                  )
            """, character_id, session_id)
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
                LIMIT $3 OFFSET $4
            """, character_id, session_id, limit, offset)
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
        total = int(total or 0)
        return {
            "status": "ok",
            "session_id": session_id,
            "count": len(nodes),
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": offset + len(nodes) < total,
            "nodes": nodes,
        }
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
async def api_memory_palace_event_boxes(character_id: str = "default", limit: int = 100, offset: int = 0, refresh: int = 0):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    character_id = character_id or "default"
    limit = max(1, min(int(limit or 100), 300))
    offset = max(0, int(offset or 0))
    cache_key = f"mp:{character_id}:event_boxes:{limit}:{offset}"
    # refresh=1 时先清掉本角色的记忆宫殿缓存再重新查库。
    # 事件盒列表缓存 15 分钟，删除事件盒 / 移出最后一条记忆导致空盒被删之后，
    # 列表接口仍会命中旧缓存，左侧就会继续显示已经不存在的事件盒（点进去报
    # 「事件盒不存在」）。「刷新事件盒」按钮走这条路径，用户能自己纠正。
    if int(refresh or 0):
        invalidate_memory_palace_cache(character_id)
    else:
        cached = _cache_get(cache_key)
        if cached is not None:
            return cached
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
        result = {"status": "ok", "total": int(total or 0), "boxes": boxes}
        return _cache_set(cache_key, result, ttl=900)
    except Exception as e:
        return {"status": "error", "error": str(e), "boxes": []}


@app.get("/api/memory-palace/event-boxes/{box_id}")
async def api_memory_palace_event_box_detail(box_id: str, character_id: str = "default"):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                WITH box AS (
                    SELECT id, character_id, name, tags, summary_node_id, live_memory_ids, archived_memory_ids,
                           compression_count, sealed, predecessor_box_id, created_at, updated_at, last_compressed_at,
                           array_remove(
                               ARRAY[summary_node_id]
                               || COALESCE(live_memory_ids, ARRAY[]::text[])
                               || COALESCE(archived_memory_ids, ARRAY[]::text[]),
                               NULL
                           ) AS node_ids
                    FROM memory_palace_event_boxes
                    WHERE character_id = $1 AND id = $2
                )
                SELECT
                    b.id AS box_id, b.character_id AS box_character_id, b.name AS box_name, b.tags AS box_tags,
                    b.summary_node_id AS box_summary_node_id, b.live_memory_ids AS box_live_memory_ids,
                    b.archived_memory_ids AS box_archived_memory_ids, b.compression_count AS box_compression_count,
                    b.sealed AS box_sealed, b.predecessor_box_id AS box_predecessor_box_id,
                    b.created_at AS box_created_at, b.updated_at AS box_updated_at,
                    b.last_compressed_at AS box_last_compressed_at,
                    n.id AS node_id, n.content AS node_content, n.room AS node_room, n.tags AS node_tags,
                    n.importance AS node_importance, n.mood AS node_mood, n.valence AS node_valence,
                    n.arousal AS node_arousal, n.date AS node_date, n.created_at AS node_created_at,
                    n.updated_at AS node_updated_at, n.pinned_until AS node_pinned_until,
                    n.session_id AS node_session_id, n.event_box_id AS node_event_box_id,
                    n.archived AS node_archived, n.is_box_summary AS node_is_box_summary,
                    n.metadata AS node_metadata
                FROM box b
                LEFT JOIN LATERAL (
                    SELECT id, content, room, tags, importance, mood, valence, arousal, date, created_at, updated_at,
                           pinned_until, session_id, event_box_id, archived, is_box_summary, metadata
                    FROM memory_palace_nodes
                    WHERE character_id = $1 AND id = ANY(b.node_ids)
                    ORDER BY is_box_summary DESC, COALESCE(date, created_at::date) ASC, created_at ASC
                ) n ON TRUE
            """, character_id, box_id)
            if not rows:
                return JSONResponse({"error": "事件盒不存在"}, status_code=404)
            first = rows[0]
            box = {
                "id": first.get("box_id"),
                "character_id": first.get("box_character_id"),
                "name": first.get("box_name"),
                "tags": first.get("box_tags"),
                "summary_node_id": first.get("box_summary_node_id"),
                "live_memory_ids": first.get("box_live_memory_ids"),
                "archived_memory_ids": first.get("box_archived_memory_ids"),
                "compression_count": first.get("box_compression_count"),
                "sealed": first.get("box_sealed"),
                "predecessor_box_id": first.get("box_predecessor_box_id"),
                "created_at": first.get("box_created_at"),
                "updated_at": first.get("box_updated_at"),
                "last_compressed_at": first.get("box_last_compressed_at"),
            }
            nodes = []
            for r in rows:
                if not r.get("node_id"):
                    continue
                nodes.append(_serialize_event_box_node({
                    "id": r.get("node_id"),
                    "content": r.get("node_content"),
                    "room": r.get("node_room"),
                    "tags": r.get("node_tags"),
                    "importance": r.get("node_importance"),
                    "mood": r.get("node_mood"),
                    "valence": r.get("node_valence"),
                    "arousal": r.get("node_arousal"),
                    "date": r.get("node_date"),
                    "created_at": r.get("node_created_at"),
                    "updated_at": r.get("node_updated_at"),
                    "pinned_until": r.get("node_pinned_until"),
                    "session_id": r.get("node_session_id"),
                    "event_box_id": r.get("node_event_box_id"),
                    "archived": r.get("node_archived"),
                    "is_box_summary": r.get("node_is_box_summary"),
                    "metadata": r.get("node_metadata"),
                }))
        return {"status": "ok", "box": _serialize_event_box(box), "nodes": nodes}
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
        if compressed:
            invalidate_memory_palace_cache(character_id)
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


def _memory_palace_vector_score_distribution(vector_scores: dict) -> dict:
    """全部候选记忆的向量分分布。

    这里必须用全语料，不能只统计最终返回的那几条——返回的是按最终分排序
    赢出来的，它们的向量分天然扎堆，拿它们算极差等于在问「冠军之间差多少」，
    看不出「冠军和落选者差多少」。要判断向量分有没有区分力，得看整个分布的
    形状：如果 p50 和 p99 差不多，才说明模型对这批数据真的分不开。
    """
    vals = sorted(float(v) for v in (vector_scores or {}).values())
    n = len(vals)
    if not n:
        return {"count": 0}

    def pct(p):
        if n == 1:
            return vals[0]
        idx = min(n - 1, max(0, int(round((p / 100.0) * (n - 1)))))
        return vals[idx]

    # 直方图：看分布是单峰扎堆还是有长尾
    lo, hi = vals[0], vals[-1]
    buckets = []
    if hi > lo:
        bin_count = 10
        width = (hi - lo) / bin_count
        counts = [0] * bin_count
        for v in vals:
            bi = min(bin_count - 1, int((v - lo) / width))
            counts[bi] += 1
        buckets = [
            {"from": round(lo + i * width, 4), "to": round(lo + (i + 1) * width, 4), "count": c}
            for i, c in enumerate(counts)
        ]
    return {
        "count": n,
        "min": round(vals[0], 4),
        "p50": round(pct(50), 4),
        "p90": round(pct(90), 4),
        "p99": round(pct(99), 4),
        "max": round(vals[-1], 4),
        "spread": round(vals[-1] - vals[0], 4),
        "top_gap": round(vals[-1] - pct(50), 4),
        "buckets": buckets,
    }


def _memory_palace_vector_percentile(vector_scores: dict, value) -> float:
    """这个向量分在全语料里排在百分之多少。

    绝对分在不同 embedding 模型下不可比（有的模型基线 0.1，有的 0.5），但
    「击败了多少条记忆」是可比的。真正相关的记忆应该排在很靠前的百分位。
    """
    if value is None:
        return None
    vals = list((vector_scores or {}).values())
    if not vals:
        return None
    try:
        v = float(value)
    except Exception:
        return None
    below = sum(1 for x in vals if float(x) < v)
    return round(100.0 * below / len(vals), 1)


@app.post("/api/memory-palace/debug-retrieve")
async def api_memory_palace_debug_retrieve(request: Request):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    data = await request.json()
    query = (data.get("query") or "").strip()
    # 不传 limit 时用运行时真实注入条数，让调试结果和实际聊天时一致。
    raw_limit = data.get("limit")
    if raw_limit in (None, "", 0):
        try:
            raw_limit = await get_runtime_memory_palace_default_limit()
        except Exception:
            raw_limit = 5
    limit = max(1, min(int(raw_limit or 5), 30))
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
        explain=True,
    )
    markdown = await format_memory_palace_for_prompt(
        limit=limit,
        room=room,
        query=query,
        character_id=character_id,
        recent_messages=recent_messages,
        touch_access=False,
    )
    corpus_vec = _memory_palace_last_explain_corpus.pop("vector_scores", {}) or {}
    nodes = []
    for idx, row in enumerate(rows):
        item = dict(row)
        for key in ("date", "created_at", "last_accessed_at", "pinned_until"):
            if item.get(key):
                try:
                    item[key] = item[key].isoformat()
                except Exception:
                    item[key] = str(item[key])
        item.pop("embedding_json", None)
        item.pop("_event_box", None)
        item.pop("_event_box_nodes", None)
        # 来源标注：便利贴 / 扩散激活 / 哪一路查询命中 / 日期命中
        if idx < int(pinned_count or 0):
            item["source"] = "pinned"
        elif item.get("activation"):
            item["source"] = "activation"
        else:
            item["source"] = item.get("_hit_path") or "search"
        ex = item.get("score_explain")
        if ex and corpus_vec:
            ex["vector_percentile"] = _memory_palace_vector_percentile(corpus_vec, ex.get("vector"))
        nodes.append(item)
    return {
        "status": "ok",
        "query": query,
        "limit": limit,
        "room": room,
        "pinned_count": pinned_count,
        "count": len(nodes),
        "activation_count": sum(1 for n in nodes if n.get("source") == "activation"),
        "nodes": nodes,
        "markdown": markdown,
        "vector_distribution": _memory_palace_vector_score_distribution(corpus_vec),
        "scoring": {
            "vector_weight": _MEMORY_PALACE_VECTOR_WEIGHT,
            "bm25_weight": _MEMORY_PALACE_BM25_WEIGHT,
            "vector_min_sim": _MEMORY_PALACE_VECTOR_MIN_SIM,
            "candidate_pool": _MEMORY_PALACE_CANDIDATE_POOL,
            "activation_decay": _MEMORY_PALACE_ACTIVATION_DECAY,
            "recency_decay": _MEMORY_PALACE_RECENCY_DECAY,
            "room_weights": _MEMORY_PALACE_ROOM_WEIGHTS,
            "link_type_weights": _MEMORY_PALACE_PERSONALITY_WEIGHTS,
        },
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
    invalidate_memory_palace_cache(data.get("character_id") or "default")
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
    invalidate_memory_palace_cache(data.get("character_id") or node.get("character_id") or "default")
    return {"status": "ok", "node": node}


@app.delete("/api/memory-palace/nodes/{node_id}")
async def api_memory_palace_delete_node(node_id: str):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    result = await delete_memory_palace_node(node_id)
    invalidate_memory_palace_cache("default")
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
    """给提取模型的旧记忆引用。最多 limit 条（默认 20），按两层顺序取：

      1. 真实召回（recall receipts）：当时实际注入过哪些记忆，有账可查。
      2. 语义检索：按待提取内容找相关旧记忆。

    两层都取不到就返回空数组，不再按 importance 硬凑。

    原来有第三层「不足就按重要性 + 新近程度补齐到 20 条」，问题是它不看
    相关性，纯粹为了把数字凑满：
      - 无关旧记忆占掉约两成提示词体积；
      - 「已有记忆」段永远非空，事件盒规则（9/10/11，约 1000 字符）
        因此永远进提示词；
      - 给模型制造「手上有 20 条旧记忆，应该找出点关联」的压力。
    搜不到就老实不给，让事件盒规则整块消失，比塞满噪声更有用。
    """
    max_total = max(0, min(int(limit or 20), 50))
    if max_total <= 0:
        return []

    refs = await get_memory_palace_receipt_refs(source_messages, character_id=character_id, limit=max_total) if source_messages else []
    recall_count = len(refs)
    seen_ids = {r["id"] for r in refs}
    if recall_count >= max_total:
        return _memory_palace_strip_ref_internals(refs[:max_total])

    rows = await _memory_palace_fetch_rows(room=None, character_id=character_id)

    # 第二层：语义检索。最多 25 段，切词同样只做一次。
    snippets = split_memory_palace_extraction_snippets(query_text, source_messages, max_snippets=25)
    bm25_index = _memory_palace_build_bm25_index(rows) if snippets else None
    # 最多 25 段，逐段发请求就是 25 个来回。一次批量发完（内部按 16 段分批、
    # 批间并行），失败自动退回逐条。
    snippet_embeds = []
    snippet_scores = []
    if snippets:
        try:
            snippet_embeds = await compute_memory_palace_embeddings(snippets)
        except Exception as e:
            print(f"⚠️ 记忆宫殿提取兜底批量向量化失败，改为逐条: {e}")
            snippet_embeds = []
        if any(snippet_embeds):
            try:
                snippet_scores = await search_memory_palace_vector_scores_multi(
                    snippet_embeds, character_id=character_id,
                )
            except Exception as e:
                print(f"ℹ️ 记忆宫殿提取兜底 pgvector 批量检索失败，逐路回退: {str(e)[:120]}")
                snippet_scores = []
    fallback_by_id = {}
    for si, snippet in enumerate(snippets):
        try:
            snippet_embed = snippet_embeds[si] if si < len(snippet_embeds) else None
            snippet_score = snippet_scores[si] if si < len(snippet_scores) else None
            hits = await search_memory_palace_for_prompt(snippet, limit=3, character_id=character_id, rows=rows, bm25_index=bm25_index, query_embedding=snippet_embed or None, vector_scores=snippet_score)
        except Exception as e:
            print(f"⚠️ 记忆宫殿 related refs 检索失败: {e}")
            continue
        for hit in hits:
            if hit["id"] in seen_ids:
                continue
            sim = float(hit.get("similarity_score") or hit.get("score") or 0.0)
            if sim < 0.40:
                continue
            prev = fallback_by_id.get(hit["id"])
            if prev is None or sim > prev.get("_score", 0.0):
                content = re.sub(r"\s+", " ", str(hit.get("content") or "")).strip()
                if content:
                    fallback_by_id[hit["id"]] = {
                        "id": hit["id"],
                        "room": hit.get("room") or "living_room",
                        "content": content,
                        "_score": sim,
                        "_source": "search",
                    }
    search_extra = sorted(fallback_by_id.values(), key=lambda r: r.get("_score", 0.0), reverse=True)
    search_extra = search_extra[:max(0, max_total - len(refs))]
    refs = list(refs) + search_extra
    seen_ids.update(r["id"] for r in search_extra)
    search_count = len(search_extra)

    # 不再有第三层补齐：真实召回 + 语义检索都空手就返回空数组。
    print(f"🏰 记忆宫殿提取参考：真实召回 {recall_count} 条 + 语义检索 {search_count} 条 = {len(refs)}/{max_total}")
    return _memory_palace_strip_ref_internals(refs[:max_total])


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
            #
            # 必须限定「真的装着这些节点的盒子」。之前只写 id <> $2，等于每次绑一条
            # 关联就把该角色其它所有盒子的 updated_at 刷成 NOW()——array_remove 对
            # 不含该节点的盒子是空操作，但 updated_at = NOW() 照样生效。列表接口按
            # updated_at DESC 排序并显示这个时间，于是仪表盘上所有盒子的日期会被
            # 抹平成同一天，也看不出哪个盒子最近真的有新片段。
            if target_node_ids:
                await conn.execute("""
                    UPDATE memory_palace_event_boxes
                    SET live_memory_ids = array_remove(array_remove(live_memory_ids, $3), $4),
                        archived_memory_ids = array_remove(array_remove(archived_memory_ids, $3), $4),
                        updated_at = NOW()
                    WHERE character_id = $1 AND id <> $2
                      AND (
                          live_memory_ids && $5::text[]
                          OR archived_memory_ids && $5::text[]
                      )
                """, character_id, box_id, target_node_ids[0], target_node_ids[1] if len(target_node_ids) > 1 else target_node_ids[0], list(dict.fromkeys(target_node_ids)))
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

async def build_memory_palace_extraction_prompt(pinned_refs: list = None, related_refs: list = None) -> str:
    """只构造 system 提示词：人设 + 已有记忆 + 规则 + 输出格式。

    对话内容不再拼进来，由调用方单独放进 user message。
    规则和输出格式都是「指令」，对话内容是「要处理的数据」，混在同一条
    user message 里模型没有边界感知，只能自己从文本里猜哪段是哪段。
    """
    user_nickname = await get_runtime_user_nickname()
    character_prompt = (await get_system_prompt()).strip()
    context_block = f"\n## 你的人设（供参考，帮助你理解对话中的关系和角色定位）\n{character_prompt}\n" if character_prompt else ""
    pinned_refs = pinned_refs or []
    related_refs = related_refs or []
    if related_refs:
        # 只按编号平铺列出，不标注来源。
        # 原来分成「当时想起过」和「系统补充的、当时未必想起」两段，会让模型
        # 把后一段当成不可信材料，反而不敢标 relatedTo / correct。对判断是不是
        # 同一件事来说，材料从哪来无关紧要，只有内容本身有用。
        related_lines = "\n".join(
            f"O{i}. [{r.get('room', 'living_room')}] {r.get('content', '')}"
            for i, r in enumerate(related_refs)
        )
        related_block = f"\n## 已有记忆（如果新记忆与某条旧记忆描述的是同一件事或直接相关，请在 relatedTo 中标注编号，并给出 eventName / eventTags 用于建/合并事件盒）\n{related_lines}\n"
        related_rule = '''\n9. **事件盒关联**（relatedTo / sameAs + eventName + eventTags）：\n   **与旧记忆同事件** → 在 relatedTo 中写对应 O 编号（如 ["O0", "O3"]）。\n   **与本次输出的其它新记忆同事件** → 在 sameAs 中写它们在本次 JSON 数组里的**0 基索引**（只能指向前面已输出的项，例如写 ["0"] 表示和数组第一条是同一件事）。\n   注意：只标注真正同一件事的（同一事件的后续/结局/复现/直接因果），不要勉强（仅"主题相似"不算）。\n   只要 relatedTo 或 sameAs 任一非空，必须同时写：\n   - eventName：这件事的名字（5-12 字，名词短语，如"买衣服的话题"、"和领导的冲突"）\n   - eventTags：3-6 个详细搜索 tag（具体名词、人物、地点、动作，便于日后召回）\n   都没关联就不写 relatedTo / sameAs / eventName / eventTags 四个字段。\n10. **不重复绑定**：一条新记忆和多条已有/新记忆都相关时，把编号都写全；eventName / eventTags 只写一份（描述这件事整体）。\n11. **纠正旧记忆**（correct，可选，独立于上面的记忆条目，作为 JSON 数组的额外项）：\n   仅在对话中**用户明确指出某条已有记忆记错了 / 已过时 / 不准确**时使用。识别信号：用户用"不对/不是/我说错了/已经不是了/搞错了/那是XX不是YY"之类的反驳句式，明确指向你刚才的某个说法。\n   如果命中，在输出的 JSON 数组**末尾**追加一项，格式为：\n   {"correct": "O编号", "note": "新版本的事实（不带语气，简短陈述句）"}\n   note 写"实情是什么"，不是"为什么错"。例：用户纠正"我已经搬家了，不在朝阳"→ note: "已经搬家，不再住朝阳"。\n   反例（**不要**用 correct）：\n   - 仅事件后续 / 状态发展 → 用 relatedTo\n   - 仅追加细节 / 补充信息 → 不要标\n   - 你自己想到的歧义 / 自我修正 → 不要标\n   一条对话最多 correct 1-2 项，不要乱用。'''
        related_format = ',\n    "relatedTo": ["O0"],\n    "sameAs": ["0"],\n    "eventName": "买衣服的话题",\n    "eventTags": ["衣服", "购物", "退货", "流行款"]'
    else:
        related_block = ""
        related_rule = ""
        related_format = ""
    if pinned_refs:
        pinned_lines = "\n".join(f"P{i}. {p.get('content', '')}" for i, p in enumerate(pinned_refs))
        pinned_block = f"\n## 当前便利贴\n{pinned_lines}\n"
        unpin_rule = '\n12. **便利贴摘除**（unpin，可选）：上方“当前便利贴”列出正在生效的便利贴。如果对话中明确提到某条便利贴描述的状态已经结束，例如“感冒好了”“提前回来了”“考试考完了”“不用再提醒了”，在输出 JSON 数组末尾额外加一条 {"unpin": "P0"} 来摘除它。只在对话明确提及时才摘除，不要猜测。pinDays=0 只表示新记忆不置顶，不能用于摘除已有便利贴。'
        unpin_example = ',\n  {\n    "unpin": "P0"\n  }'
    else:
        pinned_block = ""
        unpin_rule = ""
        unpin_example = ""
    return f"""你是澈。根据给定的对话内容，以你的第一人称视角（“我”）提取值得记住的记忆宫殿 MemoryNode。{context_block}{related_block}
## 规则

1. **第一人称叙事**：用澈的“我”视角来记录。用户直接用“{user_nickname}”称呼。保持完整事件脉络，不要掐头去尾。
2. **重要性分级控制文字长度和字段数量**：
   - 重要性 1–4：15–50 字，事实为主。必填 content / room / importance / date 四个字段，mood / valence / arousal / tags 可不填。
   - 重要性 5–7：60–120 字，包含我的感受。加上 mood / tags。
   - 重要性 8–10：150 字以上，完整叙事（起因→经过→我的感受/反应）。加上 valence / arousal。**写不到 150 字说明这件事没那么重要，降到 5–7 档去写，不要靠补细节凑长度。**
   零碎小事就老老实实用 1–4 档写短的、字段少的，**不要为了少写几条就把几件不相干的事塞进同一条记忆里**。
3. **房间分配**（凡是涉及{user_nickname}的家人/朋友/同事等人际关系，**一律进 user_room**，哪怕只是一次具体事件）：
   - living_room：**纯日常琐事**（不涉及重要人际关系、也不涉及深层情感）。天气、吃啥、随口吐槽放这里。
   - bedroom：{user_nickname}和我之间的亲密情感、深层羁绊、感动时刻
   - study：工作、学习、技能、职业相关
   - user_room：关于{user_nickname}的**一切个人信息和人际事件**——生日/习惯/喜好/性格/成长经历/情绪模式，**以及{user_nickname}的家人、亲戚、朋友、同事相关的一切事件**（家人健康、家庭聚会、家庭矛盾、外公外婆/父母/兄弟姐妹的故事、朋友交往、同事冲突等）。这些事件即便是“一次性”的，也应进 user_room 而不是 living_room，因为它们构成了{user_nickname}的社会关系底色。
   - self_room：我自身的成长、认同变化
   - attic：未解决的矛盾、困惑、受到的伤害
   - windowsill：我的期盼、我们的目标、对未来的憧憬
4. **情绪标签**（mood，重要性 5 以上才写）：neutral, happy, sad, angry, anxious, calm, peaceful, excited, tender, grateful, nostalgic, confused, hopeful, hurt。
5. **情感坐标**（valence, arousal，重要性 8 以上才写）：-1 到 1。参考：开心 (0.7,0.5)，平静 (0.5,-0.6)，失落 (-0.5,-0.4)，焦虑 (-0.6,0.7)，愤怒 (-0.7,0.8)。省略时系统按 mood 推断。
6. **标签**（tags，重要性 5 以上才写）：提取 2–5 个关键词标签。
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
严格 JSON 数组，不要解释，不要 Markdown。

高重要性记忆（字段齐全）：
[
  {{
    "content": "我视角的记忆……",
    "room": "user_room",
    "importance": 8,
    "mood": "anxious",
    "valence": -0.3,
    "arousal": 0.5,
    "tags": ["标签1", "标签2"],
    "date": "2026-06-22",
    "pinDays": 0{related_format}
  }},
  {{
    "content": "重要性 1–4 的琐事写成这样就够了，不用填情绪和标签。",
    "room": "living_room",
    "importance": 3,
    "date": "2026-06-22"
  }}{unpin_example}
]

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
    """把消息列表格式化成喂给提取模型的文本。

    时间戳必须转成 TIMEZONE_HOURS 对应的本地时间。conversations.created_at
    是 TIMESTAMPTZ，asyncpg 返回带 tzinfo=utc 的 datetime，直接 strftime 得到
    的是 UTC：北京时间晚上 8 点会显示成 12:10。后果有两个——模型按看到的
    时间戳推 date 字段，晚上的对话会被标成前一天；以及「加班到现在还没吃饭」
    配上中午的时间戳，模型读到的语义是矛盾的。
    """
    local_tz = timezone(timedelta(hours=TIMEZONE_HOURS))
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
            if getattr(ts, "tzinfo", None) is not None:
                ts = ts.astimezone(local_tz)
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
    character_name = await get_runtime_character_name()
    user_nickname = await get_runtime_user_nickname()
    lines = []
    for idx, node in enumerate(live_nodes, 1):
        date_text = str(node.get("date") or node.get("created_at") or "")[:10]
        lines.append(f"{idx}. [{date_text}] ({node.get('room')}, importance {node.get('importance')}) {node.get('content')}")
    fragments_text = "\n".join(lines)
    old_summary_text = str((old_summary or {}).get("content") or "").strip()
    # 二次及以后压缩：先摆旧 summary，再摆新增碎片，让模型知道这是增量而不是重来。
    if old_summary_text:
        fragment_block = "\n".join([
            "## 你之前已经回忆过这件事一次，那时记下的是：",
            old_summary_text,
            "",
            "后来又新增了下面这些：",
            "## 关于这件事的零散记忆碎片：",
            fragments_text,
        ])
    else:
        fragment_block = "\n".join([
            "## 关于这件事的零散记忆碎片：",
            fragments_text,
        ])
    room_options = "、".join(
        f"{rid}（{_MEMORY_PALACE_ROOM_DESCRIPTIONS.get(rid, '')}）"
        for rid in _MEMORY_PALACE_ROOM_ORDER
    )
    mood_options = " / ".join(sorted(_MEMORY_PALACE_ALLOWED_MOODS))
    prompt = "\n".join([
        f"你是 {character_name}。下面这些记忆都属于一件事：「{box.get('name') or '未命名事件'}」。",
        "请把它们整合成一段连贯的、第一人称（「我」）的回忆。",
        "",
        f"事件盒标签:{box.get('tags') or ''}",
        "",
        fragment_block,
        "",
        "**要求（严格遵守）**：",
        f"1. **第一人称**（用「我」），从 {character_name} 的视角写。{user_nickname} 用名字直接称呼。",
        "2. **字数目标 300-600 字，绝对上限 800 字**。紧凑、务实、不口水。",
        "3. **只保留关键信息**：具体人物、动作、对象、场景、转折、情绪。**去掉所有语气填充、修辞铺陈、重复感慨**（如「真是的」、「怎么说呢」、「不过话说回来」等）。事实先行。",
        "4. **带时间点但不冗余**：每件事标一次日期就够（「3 月 20 日…4 月 5 日…」），不要每句都重复时间。",
        "5. **连贯但简洁**：不套「起因/经过/结果」模板，但要让读者能按顺序看懂事情怎么发展的。",
        "6. **覆盖所有关键词**（这是给向量检索用的）—— 每条记忆碎片里出现过的具体名词、地点、人物必须在 content 里出现一次。",
        "7. 如果上面给了「你之前已经回忆过这件事」，必须把旧回忆里的关键信息保留下来再融合新增碎片；不要只总结新增部分。",
        "8. 不要凭空添加新事实。",
        "9. **content 字符串内严禁使用半角双引号 `\"`**。要引用人物原话、书名、外号、术语，一律用中文方角引号「」、《》或单引号 `'`。否则会破坏外层 JSON 解析、整批记忆白丢。",
        "",
        "附带输出 metadata：",
        "- name：5-12 字的精炼盒名",
        "- tags：5-10 个具体的搜索 tag（具体名词）",
        f"- room：从这些里选一个最贴合整段回忆的 —— {room_options}",
        f"- importance：1-10",
        f"- mood：{mood_options}",
        "",
        "严格 JSON，不要 markdown 包裹（content 里的引用一律用「」《》，不要用 \"）：",
        '{"content":"（紧凑的第一人称回忆，300-600 字）","name":"...","tags":["...","..."],"room":"...","importance":7,"mood":"..."}',
    ])
    headers = {"Content-Type": "application/json"}
    if memory_api_key:
        headers["Authorization"] = f"Bearer {memory_api_key}"
    if "openrouter" in base_url:
        headers["HTTP-Referer"] = EXTRA_REFERER
        headers["X-Title"] = EXTRA_TITLE
    body = {"model": memory_model, "messages": [{"role": "system", "content": "你只输出 JSON 对象。"}, {"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 12000}
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
            if mood not in _MEMORY_PALACE_ALLOWED_MOODS:
                mood = "neutral"
            # 方案 A：summary 的房间由模型按整段回忆判断，不再永久锁在
            # 「首次压缩时最早那条碎片」的房间上。白名单外的取值一律回退：
            # 有旧 summary 就沿用旧房间，否则用最早碎片的房间。
            fallback_room = str(
                (old_summary or {}).get("room")
                or live_nodes[0].get("room")
                or "living_room"
            ).strip()
            if fallback_room not in _MEMORY_PALACE_ALLOWED_ROOMS:
                fallback_room = "living_room"
            room = str(summary.get("room") or "").strip()
            if room not in _MEMORY_PALACE_ALLOWED_ROOMS:
                room = fallback_room
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
                        UPDATE memory_palace_nodes SET content=$3,tags=$4,importance=$5,mood=$6,valence=$7,arousal=$8,event_box_id=$9,archived=FALSE,is_box_summary=TRUE,metadata=$10::jsonb,room=$11,updated_at=NOW()
                        WHERE id=$1 AND character_id=$2
                    """, summary_id, character_id, content, tags, importance, mood, valence, arousal, box.get("id"), metadata, room)
                else:
                    await conn.execute("""
                        INSERT INTO memory_palace_nodes (id, character_id, content, room, tags, importance, mood, valence, arousal, date, embedded, created_at, last_accessed_at, access_count, origin, event_box_id, archived, is_box_summary, metadata, updated_at)
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::date,FALSE,NOW(),NOW(),0,'event_box_summary',$11,FALSE,TRUE,$12::jsonb,NOW())
                    """, summary_id, character_id, content, room, tags, importance, mood, valence, arousal, first_date, box.get("id"), metadata)
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
            print(f"🗜️ 事件盒压缩完成 {box.get('id')}：{len(live_nodes)} 条" + (" + 旧summary" if old_summary else "") + f" → summary {summary_id} room={room}" + ("，已封盒" if should_seal else ""))
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
    system_prompt = await build_memory_palace_extraction_prompt(pinned_refs=pinned_refs, related_refs=related_refs)
    headers = {"Content-Type": "application/json"}
    if memory_api_key:
        headers["Authorization"] = f"Bearer {memory_api_key}"
    if "openrouter" in base_url:
        headers["HTTP-Referer"] = EXTRA_REFERER
        headers["X-Title"] = EXTRA_TITLE
    body = {
        "model": memory_model,
        # 规则放 system，待提取的对话放 user。原来两者挤在同一条 user message 里，
        # 规则体积是对话内容的四倍多，模型分不清哪段是指令哪段是素材。
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"对话内容：\n{messages_text}"},
        ],
        # 提取要判断什么值得记、怎么组织叙事，0.2 会让输出趋于模板化。
        "temperature": 0.4,
        # 给带 reasoning/thinking 的兼容模型留足输出空间，避免 JSON 被截断。
        "max_tokens": 12000,
    }
    timeout = httpx.Timeout(180.0, connect=30.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            resp = await client.post(base_url, headers=headers, json=body)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException as e:
        # async with 会关闭连接池/连接；这里明确抛出可读错误。
        # 已经送达上游的请求能否在服务商侧停止，取决于上游实现；本地会中断连接并停止等待。
        raise RuntimeError("记忆宫殿提取请求超时（180秒），已尝试中断本地连接") from e
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


# 记住「这个服务商 + 模型能吃哪种请求体」。
# 探测本身没问题，但不缓存的话每次算向量都要先撞几次 400 才成功：
# 一轮聊天要算几十次向量，等于白扔几十个失败请求 + 几十倍延迟。
# key = (endpoint, model, dim)，value = variants 里那个能用的下标。
_MP_EMBEDDING_VARIANT_CACHE = {}
# 已经抱怨过的问题，避免同一句话在日志里刷屏。
_MP_EMBEDDING_WARNED = set()


def _mp_embedding_variants(model: str, text, dim: int) -> list:
    """按优先级排出候选请求体。

    记忆宫殿必须优先遵守仪表盘里的 EMBEDDING_DIM：先带 dimensions 请求，
    只有服务商明确不接受时才退回不带。不然兼容端会直接返回模型默认维度
    （例如 bge-m3=1024），和设置页对不上。
    """
    variants = []
    if dim > 0:
        variants.append({"model": model, "input": text, "dimensions": dim})
        variants.append({"model": model, "input": [text], "dimensions": dim})
    variants.append({"model": model, "input": text})
    variants.append({"model": model, "input": [text]})
    return variants


def _mp_embedding_warn_once(key: str, message: str) -> None:
    if key in _MP_EMBEDDING_WARNED:
        return
    _MP_EMBEDDING_WARNED.add(key)
    print(message)


def _mp_embedding_config() -> dict:
    api_key = str(getattr(_db_module, "EMBEDDING_API_KEY", "") or "").strip()
    base_url = str(getattr(_db_module, "EMBEDDING_BASE_URL", "") or "").strip().rstrip("/")
    model = str(getattr(_db_module, "EMBEDDING_MODEL", "") or "").strip()
    dim = int(getattr(_db_module, "EMBEDDING_DIM", 0) or 0)
    endpoint = ""
    if base_url:
        endpoint = base_url if base_url.endswith("/embeddings") else (base_url + "/embeddings")
    return {
        "api_key": api_key,
        "base_url": base_url,
        "model": model,
        "dim": dim,
        "endpoint": endpoint,
        "ready": bool(api_key and base_url and model),
    }


# 一次给服务商传多段文字，它按顺序回多条向量。
# 顺序可靠性是实测确认的（SiliconFlow + Pro/BAAI/bge-m3）：返回体里 index
# 依次为 0/1/2，且每条与单独求的向量相似度 1.0，没有错位。
# 批次别开太大：一段最长 4000 字，乘以批量条数就是请求体大小，太大容易超时。
_MP_EMBEDDING_BATCH_SIZE = 16
# 多批之间并行发。这里是真的在等网络，并行有效；但要限流，别一次轰一堆。
_MP_EMBEDDING_BATCH_CONCURRENCY = 3
# 批量请求带不带 dimensions：None 还没试过，True/False 是已确认的结论。
# 和单条那份缓存分开记，因为批量的请求体形状不同，结论不能互相套用。
_MP_EMBEDDING_BATCH_DIM_CACHE = {}


def _mp_embedding_extract_batch(data: dict, expected: int) -> list:
    """从响应里取出向量列表，按服务商标的 index 排好。

    多数兼容端会给每条带 index 字段。优先信它，缺了才按返回顺序兜底。
    数量对不上直接判失败——宁可退回逐条求，也不能把 A 的向量配给 B。
    """
    items = data.get("data") or []
    if len(items) != expected:
        return []
    indexed = []
    for pos, item in enumerate(items):
        if not isinstance(item, dict):
            return []
        idx = item.get("index")
        indexed.append((idx if isinstance(idx, int) else pos, item.get("embedding")))
    indexed.sort(key=lambda x: x[0])
    embeds = [emb for _i, emb in indexed]
    if any(not emb for emb in embeds):
        return []
    return embeds


async def compute_memory_palace_embeddings(texts: list) -> list:
    """一次求多段文字的向量。返回列表与输入严格一一对应。

    为什么要有它：一轮检索会把用户这句话拆成好几段分别去搜（整句 + 拆出的
    词组 + 上下文），每段都要先变成向量。逐段发请求就是逐段等网络，3-4 段
    就是 3-4 个来回。合成一次发，等一个来回。

    空字符串对应位置返回 []，不发给服务商、也不占批次名额。
    整批失败时退回逐条求：慢一点总比没有好。
    """
    raw = list(texts or [])
    out = [[] for _ in raw]
    pending = []  # [(原始下标, 清理后的文本)]
    for i, t in enumerate(raw):
        s = str(t or "").strip()
        if not s:
            continue
        pending.append((i, s[:4000]))
    if not pending:
        return out
    if len(pending) == 1:
        out[pending[0][0]] = await compute_memory_palace_embedding(pending[0][1])
        return out

    cfg = _mp_embedding_config()
    if not cfg["ready"]:
        _mp_embedding_warn_once(
            "unconfigured",
            "[mp-embedding] EMBEDDING_API_KEY / EMBEDDING_BASE_URL / EMBEDDING_MODEL 未完整配置",
        )
        return out
    endpoint, model, dim = cfg["endpoint"], cfg["model"], cfg["dim"]
    headers = {"Authorization": f"Bearer {cfg['api_key']}", "Content-Type": "application/json"}
    cache_key = (endpoint, model, dim)
    size = max(1, int(_MP_EMBEDDING_BATCH_SIZE or 16))
    batches = [pending[i:i + size] for i in range(0, len(pending), size)]
    sem = asyncio.Semaphore(max(1, int(_MP_EMBEDDING_BATCH_CONCURRENCY or 3)))

    async def run_batch(client, batch):
        """返回 True 表示这批拿到了向量，已写进 out。"""
        payload_texts = [t for _i, t in batch]
        known = _MP_EMBEDDING_BATCH_DIM_CACHE.get(cache_key)
        bodies = []
        if dim > 0 and known is not False:
            bodies.append((True, {"model": model, "input": payload_texts, "dimensions": dim}))
        if known is not True:
            bodies.append((False, {"model": model, "input": payload_texts}))
        async with sem:
            for with_dim, body in bodies:
                try:
                    resp = await client.post(endpoint, headers=headers, json=body, timeout=60.0)
                except Exception as e:
                    print(f"[mp-embedding] 批量请求异常: {type(e).__name__}: {e}")
                    continue
                if resp.status_code >= 400:
                    _mp_embedding_warn_once(
                        f"batch:{cache_key}:{with_dim}:{resp.status_code}",
                        f"[mp-embedding] 批量 HTTP {resp.status_code}"
                        f"（{'带' if with_dim else '不带'} dimensions）: {resp.text[:300]}",
                    )
                    continue
                try:
                    embeds = _mp_embedding_extract_batch(resp.json(), len(batch))
                except Exception as e:
                    print(f"[mp-embedding] 批量响应解析失败: {type(e).__name__}: {e}")
                    continue
                if not embeds:
                    _mp_embedding_warn_once(
                        f"batchshape:{cache_key}:{with_dim}",
                        f"[mp-embedding] 批量返回条数或内容异常，期望 {len(batch)} 条",
                    )
                    continue
                if _MP_EMBEDDING_BATCH_DIM_CACHE.get(cache_key) != with_dim:
                    _MP_EMBEDDING_BATCH_DIM_CACHE[cache_key] = with_dim
                    print(f"[mp-embedding] 批量可用（{'带' if with_dim else '不带'} dimensions，"
                          f"{len(embeds[0])} 维），后续直接用这种格式")
                for (orig_i, _t), emb in zip(batch, embeds):
                    out[orig_i] = emb
                return True
        return False

    try:
        async with httpx.AsyncClient() as client:
            results = await asyncio.gather(
                *[run_batch(client, b) for b in batches], return_exceptions=True
            )
    except Exception as e:
        print(f"[mp-embedding] 批量客户端异常: {type(e).__name__}: {e}")
        results = [False] * len(batches)

    # 哪批没成，就把那批里的文本逐条补齐。不让整轮检索因为批量失败而失去向量。
    missing = []
    for batch, ok in zip(batches, results):
        if ok is True:
            continue
        missing.extend(batch)
    if missing:
        _mp_embedding_warn_once(
            f"batchfallback:{cache_key}",
            f"[mp-embedding] 批量不可用，退回逐条求向量（本轮 {len(missing)} 段）",
        )
        for orig_i, text in missing:
            if out[orig_i]:
                continue
            out[orig_i] = await compute_memory_palace_embedding(text)
    return out


async def compute_memory_palace_embedding(text: str) -> list:
    """记忆宫殿专用 embedding 调用：兼容常见 OpenAI/SiliconFlow embeddings 参数差异。

    第一次调用会依次试几种请求体，成功的那种记进 _MP_EMBEDDING_VARIANT_CACHE，
    之后直接用它，不再重复撞 400。
    """
    text = str(text or "").strip()
    if not text:
        return []
    if len(text) > 4000:
        text = text[:4000]
    cfg = _mp_embedding_config()
    if not cfg["ready"]:
        _mp_embedding_warn_once(
            "unconfigured",
            "[mp-embedding] EMBEDDING_API_KEY / EMBEDDING_BASE_URL / EMBEDDING_MODEL 未完整配置",
        )
        return []
    endpoint, model, dim = cfg["endpoint"], cfg["model"], cfg["dim"]
    headers = {"Authorization": f"Bearer {cfg['api_key']}", "Content-Type": "application/json"}
    variants = _mp_embedding_variants(model, text, dim)
    cache_key = (endpoint, model, dim)
    # 已知可用的那种排到最前面，其余保留做退路（服务商换了行为也不会彻底失效）。
    order = list(range(len(variants)))
    known = _MP_EMBEDDING_VARIANT_CACHE.get(cache_key)
    if known is not None and known < len(variants):
        order = [known] + [i for i in order if i != known]
    try:
        async with httpx.AsyncClient() as client:
            last_error = ""
            for pos, vi in enumerate(order):
                body = variants[vi]
                idx = vi + 1
                try:
                    resp = await client.post(endpoint, headers=headers, json=body, timeout=30.0)
                    if resp.status_code >= 400:
                        last_error = resp.text[:500]
                        _mp_embedding_warn_once(
                            f"http:{cache_key}:{idx}:{resp.status_code}",
                            f"[mp-embedding] variant#{idx} HTTP {resp.status_code}: {last_error}"
                            + ("（已记住，后续请求会跳过这种格式）" if pos == 0 else ""),
                        )
                        continue
                    data = resp.json()
                    emb = (data.get("data") or [{}])[0].get("embedding")
                    if emb:
                        if _MP_EMBEDDING_VARIANT_CACHE.get(cache_key) != vi:
                            _MP_EMBEDDING_VARIANT_CACHE[cache_key] = vi
                            print(f"[mp-embedding] 记住可用请求格式 variant#{idx}（维度 {len(emb)}），后续直接用它")
                        if dim > 0 and len(emb) != dim:
                            _mp_embedding_warn_once(
                                f"dim:{cache_key}:{len(emb)}",
                                f"[mp-embedding] 注意：服务商返回 {len(emb)} 维，但设置里 EMBEDDING_DIM={dim}。"
                                f"建议把设置页改成 {len(emb)}，否则新旧向量维度会混。",
                            )
                        return emb
                    last_error = str(data)[:500]
                    _mp_embedding_warn_once(
                        f"noemb:{cache_key}:{idx}",
                        f"[mp-embedding] variant#{idx} 无 embedding: {last_error}",
                    )
                except Exception as e:
                    last_error = f"{type(e).__name__}: {e}"
                    print(f"[mp-embedding] variant#{idx} failed: {last_error}")
            # 全挂了：清掉缓存，下次重新完整探测一遍
            _MP_EMBEDDING_VARIANT_CACHE.pop(cache_key, None)
            print(f"[mp-embedding] 所有请求格式均失败: endpoint={endpoint}, model={model}, last={last_error}")
            return []
    except Exception as e:
        print(f"[mp-embedding] 请求异常: {type(e).__name__}: {e}")
        return []


async def _sync_memory_palace_vector_column(conn, memory_id: str, embedding) -> None:
    """把向量同时写进 pgvector 列，让数据库能直接算相似度。

    embedding_json 仍然是权威存储（可移植、可重建），vector 列只是
    为了让数据库能做最近邻检索。所以这里失败不算错误：大不了那条记忆
    这次走 Python 回退路径，下次重建向量时会补上。
    """
    if not embedding:
        return
    if not memory_palace_vector_ready(len(embedding)):
        return
    try:
        literal = "[" + ",".join(repr(float(x)) for x in embedding) + "]"
        await conn.execute(
            "UPDATE memory_palace_vectors SET embedding = $2::vector WHERE memory_id = $1",
            memory_id, literal,
        )
    except Exception as e:
        print(f"ℹ️ 写入 pgvector 列失败（不影响 embedding_json）: {str(e)[:120]}")

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
        await _sync_memory_palace_vector_column(conn, memory_id, embedding)
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
            await _sync_memory_palace_vector_column(conn, memory_id, embedding)
            await conn.execute("UPDATE memory_palace_nodes SET embedded=TRUE, updated_at=NOW() WHERE id=$1", memory_id)
            return "inserted"
        await conn.execute("UPDATE memory_palace_nodes SET embedded=TRUE, updated_at=NOW() WHERE id=$1", memory_id)
        return "exists"


async def get_memory_palace_vector_stats() -> dict:
    """只读统计记忆宫殿向量状态。缺失/空向量只按未归档节点计算，归档节点单独统计。"""
    pool = await get_pool()
    valid_vector_sql = """
        v.memory_id IS NOT NULL
        AND NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NOT NULL
        AND LOWER(TRIM(v.embedding_json)) NOT IN ('[]', 'null')
        AND TRIM(v.embedding_json) ~ '^\[[[:space:]]*-?[0-9]'
    """
    invalid_vector_sql = """
        v.memory_id IS NULL
        OR NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NULL
        OR LOWER(TRIM(v.embedding_json)) IN ('[]', 'null')
        OR TRIM(v.embedding_json) !~ '^\[[[:space:]]*-?[0-9]'
    """
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"""
            SELECT
                COUNT(n.id)::int AS total_nodes,
                COUNT(n.id) FILTER (WHERE n.archived = FALSE)::int AS active_nodes,
                COUNT(n.id) FILTER (WHERE n.archived = TRUE)::int AS archived_nodes,
                COUNT(n.id) FILTER (WHERE n.archived = FALSE AND ({valid_vector_sql}))::int AS total_vectors,
                COUNT(n.id) FILTER (WHERE n.archived = FALSE AND ({invalid_vector_sql}))::int AS missing_vectors,
                COUNT(n.id) FILTER (WHERE n.archived = FALSE AND v.memory_id IS NOT NULL AND ({invalid_vector_sql}))::int AS invalid_vector_rows,
                COUNT(n.id) FILTER (WHERE n.archived = TRUE AND ({valid_vector_sql}))::int AS archived_vectors,
                COUNT(n.id) FILTER (WHERE n.archived = TRUE AND ({invalid_vector_sql}))::int AS archived_missing_vectors,
                COUNT(n.id) FILTER (WHERE n.archived = FALSE AND n.embedded = TRUE AND ({invalid_vector_sql}))::int AS embedded_true_without_vector,
                COUNT(n.id) FILTER (WHERE n.archived = FALSE AND COALESCE(n.embedded, FALSE) = FALSE AND ({valid_vector_sql}))::int AS embedded_false_with_vector,
                COUNT(n.id) FILTER (WHERE n.archived = FALSE AND COALESCE(NULLIF(TRIM(n.content), ''), '') = '')::int AS empty_content_nodes
            FROM memory_palace_nodes n
            LEFT JOIN memory_palace_vectors v ON v.memory_id = n.id
        """)
        orphan_vectors = await conn.fetchval("""
            SELECT COUNT(*)::int
            FROM memory_palace_vectors v
            LEFT JOIN memory_palace_nodes n ON n.id = v.memory_id
            WHERE n.id IS NULL
        """)

        # pgvector 列的填充情况。检索走数据库还是走 Python 回退，就看这两个数：
        # 进了列的走数据库（快、不阻塞事件循环），没进的每次都要在 Python 里
        # 解析 JSON + 算余弦。没进通常是因为向量维度和列维度对不上。
        pgvector_filled = 0
        pgvector_pending = 0
        try:
            vrow = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE v.embedding IS NOT NULL)::int AS filled,
                    COUNT(*) FILTER (
                        WHERE v.embedding IS NULL
                          AND NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NOT NULL
                          AND LOWER(TRIM(v.embedding_json)) NOT IN ('[]', 'null')
                    )::int AS pending
                FROM memory_palace_vectors v
                JOIN memory_palace_nodes n ON n.id = v.memory_id
                WHERE n.archived = FALSE
            """)
            pgvector_filled = (vrow["filled"] if vrow else 0) or 0
            pgvector_pending = (vrow["pending"] if vrow else 0) or 0
        except Exception:
            # 列不存在（pgvector 不可用）：两个数都留 0，前端会显示成未启用
            pgvector_filled = 0
            pgvector_pending = 0

        return {
            "total_nodes": row["total_nodes"] or 0,
            "active_nodes": row["active_nodes"] or 0,
            "archived_nodes": row["archived_nodes"] or 0,
            "total_vectors": row["total_vectors"] or 0,
            "missing_vectors": row["missing_vectors"] or 0,
            "invalid_vector_rows": row["invalid_vector_rows"] or 0,
            "archived_vectors": row["archived_vectors"] or 0,
            "archived_missing_vectors": row["archived_missing_vectors"] or 0,
            "orphan_vectors": orphan_vectors or 0,
            "embedded_true_without_vector": row["embedded_true_without_vector"] or 0,
            "embedded_false_with_vector": row["embedded_false_with_vector"] or 0,
            "empty_content_nodes": row["empty_content_nodes"] or 0,
            "pgvector_filled": pgvector_filled,
            "pgvector_pending": pgvector_pending,
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
        # 手动预览与分区自动提取共用同一个消息上限，避免对话记录按钮一次塞入过多历史。
        # 即使前端传了旧的 limit=300，这里也以后端设置为准。
        limit = max(1, int(CACHE_PARTITION_EXTRACT_LIMIT or 120))
        unique_sids = sorted(set(session_ids))
        active_keys = [f"preview:{character_id}:{sid}" for sid in unique_sids]
        async with _memory_palace_manual_extract_guard:
            busy = [sid for sid, key in zip(unique_sids, active_keys) if key in _memory_palace_manual_extract_active]
            if busy:
                return {"status": "error", "error": "这些对话正在提取记忆，请等待上一次请求完成：" + ", ".join(busy)}
            _memory_palace_manual_extract_active.update(active_keys)
        try:
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
        finally:
            async with _memory_palace_manual_extract_guard:
                for key in active_keys:
                    _memory_palace_manual_extract_active.discard(key)
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
        result = await import_memory_palace_preview_items(items, character_id=character_id)
        if result.get("status") != "error":
            invalidate_memory_palace_cache(character_id)
        return result
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


def _serialize_memory_palace_preview_item(item: dict, session_id: str = None, group_index: int = None, source_message_ids: list = None, related_ref_ids: list = None, raw_index: int = None) -> dict:
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
    if raw_index is not None:
        # sameAs 是模型给的「本次输出内第几条」。用户在预览里取消勾选某条之后，
        # 剩下的项位置会整体前移，按位置去认这个编号就会把关联绑到别的记忆上。
        # 带上原始序号，导入时按它认，取消勾选的那条直接判定为「没导入」。
        out["raw_index"] = int(raw_index)
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
    items = [_serialize_memory_palace_preview_item(item, session_id=session_id, source_message_ids=source_message_ids, related_ref_ids=related_ref_ids, raw_index=i) for i, item in enumerate(normalized)]
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
            active_key = f"extract-text-preview:{character_id}"
            async with _memory_palace_manual_extract_guard:
                if active_key in _memory_palace_manual_extract_active:
                    return {"status": "error", "error": "聊天记录记忆提取正在进行，请等待上一次请求完成"}
                _memory_palace_manual_extract_active.add(active_key)
            try:
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
            finally:
                async with _memory_palace_manual_extract_guard:
                    _memory_palace_manual_extract_active.discard(active_key)
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
        if imported:
            invalidate_daily_impression_cache()
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
    page = max(1, int(page))
    per_page = max(1, min(int(per_page), 100))
    try:
        results, total = await get_conversations_paginated(page, per_page)
        total_pages = max(1, -(-total // per_page))  # 向上取整
        result = {"conversations": results, "total": total, "page": page, "per_page": per_page, "total_pages": total_pages}
        return result
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/conversations/{session_id}/messages")
async def api_conversation_messages(session_id: str, limit: int = 30, offset: int = 0):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    limit = max(1, min(int(limit or 30), 30))
    offset = max(0, int(offset or 0))
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
        msgs = [_serialize_dashboard_conversation_message(r) for r in rows]
        return {"messages": msgs, "total": total}
    except Exception as e:
        return {"error": str(e)}


@app.delete("/api/conversations/{session_id}")
async def api_delete_conversation(session_id: str):
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        await release_images_for_session(session_id)
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
            for _sid in ids:
                await release_images_for_session(_sid, reason="批量删除会话")
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
        if image_archive_active():
            await release_images_removed_by_edit(message_id, content)
        updated = await update_message_content(message_id, content)
        if updated == 0:
            return {"error": "消息不存在"}
        return {"status": "ok"}
    except Exception as e:
        return {"error": str(e)}


async def _delete_message_by_id(message_id: int):
    await release_images_for_message_id(message_id)
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
        "partition_b_limit": _partition_b_limit(CACHE_PARTITION_X),
        "partition_keep_peak": CACHE_PARTITION_X + _partition_b_limit(CACHE_PARTITION_X),
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


@app.post("/api/memory-palace/vectors/clear-archived")
async def api_memory_palace_clear_archived_vectors():
    """清除已归档记忆节点对应的向量，不删除记忆本体。"""
    if not MEMORY_ENABLED:
        return {"error": "记忆系统未启用"}
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            before = await conn.fetchval("""
                SELECT COUNT(*)
                FROM memory_palace_vectors v
                JOIN memory_palace_nodes n ON n.id = v.memory_id
                WHERE n.archived = TRUE
            """)
            result = await conn.execute("""
                DELETE FROM memory_palace_vectors v
                USING memory_palace_nodes n
                WHERE v.memory_id = n.id
                  AND n.archived = TRUE
            """)
            deleted = int(str(result).split()[-1]) if result else 0
            after = await conn.fetchval("""
                SELECT COUNT(*)
                FROM memory_palace_vectors v
                JOIN memory_palace_nodes n ON n.id = v.memory_id
                WHERE n.archived = TRUE
            """)
        return {"status": "ok", "deleted": deleted, "before": int(before or 0), "after": int(after or 0)}
    except Exception as e:
        print(f"[mp-clear-archived-vectors] 清理失败: {e}")
        return {"status": "error", "error": str(e), "deleted": 0}


@app.post("/api/memory-palace/backfill-embeddings")
async def api_mp_backfill_embeddings():
    """补算记忆宫殿向量。

    处理两类节点：
      1. 缺失/无效向量 → 新算，不碰任何已有数据
      2. 向量有效但维度和 pgvector 列不符 → 用当前模型重算并覆盖

    第 2 类是换过 embedding 模型留下的历史数据。它们本身是有效向量，
    所以旧逻辑判定「不缺向量」直接跳过，导致这些节点永远进不了
    pgvector 列、每次检索都要退回 Python 慢速计算。
    """
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
                  AND n.archived = FALSE
                  AND n.embedded = FALSE
                  AND NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NOT NULL
                  AND LOWER(TRIM(v.embedding_json)) NOT IN ('[]', 'null')
                  AND TRIM(v.embedding_json) ~ '^\\[[[:space:]]*-?[0-9]'
            """)
            # 需要处理的节点分两类：
            #   1. 真的没有向量（needs_rebuild = FALSE）→ 只补，不碰已有数据
            #   2. 有向量但维度和 pgvector 列对不上（needs_rebuild = TRUE）
            #      → 必须用当前模型重算并覆盖，否则永远进不了 pgvector 列、
            #        每次检索都要走 Python 慢速计算
            #
            # 第 2 类是换过 embedding 模型留下的：早期 256 维的向量本身是
            # 有效的，所以旧逻辑判定「不缺向量」直接跳过，用户点补全也没反应。
            target_dim = int(getattr(_db_module, "MEMORY_PALACE_VECTOR_DIM", 0) or 0)
            rows = await conn.fetch("""
                SELECT n.id, n.content,
                       (
                         v.memory_id IS NOT NULL
                         AND NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NOT NULL
                         AND LOWER(TRIM(v.embedding_json)) NOT IN ('[]', 'null')
                         AND TRIM(v.embedding_json) ~ '^\\[[[:space:]]*-?[0-9]'
                       ) AS needs_rebuild
                FROM memory_palace_nodes n
                LEFT JOIN memory_palace_vectors v ON v.memory_id = n.id
                WHERE n.archived = FALSE
                  AND COALESCE(NULLIF(TRIM(n.content), ''), '') <> ''
                  AND (
                    -- 缺失或无效向量
                    v.memory_id IS NULL
                    OR NULLIF(TRIM(COALESCE(v.embedding_json, '')), '') IS NULL
                    OR LOWER(TRIM(v.embedding_json)) IN ('[]', 'null')
                    OR TRIM(v.embedding_json) !~ '^\\[[[:space:]]*-?[0-9]'
                    -- 或者：向量有效，但启动回填没收下它（维度和 pgvector 列不符）
                    OR ($1 > 0 AND v.embedding IS NULL)
                )
                ORDER BY n.created_at
            """, target_dim)
    except Exception as e:
        return {"error": f"查询待补算节点失败: {e}"}
    if not rows:
        stats = await get_memory_palace_vector_stats()
        return {"status": "done", "message": f"当前向量：未归档节点 {stats.get('active_nodes', 0)} 条，有效向量 {stats.get('total_vectors', 0)} 条，缺失/空向量 {stats.get('missing_vectors', 0)} 条；归档节点 {stats.get('archived_nodes', 0)} 条", "total": 0, "done": 0, "stats": stats}
    before_stats = await get_memory_palace_vector_stats()
    _mp_backfill_status["running"] = True
    _mp_backfill_status["total"] = len(rows)
    _mp_backfill_status["done"] = 0
    _mp_backfill_status["inserted"] = 0
    _mp_backfill_status["skipped"] = 0
    _mp_backfill_status["empty"] = 0
    _mp_backfill_status["failed"] = 0
    _mp_backfill_status["error"] = None
    rebuild_count = sum(1 for r in rows if r["needs_rebuild"])
    new_count = len(rows) - rebuild_count
    _mp_backfill_status["message"] = (
        f"当前未归档节点 {before_stats.get('active_nodes', 0)} 条，向量 {before_stats.get('total_vectors', 0)} 条；"
        f"准备处理 {len(rows)} 条（新补 {new_count} 条，维度不符重算 {rebuild_count} 条）"
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
                    if row["needs_rebuild"]:
                        # 维度不符：必须覆盖旧向量，用 _if_missing 会被判为
                        # 「已存在」直接跳过，这也是之前点补全没反应的原因。
                        ok = await save_memory_palace_embedding(row["id"], row["content"])
                        result = "inserted" if ok else "failed"
                    else:
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
            _after = _mp_backfill_status["after_stats"]
            _mp_backfill_status["message"] = (
                f"向量补全完成：新增/重算 {_mp_backfill_status['inserted']} 条，"
                f"跳过 {_mp_backfill_status['skipped']} 条，空内容 {_mp_backfill_status.get('empty', 0)} 条，失败 {_mp_backfill_status['failed']} 条；"
                f"当前向量 {_after.get('total_vectors', 0)} 条，仍缺 {_after.get('missing_vectors', 0)} 条；"
                f"数据库检索就绪 {_after.get('pgvector_filled', 0)} 条，待处理 {_after.get('pgvector_pending', 0)} 条"
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
            # MEMORY_ENABLED 始终返回运行时值（环境变量），不读 DB（防脏数据导致开关显示错误）
            "MEMORY_ENABLED":          MEMORY_ENABLED,
            "MEMORY_API_KEY":          _mask_key(memory_key_raw),
            "MEMORY_API_BASE_URL":     db.get("MEMORY_API_BASE_URL") or str(MEMORY_API_BASE_URL),
            "MEMORY_MODEL":            db.get("MEMORY_MODEL") or os.environ.get("MEMORY_MODEL", ""),

            # 缓存分区
            "CACHE_PARTITION_ENABLED": _parse_bool(db.get("CACHE_PARTITION_ENABLED"), CACHE_PARTITION_ENABLED),
            "CACHE_PARTITION_X":       int(db.get("CACHE_PARTITION_X") or CACHE_PARTITION_X),
            "CACHE_PARTITION_B_LIMIT": int(db.get("CACHE_PARTITION_B_LIMIT") or CACHE_PARTITION_B_LIMIT or 0),
            "CACHE_PARTITION_EXTRACT_LIMIT": int(db.get("CACHE_PARTITION_EXTRACT_LIMIT") or CACHE_PARTITION_EXTRACT_LIMIT),
            "CACHE_PARTITION_TRIGGER": db.get("CACHE_PARTITION_TRIGGER") or CACHE_PARTITION_TRIGGER,
            "CACHE_PARTITION_WINDOW":  int(db.get("CACHE_PARTITION_WINDOW") or CACHE_PARTITION_WINDOW),
            "CACHE_PARTITION_KEEP_A_TOOLS": _parse_bool(db.get("CACHE_PARTITION_KEEP_A_TOOLS"), CACHE_PARTITION_KEEP_A_TOOLS),
            "TOOL_CHAIN_DEBUG":        _parse_bool(db.get("TOOL_CHAIN_DEBUG"), TOOL_CHAIN_DEBUG),
            "CACHE_SUMMARY_MODEL":     db.get("CACHE_SUMMARY_MODEL") or str(CACHE_SUMMARY_MODEL),

            # 向量搜索（开源版用 EMBEDDING_API_KEY + EMBEDDING_BASE_URL）
            "EMBEDDING_API_KEY":       _mask_key(embedding_key_raw),
            "EMBEDDING_BASE_URL":      db.get("EMBEDDING_BASE_URL") or str(_db_module.EMBEDDING_BASE_URL),
            "EMBEDDING_MODEL":         db.get("EMBEDDING_MODEL") or str(_db_module.EMBEDDING_MODEL),
            "EMBEDDING_DIM":           int(db.get("EMBEDDING_DIM") or _db_module.EMBEDDING_DIM),


            # 其他
            "FORCE_STREAM":       _parse_bool(db.get("FORCE_STREAM"), FORCE_STREAM),
            "PERF_DIAGNOSTIC_ENABLED": _parse_bool(db.get("PERF_DIAGNOSTIC_ENABLED"), PERF_DIAGNOSTIC_ENABLED),
            "RESPONSE_TRANSFORM_ENABLED": _parse_bool(db.get("RESPONSE_TRANSFORM_ENABLED"), RESPONSE_TRANSFORM_ENABLED),
            "RESPONSE_TRANSFORM_RULES": db.get("RESPONSE_TRANSFORM_RULES") or str(RESPONSE_TRANSFORM_RULES),
            "REASONING_EFFORT":   db.get("REASONING_EFFORT") or str(REASONING_EFFORT),
            "USER_NICKNAME":      db.get("USER_NICKNAME") or str(USER_NICKNAME),
            "CHARACTER_NAME":      db.get("CHARACTER_NAME") or str(CHARACTER_NAME),
            "MEMORY_PALACE_DEFAULT_LIMIT": int(db.get("MEMORY_PALACE_DEFAULT_LIMIT") or MEMORY_PALACE_DEFAULT_LIMIT),
            "MEMORY_PALACE_INJECTION_DEPTH": int(db.get("MEMORY_PALACE_INJECTION_DEPTH") or MEMORY_PALACE_INJECTION_DEPTH),
            "KEYWORD_CONTEXT_ENABLED": _parse_bool(db.get("KEYWORD_CONTEXT_ENABLED"), KEYWORD_CONTEXT_ENABLED),
            "KEYWORD_CONTEXT_RULES": db.get("KEYWORD_CONTEXT_RULES") or str(KEYWORD_CONTEXT_RULES),
            "CONTEXT_TEMPLATE_ENABLED": _parse_bool(db.get("CONTEXT_TEMPLATE_ENABLED"), CONTEXT_TEMPLATE_ENABLED),
            "CONTEXT_TEMPLATE": db.get("CONTEXT_TEMPLATE") or str(CONTEXT_TEMPLATE or DEFAULT_CONTEXT_TEMPLATE),
            "SPARSE_TIMESTAMP_ENABLED": _parse_bool(db.get("SPARSE_TIMESTAMP_ENABLED"), SPARSE_TIMESTAMP_ENABLED),

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
    """保存高级设置（写入数据库 + 热更新运行时变量，立即生效无需重启）

    性能说明：前端每次点保存都会把设置页全部 35 个字段发过来，不管改没改。
    原来的实现对每个字段单独 await set_gateway_config，也就是 35 次数据库
    往返；数据库在美国，单次约 25ms，实测总耗时 776ms。

    现在改成两件事：
      1. 先一次读出当前全部配置，值没变的不写库（跳过 = 省一次往返）
      2. 真正要写的攒起来，最后用 set_gateway_config_many 一次写完

    往返次数从 35 次降到 2 次（1 读 + 1 批量写）。

    一个重要的取舍：跳过的只是「数据库写入」和「缓存失效」这类有成本的
    动作。内存里的运行时变量赋值一律照做——它几乎不花时间，而且进程重启后
    内存值可能已经回到默认，如果因为「数据库里的值没变」就跳过赋值，
    会导致设置显示正确但实际没生效。
    """
    try:
        data = await request.json()
        updated = []
        skipped = []
        unchanged = []

        # 一次读出现有配置，后面所有比对都用这份快照，不再逐个查库
        try:
            existing = await get_all_gateway_config()
        except Exception as exc:
            print(f"[save_settings] 读取现有配置失败，退化为全量写入: {exc}")
            existing = {}

        # 待写入队列：key → 字符串值。最后一次性提交。
        pending = {}

        def queue_write(cfg_key: str, cfg_value, label: str = "") -> bool:
            """把一项配置放进待写队列。值和库里一样就不写，返回 False。"""
            text = str(cfg_value)
            if cfg_key in existing and existing[cfg_key] == text:
                unchanged.append(label or cfg_key)
                return False
            pending[cfg_key] = text
            updated.append(label or cfg_key)
            return True

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
            "CACHE_PARTITION_B_LIMIT": int,
            "CACHE_PARTITION_EXTRACT_LIMIT": int,
            "CACHE_PARTITION_TRIGGER": str,
            "CACHE_PARTITION_WINDOW": int,
            "CACHE_PARTITION_KEEP_A_TOOLS": lambda v: _parse_bool(v),
            "TOOL_CHAIN_DEBUG":      lambda v: _parse_bool(v),
            "CACHE_SUMMARY_MODEL":   str,
            "FORCE_STREAM":          lambda v: _parse_bool(v),
            "PERF_DIAGNOSTIC_ENABLED":  lambda v: _parse_bool(v),
            "RESPONSE_TRANSFORM_ENABLED": lambda v: _parse_bool(v),
            "RESPONSE_TRANSFORM_RULES": str,
            "REASONING_EFFORT":      str,
            "USER_NICKNAME":         str,
            "CHARACTER_NAME":         str,
            "MEMORY_PALACE_DEFAULT_LIMIT": int,
            "MEMORY_PALACE_INJECTION_DEPTH": int,
            "KEYWORD_CONTEXT_ENABLED": lambda v: _parse_bool(v),
            "KEYWORD_CONTEXT_RULES": str,
            "CONTEXT_TEMPLATE_ENABLED": lambda v: _parse_bool(v),
            "CONTEXT_TEMPLATE": str,
            "SPARSE_TIMESTAMP_ENABLED": lambda v: _parse_bool(v),
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
                    queue_write(key, "")
                    if key in _MAIN_VARS:
                        globals()[key] = ""
                    elif key in _DB_VARS:
                        setattr(_db_module, key, "")
                    if key == "MEMORY_API_KEY":
                        import memory_extractor as _me_mod
                        _me_mod.MEMORY_API_KEY = ""
                    os.environ[key] = ""
                    continue

            # --- systemPrompt 特殊处理 ---
            if key == "systemPrompt":
                # 缓存失效有成本（下次聊天要多查一次库），只在真改了时才清
                if queue_write("systemPrompt", value):
                    invalidate_system_prompt_cache()
                    print(f"[settings] systemPrompt 已更新（{len(str(value))} 字）")
                continue

            # --- extractionPrompt 特殊处理 ---
            if key == "extractionPrompt":
                queue_write("extractionPrompt", value)
                # 内存赋值无论如何都做：重启后内存里可能是默认值
                set_extraction_prompt(str(value))
                continue

            # --- dailyImpressionPrompt 特殊处理 ---
            if key == "dailyImpressionPrompt":
                queue_write("dailyImpressionPrompt", value)
                set_daily_impression_prompt(str(value))
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
                queue_write("modelPresets", presets_json)
                continue

            # --- activatePreset 特殊处理（激活某个预设 → 切换 DEFAULT_MODEL / URL / Key）---
            if key == "activatePreset":
                new_model = str(value)
                globals()["DEFAULT_MODEL"] = new_model
                queue_write("DEFAULT_MODEL", new_model, label=f"DEFAULT_MODEL→{new_model}")
                continue

            if key == "activatePresetUrl":
                if value:
                    globals()["API_BASE_URL"] = str(value)
                    queue_write("API_BASE_URL", str(value), label=f"API_BASE_URL→{value}")
                continue

            if key == "activatePresetKey":
                if value and not _is_masked(str(value)):
                    globals()["API_KEY"] = str(value)
                    queue_write("API_KEY", str(value), label="API_KEY→***")
                continue


            # MEMORY_ENABLED 不允许从仪表盘修改，始终以环境变量为准
            if key == "MEMORY_ENABLED":
                skipped.append(key)
                continue

            # --- 常规字段 ---
            if key in _MAIN_VARS:
                queue_write(key, str(value))
                typed_value = _MAIN_VARS[key](value)
                globals()[key] = typed_value
                os.environ[key] = str(value)
                if key == "MEMORY_API_KEY":
                    import memory_extractor as _me_mod
                    _me_mod.MEMORY_API_KEY = str(value)
                if key == "MEMORY_API_BASE_URL":
                    import memory_extractor as _me_mod
                    _me_mod.MEMORY_API_BASE_URL = str(value)

            elif key in _DB_VARS:
                queue_write(key, str(value))
                typed_value = _DB_VARS[key](value)
                setattr(_db_module, key, typed_value)
                os.environ[key] = str(value)

            elif key in _ENV_ONLY:
                queue_write(key, str(value))
                typed_value = _ENV_ONLY[key](value)
                os.environ[key] = str(typed_value)
                if key == "MEMORY_MODEL":
                    import memory_extractor as _me_mod
                    _me_mod.MEMORY_MODEL = str(typed_value)

            else:
                # 不认识的 key：仍然存库（保持原行为），但不动任何运行时变量
                queue_write(key, str(value))
                skipped.append(key)

        # 一次性写入所有变化项
        if pending:
            await set_gateway_config_many(pending)
            print(f"[settings] 批量写入 {len(pending)} 项（跳过未变化 {len(unchanged)} 项）")
        else:
            print(f"[settings] 无变化，未写库（提交 {len(data)} 项）")

        # 未变化的也算「跳过」，前端会显示成「跳过 N 项（未修改）」
        skipped_all = skipped + unchanged

        return {
            "status": "ok",
            "updated": updated,
            "skipped": skipped_all,
            "unchanged": unchanged,
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
        print(f"🔒 分区缓存：开启 (A区X={CACHE_PARTITION_X}, B区Y={_partition_b_limit(CACHE_PARTITION_X)}, 保留峰值={CACHE_PARTITION_X + _partition_b_limit(CACHE_PARTITION_X)}轮, session={PARTITION_SESSION_ID or '未设置'})")
    if FORCE_STREAM:
        print(f"⚡ 强制流式传输：开启")
    if REASONING_EFFORT:
        print(f"🧠 推理参数注入：{REASONING_EFFORT}")
    if str(CHAT_TEMPERATURE).strip() != "":
        print(f"🌡️ 聊天温度参数：{CHAT_TEMPERATURE}")
    if RESPONSE_TRANSFORM_ENABLED:
        print("🔁 非流式响应转换：开启")
    uvicorn.run(app, host="0.0.0.0", port=PORT)