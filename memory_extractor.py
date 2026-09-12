"""
记忆提取模块 —— 用 LLM 从对话中提炼关键记忆
=============================================
每次对话结束后，把最近的对话内容发给一个便宜的模型，
让它提取出值得记住的信息，存到数据库里。

v2.3 改进：提取时注入已有记忆，让模型对比后只提取全新信息。
"""

import os
import json
import re
import httpx
from typing import List, Dict

API_KEY = os.getenv("API_KEY", "")
API_BASE_URL = os.getenv("API_BASE_URL", "https://openrouter.ai/api/v1/chat/completions")

# 记忆模型专用 API 地址。
# 注意：这里故意不回退到主 API_BASE_URL。
# 如果没有设置 MEMORY_API_BASE_URL，记忆提取/评分会跳过，避免偷偷占用聊天模型或聊天中转。
MEMORY_API_BASE_URL = os.getenv("MEMORY_API_BASE_URL", "")

# 记忆模型专用 API Key（不设则回退到主 API_KEY）
# 适用于中转站按模型分组、不同模型需要不同 Key 的场景
MEMORY_API_KEY = os.getenv("MEMORY_API_KEY", "")

# 用来提取记忆的模型（便宜的就行）
MEMORY_MODEL = os.getenv("MEMORY_MODEL", "anthropic/claude-haiku-4")

# 最近一次提取状态，供主流程判断是否移动“提取书签”

def get_memory_api_key() -> str:
    return MEMORY_API_KEY or API_KEY

def get_memory_api_base_url() -> str:
    return MEMORY_API_BASE_URL

def _parse_json_array_from_text(text: str):
    """Parse a JSON array from model output that may contain extra prose or fences."""
    cleaned = (text or "").strip()
    if cleaned.startswith("```json"):
        cleaned = cleaned[7:]
    if cleaned.startswith("```"):
        cleaned = cleaned[3:]
    if cleaned.endswith("```"):
        cleaned = cleaned[:-3]
    cleaned = cleaned.strip()

    def normalize(parsed):
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            if "content" in parsed:
                return [parsed]
            for key in ("memories", "memory", "items", "data", "result", "results"):
                value = parsed.get(key)
                if isinstance(value, list):
                    return value
                if isinstance(value, dict) and "content" in value:
                    return [value]
        return None

    try:
        parsed = json.loads(cleaned)
        normalized = normalize(parsed)
        if normalized is not None:
            return normalized
    except json.JSONDecodeError:
        pass

    starts = [idx for idx, char in enumerate(cleaned) if char == "["]
    ends = [idx for idx, char in enumerate(cleaned) if char == "]"]
    for start in starts:
        for end in reversed(ends):
            if end <= start:
                continue
            candidate = cleaned[start:end + 1]
            try:
                parsed = json.loads(candidate)
                normalized = normalize(parsed)
                if normalized is not None:
                    return normalized
            except json.JSONDecodeError:
                continue

    object_starts = [idx for idx, char in enumerate(cleaned) if char == "{"]
    object_ends = [idx for idx, char in enumerate(cleaned) if char == "}"]
    for start in object_starts:
        for end in reversed(object_ends):
            if end <= start:
                continue
            candidate = cleaned[start:end + 1]
            try:
                parsed = json.loads(candidate)
                normalized = normalize(parsed)
                if normalized is not None:
                    return normalized
            except json.JSONDecodeError:
                continue

    # 方式A：匹配 "content": "...", "temperature"/"importance": N 整块（处理未转义引号）
    try:
        pattern_a = re.compile(
            r'"content"\s*:\s*"(.*?)"\s*,\s*"(?:temperature|importance)"\s*:\s*(-?\d+)',
            re.S
        )
        fallback_a = []
        for match in pattern_a.finditer(cleaned):
            raw_content = match.group(1)
            importance = int(match.group(2))
            try:
                decoded = json.loads(f'"{raw_content}"')
            except json.JSONDecodeError:
                decoded = raw_content.replace('\\"', '"').replace('\\n', '\n')
            if str(decoded).strip():
                fallback_a.append({"content": str(decoded).strip(), "importance": importance})
        if fallback_a:
            print(f"\U0001f4dd JSON\u5bbd\u677e\u5156\u5e95\u63d0\u53d6\u6210\u529f\uff08\u65b9\u5f0fA\uff09: {len(fallback_a)} \u6761\uff0c\u542b importance")
            return fallback_a
    except Exception:
        pass

    # 方式B：只匹配 content（最终兜底）
    fallback = []
    for match in re.finditer(r'"content"\s*:\s*"((?:\\\\.|[^"\\\\])*)("?)', cleaned, flags=re.S):
        raw_content = match.group(1)
        try:
            content_val = json.loads(f'"{raw_content}"')
        except json.JSONDecodeError:
            content_val = raw_content.replace('\\"', '"').replace('\\n', '\n')
        if str(content_val).strip():
            fallback.append({"content": str(content_val).strip(), "importance": 0})
    if fallback:
        print(f"\U0001f4dd JSON\u5bbd\u677e\u5156\u5e95\u63d0\u53d6\u6210\u529f\uff08\u65b9\u5f0fB\uff09: {len(fallback)} \u6761content")
        return fallback

    raise json.JSONDecodeError("No valid memory JSON found in model output", cleaned, 0)
EXTRACTION_PROMPT = """你是信息提取专家，负责从对话中识别并提取值得长期记住的关键信息。

# 提取重点
- 关键信息：提取用户的重要信息和值得回忆的生活细节
- 重要事件：记忆深刻的互动，需包含人物、时间、地点（如有）

# 提取范围
- 个人：年龄、生日、职业、学历、居住地
- 偏好：明确表达的喜好或厌恶
- 健康：身体状况、过敏史、饮食禁忌
- 事件：与AI的重要互动、约定、里程碑
- 关系：家人、朋友、重要同事
- 价值观：表达的信念或长期目标
- 情感：重要的情感时刻或关系里程碑
- 生活：用户当天的活动、饮食、出行、日常经历等生活细节

# 不要提取
- 日常寒暄（"你好""在吗"）
- AI助手自己的回复内容
- 关于记忆系统本身的讨论（"某条记忆没有被记录""记忆遗漏""没有被提取"等）
- 技术调试、bug修复的过程性讨论（除非涉及用户技能或项目里程碑）
- AI的思考过程、思维链内容

# 已知信息处理【最重要】
<已知信息>
{existing_memories}
</已知信息>

- 新信息必须与已知信息逐条比对
- 相同、相似或语义重复的信息必须忽略（例如已知"用户去妈妈家吃团年饭"，就不要再提取"用户春节去了妈妈家"）
- 已知信息的补充或更新可以提取（例如已知"用户养了一只猫"，新信息"猫最近生病了"可以提取）
- 与已知信息矛盾的新信息可以提取（标注为更新）
- 仅提取完全新增且不与已知信息重复的内容
- 如果对话中没有任何新信息，返回空数组 []

# 输出格式
请用以下 JSON 格式返回（不要包含其他内容）：
[
  {{"content": "记忆内容", "importance": 分数}},
  {{"content": "记忆内容", "importance": 分数}}
]

importance 分数 1-10，10 最重要。
如果没有值得记住的新信息，返回空数组：[]
"""

_DEFAULT_EXTRACTION_PROMPT = EXTRACTION_PROMPT

def get_extraction_prompt() -> str:
    """获取当前生效的提取提示词"""
    return EXTRACTION_PROMPT or _DEFAULT_EXTRACTION_PROMPT

def set_extraction_prompt(new_prompt: str):
    """仪表盘热更新提取提示词（传空字符串恢复默认）"""
    global EXTRACTION_PROMPT
    if new_prompt and new_prompt.strip():
        EXTRACTION_PROMPT = new_prompt.strip()
        print(f"📝 记忆提取提示词已更新 ({len(EXTRACTION_PROMPT)}字)")
    else:
        EXTRACTION_PROMPT = _DEFAULT_EXTRACTION_PROMPT
        print(f"📝 记忆提取提示词已恢复默认")