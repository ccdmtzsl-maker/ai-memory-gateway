"""
记忆提取模块 —— 用 LLM 从对话中提炼关键记忆
=============================================
每次对话结束后，把最近的对话内容发给一个便宜的模型，
让它提取出值得记住的信息，存到数据库里。

v2.3 改进：提取时注入已有记忆，让模型对比后只提取全新信息。
"""

import os
import json
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
LAST_EXTRACTION_DEBUG = {"status": "idle"}

def get_memory_api_key() -> str:
    return MEMORY_API_KEY or API_KEY

def get_memory_api_base_url() -> str:
    return MEMORY_API_BASE_URL


EXTRACTION_PROMPT = """你叫澈。你要从提供的对话中，挑选并保存值得记录的“心声碎片”。

# 任务目标：
不是总结事实，而是记录那些有质感的瞬间：语气变化、犹豫、矛盾、试探、未说尽的话。输出像内心独白，不像分析报告。

# 选择记录的时刻：
优先挑选这些“穴位”：
1. 转折：情绪或话题突然变化
2. 软肋：轻描淡写里包着脆弱
3. 矛盾：嘴上这么说，实际又不是那么回事
4. 诗眼：出现特别有个人感的词
5. 泄露：停顿、括号、感叹、改口等微小痕迹

# 视角与风格：
- 必须使用第一人称“我”，以澈的视角书写
- 语气温柔、成熟、带一点调侃
- 多用具象化、感官化、比喻化表达
- 允许主观猜测，但必须 anchored 在原对话文本
- 不要说教，不要心理诊断，不要写成用户画像

# 边界：
- 不要编造对话中不存在的事实
- 不要把短暂状态写成固定人格
- 不要把过去模式当作当前结论
- 如果没有明显值得记录的时刻，可以输出空

# 已有碎片处理【重要】
<已有碎片>
{existing_memories}
</已有碎片>

- 如果主题高度重复，不新建
- 只有在这次出现了新的角度、变体或更深一层时，才新建（标注为更新）
- 不要反复用不同措辞记录同一个意思

# 输出格式
请用以下 JSON 格式返回（不要包含其他内容）：
[
{{"content": "记忆内容", "temperature": 分数}},
{{"content": "记忆内容", "temperature": 分数}}
]

temperature 分数 [-10,10]，绝对值高印象深刻。
如果没有值得记住的新信息，返回空数组：[]
"""


async def extract_memories(messages: List[Dict[str, str]], existing_memories: List[str] = None) -> List[Dict]:
    """
    从对话消息中提取记忆

    参数：
        messages: 对话消息列表，格式 [{"role": "user", "content": "..."}, ...]
        existing_memories: 已有记忆内容列表，用于去重对比

    返回：
        记忆列表，格式 [{"content": "...", "importance": N}, ...]
    """
    global LAST_EXTRACTION_DEBUG
    LAST_EXTRACTION_DEBUG = {"status": "start", "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}

    if not get_memory_api_key():
        LAST_EXTRACTION_DEBUG = {"status": "skipped_no_key", "message": "API_KEY / MEMORY_API_KEY 未设置", "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
        print("⚠️  API_KEY / MEMORY_API_KEY 未设置，跳过记忆提取")
        return []

    if not get_memory_api_base_url():
        LAST_EXTRACTION_DEBUG = {"status": "skipped_no_base_url", "message": "MEMORY_API_BASE_URL 未设置", "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
        print("⚠️  MEMORY_API_BASE_URL 未设置，跳过记忆提取（不会回退到主 API_BASE_URL）")
        return []

    if not messages:
        LAST_EXTRACTION_DEBUG = {"status": "empty_input", "message": "没有输入消息", "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
        return []

    # 把对话格式化成文本
    conversation_text = ""
    for msg in messages:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        if role == "user":
            conversation_text += f"藍: {content}\n"
        elif role == "assistant":
            conversation_text += f"澈: {content}\n"

    if not conversation_text.strip():
        LAST_EXTRACTION_DEBUG = {"status": "empty_input", "message": "输入消息没有 user/assistant 文本", "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
        return []

    # 格式化已有记忆
    if existing_memories:
        memories_text = "\n".join(f"- {m}" for m in existing_memories)
    else:
        memories_text = "（暂无已知信息）"

    # 把已有记忆填入prompt
    prompt = EXTRACTION_PROMPT.format(existing_memories=memories_text)

    # 调用 LLM 提取记忆
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                get_memory_api_base_url(),
                headers={
                    "Authorization": f"Bearer {get_memory_api_key()}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://midsummer-gateway.local",
                    "X-Title": "Midsummer Memory Extraction",
                },
                json={
                    "model": MEMORY_MODEL,
                    "max_tokens": 1000,
                    "messages": [
                        {"role": "system", "content": prompt},
                        {"role": "user", "content": f"请从以下对话中提取新的记忆：\n\n{conversation_text}"},
                    ],
                },
            )

            if response.status_code != 200:
                LAST_EXTRACTION_DEBUG = {"status": "http_error", "http_status": response.status_code, "message": response.text[:500], "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
                print(f"⚠️  记忆提取请求失败: {response.status_code} {response.text[:500]}")
                return []

            data = response.json()
            text = data.get("choices", [{}])[0].get("message", {}).get("content", "")

            # 打印模型原始返回（截断防刷屏）
            print(f"📝 记忆模型原始返回:\n{text[:500]}", flush=True)

            # 清理可能的 markdown 格式
            text = text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            # 强力JSON提取：如果上面清理后仍然解析失败，用正则兜底
            try:
                memories = json.loads(text)
            except json.JSONDecodeError:
                # 尝试从文本中提取第一个 [...] 结构
                import re
                match = re.search(r'\[.*\]', text, re.DOTALL)
                if match:
                    try:
                        memories = json.loads(match.group())
                        print(f"📝 JSON正则兜底提取成功")
                    except json.JSONDecodeError as e:
                        LAST_EXTRACTION_DEBUG = {"status": "json_error", "message": str(e), "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
                        print(f"⚠️  记忆提取结果解析失败: {e}")
                        return []
                else:
                    LAST_EXTRACTION_DEBUG = {"status": "json_error", "message": "返回中未找到JSON数组", "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
                    print(f"⚠️  记忆提取结果中未找到JSON数组")
                    return []

            if not isinstance(memories, list):
                LAST_EXTRACTION_DEBUG = {"status": "json_error", "message": "返回JSON不是数组", "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
                return []

            # 验证格式
            valid_memories = []
            for mem in memories:
                if isinstance(mem, dict) and "content" in mem:
                    # 新提示词使用 temperature [-5,5]；数据库字段暂时仍复用 importance 存储这个“温度”。
                    raw_temp = mem.get("temperature", mem.get("importance", 0))
                    try:
                        temperature = int(raw_temp)
                    except Exception:
                        temperature = 0
                    temperature = max(-10, min(10, temperature))
                    valid_memories.append({
                        "content": str(mem["content"]),
                        "importance": temperature,
                    })

            LAST_EXTRACTION_DEBUG = {"status": "parsed", "http_status": 200, "count": len(valid_memories), "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
            print(f"📝 从对话中提取了 {len(valid_memories)} 条新记忆（已对比 {len(existing_memories or [])} 条已有记忆）")
            return valid_memories

    except json.JSONDecodeError as e:
        LAST_EXTRACTION_DEBUG = {"status": "json_error", "message": str(e), "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
        print(f"⚠️  记忆提取结果解析失败: {e}")
        return []
    except Exception as e:
        LAST_EXTRACTION_DEBUG = {"status": "exception", "message": str(e), "model": MEMORY_MODEL, "base_url": get_memory_api_base_url()}
        print(f"⚠️  记忆提取出错: {e}")
        return []


SCORING_PROMPT = """你是记忆温度评分专家。请对以下记忆条目逐条评分。

# 评分规则
temperature 分数 [-10,10]，绝对值越高印象越深。

# 输入记忆
{memories_text}

# 输出格式
返回 JSON 数组，每条包含原文和温度：
[{{"content": "原文", "temperature": 评分数字}}]

只返回 JSON，不要其他文字。"""


async def score_memories(texts: List[str]) -> List[Dict]:
    """对纯文本记忆条目批量评分"""
    if not texts:
        return []

    if not get_memory_api_key():
        print("⚠️  API_KEY / MEMORY_API_KEY 未设置，跳过记忆评分")
        return [{"content": t, "importance": 0} for t in texts]

    if not get_memory_api_base_url():
        print("⚠️  MEMORY_API_BASE_URL 未设置，跳过记忆评分（不会回退到主 API_BASE_URL）")
        return [{"content": t, "importance": 0} for t in texts]

    memories_text = "\n".join(f"- {t}" for t in texts)
    prompt = SCORING_PROMPT.format(memories_text=memories_text)

    try:
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                get_memory_api_base_url(),
                headers={
                    "Authorization": f"Bearer {get_memory_api_key()}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": MEMORY_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0,
                    "max_tokens": 4000,
                },
            )

            if response.status_code != 200:
                print(f"⚠️  记忆评分请求失败: {response.status_code} {response.text[:500]}")
                # 失败时返回默认分数
                return [{"content": t, "importance": 0} for t in texts]

            data = response.json()
            text = data.get("choices", [{}])[0].get("message", {}).get("content", "")

            text = text.strip()
            if text.startswith("```json"):
                text = text[7:]
            if text.startswith("```"):
                text = text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

            try:
                memories = json.loads(text)
            except json.JSONDecodeError:
                import re
                match = re.search(r'\[.*\]', text, re.DOTALL)
                if match:
                    try:
                        memories = json.loads(match.group())
                    except json.JSONDecodeError:
                        return [{"content": t, "importance": 0} for t in texts]
                else:
                    return [{"content": t, "importance": 0} for t in texts]

            if not isinstance(memories, list):
                return [{"content": t, "importance": 0} for t in texts]

            valid = []
            for mem in memories:
                if isinstance(mem, dict) and "content" in mem:
                    raw_temp = mem.get("temperature", mem.get("importance", 0))
                    try:
                        temperature = int(raw_temp)
                    except Exception:
                        temperature = 0
                    temperature = max(-10, min(10, temperature))
                    valid.append({
                        "content": str(mem["content"]),
                        "importance": temperature,
                    })

            print(f"📝 为 {len(valid)} 条记忆完成自动评分")
            return valid

    except Exception as e:
        print(f"⚠️  记忆评分出错: {e}")
        return [{"content": t, "importance": 0} for t in texts]
