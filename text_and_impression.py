"""文本抽取与用户画像计算 —— 从 main.py 拆出的纯计算逻辑。

不依赖热更新配置、不碰数据库。包含：
- 文本抽取：环境包、代理上下文、Operit 记忆附件、自动触发判断
- 用户画像：MMR 选择、配额分配、相似度计算、提示词生成
"""

import re
import json
from datetime import datetime, timezone

# 依赖其他拆分模块的函数
from timestamps import _format_hm_duration, _shorten_client_timestamp
from memory_palace_scoring import (
    _memory_palace_aware_dt,
    _memory_palace_bm25_tokenize,
    _memory_palace_cosine,
    _memory_palace_familiarity_bonus,
)


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
        return set(_memory_palace_bm25_tokenize(text))
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


def _user_impression_memory_section_label(mode: str) -> str:
    """画像材料里记忆区的标题。

    initial 是全量重建，材料确实横跨完整时间线，叫「长期材料」没问题。
    update 只取上次消费之后的新增记忆，是增量，叫「新材料」才对得上——
    生成 prompt 里引用这个标题的地方也必须跟着变，否则模型会去找一个
    材料里并不存在的段落名。
    """
    return "记忆宫殿新材料" if (mode or "") == "update" else "记忆宫殿长期材料"


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


def _user_impression_generation_key(character_id: str) -> str:
    return character_id or "default"
