"""记忆宫殿检索打分 —— 从 main.py 拆出的纯计算逻辑。

这里只放不依赖运行时配置、不碰数据库、不碰进程内可变状态的函数：
BM25 打分、向量余弦、时间衰减、情绪/人物关联强度、综合排序、向量分布统计。

拆分约束（重要）：
main.py 的设置页保存配置时用 globals()[key] = value 热更新模块级变量，
那种改写只在 main.py 自己的命名空间生效。所以本模块刻意不引用任何
会被热更新的配置变量（MEMORY_ENABLED / CACHE_PARTITION_* / API_* 等），
只依赖参数和下面这些编译期常量。往这里加函数时请守住这条线。
"""

import re
import json
import math as _math
from datetime import datetime, timezone


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


_MEMORY_PALACE_RECENCY_DECAY = 0.999


_MEMORY_PALACE_FAMILIARITY_WEIGHT = 0.05


# 首曝加成：从未被检索过的新记忆额外加分，给它们展示机会。
# 加成在 access_count >= 1 时立即消失（被检索过了）。
# 衰减：刚建立 +0.05，72h 后约 +0.02，7 天后 ≈ 0。
_MEMORY_PALACE_FIRST_EXPOSURE_BONUS = 0.05


_MEMORY_PALACE_FIRST_EXPOSURE_DECAY = 0.987  # 每小时 ×0.987


_MEMORY_PALACE_VECTOR_WEIGHT = 0.85


_MEMORY_PALACE_BM25_WEIGHT = 0.15


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
