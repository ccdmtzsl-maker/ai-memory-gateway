"""
数据库模块 —— 负责所有跟 PostgreSQL 打交道的事情
==============================================
包括：
- 创建表结构
- 存储对话记录
- 存储/检索记忆（带中文分词和加权排序）
"""

import os
import time
import json
import re
from typing import Optional, List
from datetime import datetime, timedelta, timezone as dt_timezone

import asyncpg

# 时区偏移（和 main.py 保持一致）
TIMEZONE_HOURS = int(os.getenv("TIMEZONE_HOURS", "8"))

DATABASE_URL = os.getenv("DATABASE_URL", "")

HAS_PGVECTOR = False  # 在init_tables时检测

# Embedding 配置（向量搜索用）
EMBEDDING_API_KEY = os.getenv("EMBEDDING_API_KEY", "")
EMBEDDING_BASE_URL = os.getenv("EMBEDDING_BASE_URL", "https://api.openai.com/v1")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "text-embedding-3-small")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "256"))


# ============================================================
# 时段亲和辅助（同一时段的历史记忆加分）
# ============================================================

_LOCAL_UTC_OFFSET_HOURS = int(os.getenv("LOCAL_UTC_OFFSET", "8"))  # 默认 UTC+8


def _time_period(hour: int) -> int:
    """将小时映射到时段：0=早(7-13), 1=午(13-19), 2=晚(19-次日7)"""
    if 7 <= hour < 13:
        return 0
    if 13 <= hour < 19:
        return 1
    return 2


def _period_bonus(now_utc, mem_created_at) -> float:
    """
    同时段且非当天 → 1.0，否则 0.0
    now_utc: datetime (UTC aware)
    mem_created_at: datetime (UTC, 可能 aware 或 naive)
    """
    from datetime import timedelta
    offset = timedelta(hours=_LOCAL_UTC_OFFSET_HOURS)
    # 转本地时间
    now_local = now_utc + offset
    # mem 可能是 naive UTC 或 aware UTC
    mem_utc = mem_created_at.replace(tzinfo=None) if mem_created_at.tzinfo else mem_created_at
    mem_local = mem_utc + offset
    # 排除当天
    if now_local.date() == mem_local.date():
        return 0.0
    # 同时段硬切
    return 1.0 if _time_period(now_local.hour) == _time_period(mem_local.hour) else 0.0


# ============================================================
# 连接池管理
# ============================================================

_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise RuntimeError("DATABASE_URL 未设置！")
        _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5, statement_cache_size=0)
        print("✅ 数据库连接池已创建")
    return _pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        print("✅ 数据库连接池已关闭")


# ============================================================
# 表结构初始化
# ============================================================

# 记忆宫殿向量列的实际维度。None 表示尚未就绪（没建列或建失败）。
MEMORY_PALACE_VECTOR_DIM = None


async def _detect_memory_palace_vector_dim(conn) -> int:
    """决定 vector 列用多少维。

    pgvector 的 vector(N) 必须写死维度，写错了插入就会报错。所以不能
    直接信 EMBEDDING_DIM 这个环境变量——它默认 256，而实际用的模型
    （比如 bge-m3）返回 1024 维。以库里已有向量的真实维度为准最可靠。
    """
    row = await conn.fetchrow("""
        SELECT dimensions, COUNT(*) AS cnt
        FROM memory_palace_vectors
        WHERE dimensions IS NOT NULL AND dimensions > 0
        GROUP BY dimensions
        ORDER BY cnt DESC, dimensions DESC
        LIMIT 1
    """)
    if row and row["dimensions"]:
        return int(row["dimensions"])
    # 库里还没有任何向量：用设置里的维度
    return int(EMBEDDING_DIM or 1024)


async def _ensure_memory_palace_vector_column(conn):
    """建 vector 列、回填历史数据、建索引。可重复执行。"""
    global MEMORY_PALACE_VECTOR_DIM

    existing_dim = await conn.fetchval("""
        SELECT a.atttypmod
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        WHERE c.relname = 'memory_palace_vectors'
          AND a.attname = 'embedding'
          AND a.attnum > 0 AND NOT a.attisdropped
    """)

    if existing_dim is not None:
        # 列已存在。atttypmod 就是 vector 的维度。
        MEMORY_PALACE_VECTOR_DIM = int(existing_dim)
        target_dim = await _detect_memory_palace_vector_dim(conn)
        if target_dim != MEMORY_PALACE_VECTOR_DIM:
            # 换过 embedding 模型，维度变了。不自动重建（会丢数据），
            # 只告警；要迁移就走仪表盘的重建向量流程。
            print(
                f"⚠️ 记忆宫殿向量列是 {MEMORY_PALACE_VECTOR_DIM} 维，"
                f"但现有向量多为 {target_dim} 维；新维度的向量不会写入 pgvector 列"
            )
    else:
        dim = await _detect_memory_palace_vector_dim(conn)
        await conn.execute(
            f"ALTER TABLE memory_palace_vectors ADD COLUMN embedding vector({dim})"
        )
        MEMORY_PALACE_VECTOR_DIM = dim
        print(f"✅ 记忆宫殿向量列已创建（{dim} 维）")

    dim = MEMORY_PALACE_VECTOR_DIM

    # 回填：把 embedding_json 里维度对得上的向量灌进 vector 列。
    # 只处理 embedding IS NULL 的行，所以重复执行不会重做已完成的部分。
    filled = await conn.fetchval(f"""
        WITH candidates AS (
            SELECT memory_id, embedding_json
            FROM memory_palace_vectors
            WHERE embedding IS NULL
              AND embedding_json IS NOT NULL
              AND dimensions = {dim}
              AND TRIM(embedding_json) ~ '^\\[[[:space:]]*-?[0-9]'
            LIMIT 5000
        ), updated AS (
            UPDATE memory_palace_vectors v
            SET embedding = c.embedding_json::vector
            FROM candidates c
            WHERE v.memory_id = c.memory_id
            RETURNING 1
        )
        SELECT COUNT(*) FROM updated
    """)
    if filled:
        print(f"✅ 记忆宫殿向量回填 {filled} 条到 pgvector 列")

    # 索引：优先 HNSW（查询更快），旧版 pgvector 没有就退 IVFFlat。
    # 数据量小的时候索引意义不大，但先建好，省得以后涨上来再折腾。
    for stmt, label in (
        ("""CREATE INDEX IF NOT EXISTS idx_mp_vectors_embedding_hnsw
            ON memory_palace_vectors USING hnsw (embedding vector_cosine_ops)""", "HNSW"),
        ("""CREATE INDEX IF NOT EXISTS idx_mp_vectors_embedding_ivf
            ON memory_palace_vectors USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 20)""", "IVFFlat"),
    ):
        try:
            await conn.execute(stmt)
            break
        except Exception as e:
            print(f"ℹ️ 记忆宫殿向量索引 {label} 建立失败: {str(e)[:120]}")

# 向量粗筛上限：数据库按余弦距离排序后最多返回这么多条。
# 节点少于这个数时等于「全部算一遍」，行为和原来完全一致；
# 涨上去之后就变成「先粗筛 top-N，再由 Python 综合重排」。
MEMORY_PALACE_VECTOR_CANDIDATES = 500


def memory_palace_vector_ready(query_dim: int = 0) -> bool:
    """pgvector 路径能不能用。

    维度必须完全一致：pgvector 的 vector(N) 对不上维度会直接报错，
    而 Python 版余弦会截断到较短的那个长度。宁可回退也不要报错。
    """
    if not HAS_PGVECTOR or not MEMORY_PALACE_VECTOR_DIM:
        return False
    if query_dim and int(query_dim) != int(MEMORY_PALACE_VECTOR_DIM):
        return False
    return True


async def search_memory_palace_vector_scores(
    query_embedding,
    character_id: str = "default",
    room: str = None,
    include_archived: bool = False,
    limit: int = None,
) -> dict:
    """让数据库算余弦相似度，返回 {memory_id: similarity}。

    替代原来「把所有向量的 JSON 文本拉到 Python、逐个 json.loads、
    纯 Python 算余弦」的做法。同样的计算在数据库里用 C 完成，而且
    不占用事件循环（这里是 await，会让出控制权）。

    `<=>` 是 pgvector 的余弦距离运算符，相似度 = 1 - 距离。距离最大
    可以到 2（方向完全相反），所以要夹到 [0, 1]，和 Python 版
    max(0, min(1, ...)) 的口径保持一致。
    """
    if not query_embedding:
        return {}
    if not memory_palace_vector_ready(len(query_embedding)):
        return {}

    cap = int(limit or MEMORY_PALACE_VECTOR_CANDIDATES)
    cap = max(1, min(cap, 5000))
    # asyncpg 没有 vector 类型的编解码器，用文本字面量再显式转型。
    vector_literal = "[" + ",".join(repr(float(x)) for x in query_embedding) + "]"

    sql = """
        SELECT v.memory_id,
               GREATEST(0.0, LEAST(1.0, 1.0 - (v.embedding <=> $1::vector))) AS similarity
        FROM memory_palace_vectors v
        JOIN memory_palace_nodes n ON n.id = v.memory_id
        WHERE v.embedding IS NOT NULL
          AND n.character_id = $2
          AND ($3::boolean OR n.archived = FALSE)
    """
    params = [vector_literal, character_id, include_archived]
    if room:
        sql += " AND n.room = $4"
        params.append(room)
    sql += f" ORDER BY v.embedding <=> $1::vector LIMIT {cap}"

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return {r["memory_id"]: float(r["similarity"]) for r in rows}

async def init_tables():
    global HAS_PGVECTOR
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS conversations (
                id              SERIAL PRIMARY KEY,
                session_id      TEXT NOT NULL,
                role            TEXT NOT NULL,
                content         TEXT,
                model           TEXT,
                created_at      TIMESTAMPTZ DEFAULT NOW(),
                metadata        TEXT
            );
        """)
        
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_conversations_session 
            ON conversations (session_id, created_at);
        """)
        
        # 工具调用支持：加 metadata 字段（已有表自动迁移）
        await conn.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'conversations' AND column_name = 'metadata'
                ) THEN
                    ALTER TABLE conversations ADD COLUMN metadata TEXT;
                END IF;
            END $$;
        """)
        
        # content 允许 NULL（工具调用时 assistant 的 content 可能为空）
        await conn.execute("""
            ALTER TABLE conversations ALTER COLUMN content DROP NOT NULL;
        """)
        
        # 网关配置表（存储运行时可变配置）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS gateway_config (
                key     TEXT PRIMARY KEY,
                value   TEXT DEFAULT ''
            );
        """)
        
        # 分区缓存状态表（存储每个session的轮转状态）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS session_cache_state (
                session_id      TEXT PRIMARY KEY,
                summary         TEXT DEFAULT '',
                a_start_round   INTEGER DEFAULT 0,
                retained_tool_chains JSONB DEFAULT '[]'::jsonb,
                keep_a_tools_enabled BOOLEAN DEFAULT FALSE,
                evicted_through_message_id BIGINT DEFAULT 0,
                updated_at      TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        await conn.execute("""
            ALTER TABLE session_cache_state
            ADD COLUMN IF NOT EXISTS retained_tool_chains JSONB DEFAULT '[]'::jsonb;
        """)
        await conn.execute("""
            ALTER TABLE session_cache_state
            ADD COLUMN IF NOT EXISTS keep_a_tools_enabled BOOLEAN DEFAULT FALSE;
        """)
        await conn.execute("""
            ALTER TABLE session_cache_state
            ADD COLUMN IF NOT EXISTS evicted_through_message_id BIGINT DEFAULT 0;
        """)
        
        # 日印象表（每天一条叙事摘要，不影响碎片/事件/核心三层结构）
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_impressions (
                impression_date     DATE PRIMARY KEY,
                summary             TEXT NOT NULL,
                tags                TEXT DEFAULT '',
                mood                TEXT DEFAULT '',
                source_fragment_ids INTEGER[] DEFAULT NULL,
                created_at          TIMESTAMPTZ DEFAULT NOW(),
                updated_at          TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_daily_impressions_updated
            ON daily_impressions (updated_at DESC);
        """)
        
        # 兼容早期/实验版 daily_impressions 表结构
        await conn.execute("""
            DO $$ BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'daily_impressions' AND column_name = 'date'
                ) AND NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'daily_impressions' AND column_name = 'impression_date'
                ) THEN
                    ALTER TABLE daily_impressions RENAME COLUMN date TO impression_date;
                END IF;
            END $$;
        """)
        await conn.execute("""
            DO $$ BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'daily_impressions' AND column_name = 'topics'
                ) AND NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'daily_impressions' AND column_name = 'tags'
                ) THEN
                    ALTER TABLE daily_impressions RENAME COLUMN topics TO tags;
                END IF;
            END $$;
        """)
        await conn.execute("""
            ALTER TABLE daily_impressions
            ADD COLUMN IF NOT EXISTS impression_date DATE,
            ADD COLUMN IF NOT EXISTS summary TEXT DEFAULT '',
            ADD COLUMN IF NOT EXISTS tags TEXT DEFAULT '',
            ADD COLUMN IF NOT EXISTS mood TEXT DEFAULT '',
            ADD COLUMN IF NOT EXISTS source_fragment_ids INTEGER[] DEFAULT NULL,
            ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
            ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
        """)
        await conn.execute("""
            UPDATE daily_impressions
            SET impression_date = COALESCE(impression_date, created_at::date)
            WHERE impression_date IS NULL;
        """)
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_impressions_date_unique
            ON daily_impressions (impression_date);
        """)
        
        # 尝试启用pgvector扩展（向量搜索）
        try:
            await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            HAS_PGVECTOR = True
            print("✅ pgvector扩展已启用")
            
            # 对话表向量列
            await conn.execute(f"""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'conversations' AND column_name = 'embedding'
                    ) THEN
                        ALTER TABLE conversations ADD COLUMN embedding vector({EMBEDDING_DIM});
                    END IF;
                END $$;
            """)
            
        except Exception as e:
            HAS_PGVECTOR = False
            print(f"⚠️ pgvector不可用（{e}），向量搜索将使用Python端计算")
            
            # 回退：用TEXT列存JSON格式的向量
            await conn.execute("""
                DO $$ BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'conversations' AND column_name = 'embedding_json'
                    ) THEN
                        ALTER TABLE conversations ADD COLUMN embedding_json TEXT;
                    END IF;
                END $$;
            """)
    

        
        # 记忆宫殿（Memory Palace）阶段 1：独立新表
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_palace_nodes (
                id TEXT PRIMARY KEY,
                session_id TEXT,
                character_id TEXT DEFAULT 'default',
                content TEXT NOT NULL,
                room TEXT NOT NULL CHECK (
                    room IN (
                        'living_room',
                        'bedroom',
                        'study',
                        'user_room',
                        'self_room',
                        'attic',
                        'windowsill'
                    )
                ),
                tags TEXT DEFAULT '',
                importance INTEGER DEFAULT 5 CHECK (importance >= 1 AND importance <= 10),
                mood TEXT DEFAULT 'neutral',
                valence DOUBLE PRECISION,
                arousal DOUBLE PRECISION,
                date DATE DEFAULT CURRENT_DATE,
                embedded BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                last_accessed_at TIMESTAMPTZ DEFAULT NOW(),
                access_count INTEGER DEFAULT 0,
                pinned_until TIMESTAMPTZ,
                source_id TEXT,
                origin TEXT DEFAULT 'manual',
                event_box_id TEXT,
                archived BOOLEAN DEFAULT FALSE,
                is_box_summary BOOLEAN DEFAULT FALSE,
                source_message_start_id BIGINT,
                source_message_end_id BIGINT,
                source_session_id TEXT,
                metadata JSONB DEFAULT '{}'::jsonb,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        await conn.execute("""
            ALTER TABLE memory_palace_nodes
            ADD COLUMN IF NOT EXISTS date DATE DEFAULT CURRENT_DATE;
        """)
        await conn.execute("""
            UPDATE memory_palace_nodes
            SET date = COALESCE(date, created_at::date, CURRENT_DATE)
            WHERE date IS NULL;
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_nodes_room
            ON memory_palace_nodes (room);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_nodes_character_room
            ON memory_palace_nodes (character_id, room);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_nodes_created
            ON memory_palace_nodes (created_at DESC);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_nodes_date
            ON memory_palace_nodes (date DESC);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_nodes_event_box
            ON memory_palace_nodes (event_box_id);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_nodes_archived
            ON memory_palace_nodes (archived);
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_palace_vectors (
                memory_id TEXT PRIMARY KEY REFERENCES memory_palace_nodes(id) ON DELETE CASCADE,
                character_id TEXT DEFAULT 'default',
                embedding_json TEXT,
                dimensions INTEGER,
                model TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_vectors_character
            ON memory_palace_vectors (character_id);
        """)

        # 记忆宫殿向量：pgvector 列 + 索引
        #
        # 原来只有 embedding_json TEXT，数据库看到的是一串文本，没法做
        # 向量最近邻搜索。结果每次检索都要把全部节点的向量捞到 Python，
        # 逐个 json.loads + 纯 Python 算余弦，130 节点约 90ms 且完全
        # 阻塞事件循环（实测日志里出现过 1087ms 阻塞）。
        #
        # 加一个真正的 vector 列，让数据库自己算 top-K。
        # embedding_json 保留不动：它是重建向量列的依据，也是 pgvector
        # 不可用时的回退路径。
        if HAS_PGVECTOR:
            try:
                await _ensure_memory_palace_vector_column(conn)
            except Exception as e:
                print(f"⚠️ 记忆宫殿 pgvector 列初始化失败（将回退 Python 计算）: {e}")
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_palace_links (
                id TEXT PRIMARY KEY,
                character_id TEXT DEFAULT 'default',
                source_id TEXT NOT NULL REFERENCES memory_palace_nodes(id) ON DELETE CASCADE,
                target_id TEXT NOT NULL REFERENCES memory_palace_nodes(id) ON DELETE CASCADE,
                link_type TEXT NOT NULL CHECK (
                    link_type IN ('temporal', 'emotional', 'causal', 'person', 'metaphor')
                ),
                strength DOUBLE PRECISION DEFAULT 0.5,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(source_id, target_id, link_type)
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_links_source
            ON memory_palace_links (source_id);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_links_target
            ON memory_palace_links (target_id);
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_palace_event_boxes (
                id TEXT PRIMARY KEY,
                character_id TEXT DEFAULT 'default',
                name TEXT NOT NULL DEFAULT '未命名事件',
                tags TEXT DEFAULT '',
                summary_node_id TEXT REFERENCES memory_palace_nodes(id) ON DELETE SET NULL,
                live_memory_ids TEXT[] DEFAULT '{}',
                archived_memory_ids TEXT[] DEFAULT '{}',
                compression_count INTEGER DEFAULT 0,
                sealed BOOLEAN DEFAULT FALSE,
                predecessor_box_id TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                last_compressed_at TIMESTAMPTZ
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_event_boxes_character
            ON memory_palace_event_boxes (character_id);
        """)
        

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_palace_extracted_messages (
                id BIGSERIAL PRIMARY KEY,
                character_id TEXT DEFAULT 'default',
                session_id TEXT NOT NULL,
                message_id BIGINT NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
                source TEXT DEFAULT 'manual_preview',
                extracted_at TIMESTAMPTZ DEFAULT NOW(),
                metadata JSONB DEFAULT '{}'::jsonb,
                UNIQUE(character_id, message_id)
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_extracted_session
            ON memory_palace_extracted_messages (character_id, session_id, message_id);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_extracted_at
            ON memory_palace_extracted_messages (extracted_at DESC);
        """)


        await conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_palace_extraction_cursor (
                character_id TEXT NOT NULL DEFAULT 'default',
                session_id TEXT NOT NULL,
                last_message_id BIGINT DEFAULT 0,
                last_source TEXT DEFAULT '',
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (character_id, session_id)
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_extraction_cursor_updated
            ON memory_palace_extraction_cursor (updated_at DESC);
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_palace_recall_receipts (
                id BIGSERIAL PRIMARY KEY,
                character_id TEXT DEFAULT 'default',
                session_id TEXT DEFAULT '',
                memory_id TEXT NOT NULL REFERENCES memory_palace_nodes(id) ON DELETE CASCADE,
                injected_at TIMESTAMPTZ DEFAULT NOW(),
                metadata JSONB DEFAULT '{}'::jsonb
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_recall_receipts_time
            ON memory_palace_recall_receipts (character_id, injected_at DESC);
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_mp_recall_receipts_memory
            ON memory_palace_recall_receipts (memory_id);
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS memory_palace_state (
                character_id TEXT PRIMARY KEY DEFAULT 'default',
                last_processed_message_id BIGINT DEFAULT 0,
                digest_round_count INTEGER DEFAULT 0,
                last_digest_at TIMESTAMPTZ,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        # 用户画像 / 印象档案：每个角色一份当前画像
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_impressions (
                id BIGSERIAL PRIMARY KEY,
                character_id TEXT NOT NULL DEFAULT 'default',
                version DOUBLE PRECISION DEFAULT 3.0,
                impression JSONB NOT NULL DEFAULT '{}'::jsonb,
                source_mode TEXT DEFAULT 'initial',
                source_message_count INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE(character_id)
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_user_impressions_updated
            ON user_impressions (updated_at DESC);
        """)

        await conn.execute("""
            ALTER TABLE user_impressions
            ADD COLUMN IF NOT EXISTS last_consumed_node_id TEXT DEFAULT NULL;
        """)
    print("✅ 数据库表结构已就绪")


# ============================================================
# 中文分词工具（基于 jieba）
# ============================================================

import jieba
import jieba.analyse

# 静默加载词典
jieba.setLogLevel(jieba.logging.INFO)

EN_WORD_PATTERN = re.compile(r'[a-zA-Z][a-zA-Z0-9]*')
NUM_PATTERN = re.compile(r'\d{2,}')
# 清理查询开头的时间戳（如 "2026-05-02 20:26"）
TIMESTAMP_PATTERN = re.compile(r'^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}\s*\d{1,2}:\d{1,2}\s*')

# 中文停用词（高频但无搜索价值的词）
_STOP_WORDS = frozenset({
    "的", "了", "在", "是", "我", "你", "他", "她", "它", "们",
    "这", "那", "有", "和", "与", "也", "都", "又", "就", "但",
    "而", "或", "到", "被", "把", "让", "从", "对", "为", "以",
    "及", "等", "个", "不", "没", "很", "太", "吗", "呢", "吧",
    "啊", "嗯", "哦", "哈", "呀", "嘛", "么", "啦", "哇", "喔",
    "会", "能", "要", "想", "去", "来", "说", "做", "看", "给",
    "上", "下", "里", "中", "大", "小", "多", "少", "好", "可以",
    "什么", "怎么", "如何", "哪里", "哪个", "为什么", "还是",
    "然后", "因为", "所以", "虽然", "但是", "可以", "已经",
    "一个", "一些", "一下", "一点", "一起", "一样",
    "比较", "应该", "可能", "如果", "这个", "那个",
    "自己", "知道", "觉得", "感觉", "时候", "现在",
})

# jieba 用户词典补充（默认词典缺失的词）
for _w in ["手账", "手帐", "搭子", "种草", "拔草", "安利", "内卷", "摆烂", "emo", "网关"]:
    jieba.add_word(_w)


def extract_search_keywords(query: str) -> List[str]:
    """
    从查询中提取搜索关键词（TF-IDF + 正则）

    1. 去掉开头的时间戳噪音
    2. 用 jieba.analyse.extract_tags (TF-IDF) 提取中文关键词
    3. 正则提取英文单词
    4. 保留4位以上数字（年份等，过滤短数字噪音）

    例如：
    "2026-05-02 20:26 写写手账看看书 放松大脑" → ["手账", "放松", "大脑"]
    "我昨天在手机上部署了Render然后吃了晚饭" → ["手机", "部署", "Render", "晚饭"]
    "春节干了什么" → ["春节"]
    "2026除夕"    → ["2026", "除夕"]
    """
    # 去掉时间戳前缀
    cleaned = TIMESTAMP_PATTERN.sub('', query).strip()
    if not cleaned:
        cleaned = query

    keywords = set()

    # 英文单词（2字符以上）
    for match in EN_WORD_PATTERN.finditer(cleaned):
        word = match.group()
        if len(word) >= 2:
            keywords.add(word)

    # 数字串（只保留4位以上，过滤 "05" "20" 这种时间噪音）
    for match in NUM_PATTERN.finditer(cleaned):
        num = match.group()
        if len(num) >= 4:
            keywords.add(num)

    # TF-IDF 关键词提取（比手动分词+停用词好很多）
    tags = jieba.analyse.extract_tags(cleaned, topK=10)
    for tag in tags:
        # 跳过纯英文/数字（已在上面处理）
        if EN_WORD_PATTERN.fullmatch(tag) or NUM_PATTERN.fullmatch(tag):
            continue
        if tag in _STOP_WORDS:
            continue
        keywords.add(tag)

    return list(keywords)


# ============================================================
# 向量搜索（OpenAI 兼容 Embedding API）
# ============================================================

async def compute_embedding(text: str) -> list:
    """调用 OpenAI 兼容的 Embedding API 计算文本向量"""
    if not EMBEDDING_API_KEY:
        return []

    try:
        import httpx

        text = str(text or "").strip()
        if not text:
            return []
        if len(text) > 4000:
            text = text[:4000]

        base_body = {
            "model": EMBEDDING_MODEL,
            "input": text,
        }

        async def _post_embedding(client, body):
            resp = await client.post(
                f"{EMBEDDING_BASE_URL}/embeddings",
                headers={
                    "Authorization": f"Bearer {EMBEDDING_API_KEY}",
                    "Content-Type": "application/json",
                },
                json=body,
                timeout=30.0,
            )
            if resp.status_code >= 400:
                preview = ""
                try:
                    preview = resp.text[:500]
                except Exception:
                    preview = ""
                print(f"⚠️ Embedding接口返回 {resp.status_code}: {preview}")
            resp.raise_for_status()
            data = resp.json()
            return data["data"][0]["embedding"]

        async with httpx.AsyncClient() as client:
            body = dict(base_body)
            if EMBEDDING_DIM > 0:
                body["dimensions"] = EMBEDDING_DIM
            try:
                return await _post_embedding(client, body)
            except httpx.HTTPStatusError as e:
                # 部分 OpenAI 兼容服务/模型不接受 dimensions 字段，400 时自动无 dimensions 重试一次。
                if e.response is not None and e.response.status_code == 400 and "dimensions" in body:
                    print("⚠️ Embedding带 dimensions 失败，重试不带 dimensions")
                    return await _post_embedding(client, base_body)
                raise
    except Exception as e:
        print(f"⚠️ Embedding计算失败: {type(e).__name__}: {e}")
        return []


# ============================================================
# 对话记录操作
# ============================================================

async def save_message(session_id: str, role: str, content: str, model: str = "", metadata: str = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO conversations (session_id, role, content, model, metadata) VALUES ($1, $2, $3, $4, $5)",
            session_id, role, content, model, metadata,
        )


async def get_last_user_content(session_id: str) -> str:
    """获取指定session最后一条user消息的content"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT content FROM conversations
            WHERE session_id = $1 AND role = 'user'
            ORDER BY created_at DESC
            LIMIT 1
        """, session_id)
        return row['content'] if row else ""


async def update_last_assistant_message(session_id: str, new_content: str, model: str = ""):
    """覆盖指定session最后一条assistant消息的content（用于re-roll去重）。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id FROM conversations
            WHERE session_id = $1 AND role = 'assistant'
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        """, session_id)
        if row:
            await conn.execute(
                "UPDATE conversations SET content = $1, model = $2 WHERE id = $3",
                new_content, model, row['id']
            )
            return True
        return False


async def update_last_assistant_if_same_user(session_id: str, user_content: str, new_content: str, model: str = "", metadata: str = None):
    """如果最后一条 user 与当前 user 相同，则一次 SQL 内覆盖最后一条 assistant，用于 re-roll 去重。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            WITH last_user AS (
                SELECT content
                FROM conversations
                WHERE session_id = $1 AND role = 'user'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            ),
            last_assistant AS (
                SELECT id
                FROM conversations
                WHERE session_id = $1 AND role = 'assistant'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
            ),
            updated AS (
                UPDATE conversations c
                SET content = $3, model = $4, metadata = $5
                FROM last_user, last_assistant
                WHERE c.id = last_assistant.id
                  AND btrim(COALESCE(last_user.content, '')) = btrim(COALESCE($2, ''))
                RETURNING c.id
            )
            SELECT id FROM updated
        """, session_id, user_content or "", new_content or "", model or "", metadata)
        return bool(row)


async def get_recent_messages(session_id: str, limit: int = 20):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT role, content, metadata, created_at FROM conversations WHERE session_id = $1 ORDER BY created_at DESC LIMIT $2",
            session_id, limit,
        )
        return list(reversed(rows))


async def search_conversations(query: str, limit: int = 20, offset: int = 0):
    """搜索对话内容，返回匹配的session列表"""
    keywords = extract_search_keywords(query)
    if not keywords:
        return [], 0
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        where_parts = []
        params = []
        for i, kw in enumerate(keywords):
            where_parts.append(f"c.content ILIKE '%' || ${i+1} || '%'")
            params.append(kw)
        where_clause = " OR ".join(where_parts)
        
        count_sql = f"""
            SELECT COUNT(DISTINCT c.session_id) as total
            FROM conversations c
            WHERE {where_clause}
        """
        total_row = await conn.fetchrow(count_sql, *params)
        total = total_row['total'] if total_row else 0
        
        if total == 0:
            return [], 0
        
        limit_idx = len(params) + 1
        offset_idx = len(params) + 2
        params.extend([limit, offset])
        
        sql = f"""
            WITH matched_sessions AS (
                SELECT DISTINCT c.session_id
                FROM conversations c
                WHERE {where_clause}
            ),
            session_info AS (
                SELECT 
                    ms.session_id,
                    MIN(c.created_at) as first_time,
                    MAX(c.created_at) as last_time,
                    COUNT(*) as message_count
                FROM matched_sessions ms
                JOIN conversations c ON c.session_id = ms.session_id
                GROUP BY ms.session_id
            )
            SELECT 
                si.session_id,
                si.first_time,
                si.last_time,
                si.message_count
            FROM session_info si
            ORDER BY si.last_time DESC
            LIMIT ${limit_idx} OFFSET ${offset_idx}
        """
        rows = await conn.fetch(sql, *params)
        
        results = []
        for r in rows:
            results.append({
                'session_id': r['session_id'],
                'first_time': r['first_time'].isoformat() if r['first_time'] else None,
                'last_time': r['last_time'].isoformat() if r['last_time'] else None,
                'message_count': r['message_count'],
            })
        
        return results, total


async def update_message_content(message_id: int, new_content: str):
    """更新单条对话消息的内容"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE conversations SET content = $1 WHERE id = $2",
            new_content, message_id,
        )
        return int(result.split()[-1]) if result else 0


# ============================================================
# 网关配置
# ============================================================

async def get_gateway_config(key: str, default: str = "") -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT value FROM gateway_config WHERE key = $1", key)
        return row['value'] if row else default


async def set_gateway_config(key: str, value: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO gateway_config (key, value) VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE SET value = $2
        """, key, value)


async def set_gateway_config_many(items: dict) -> int:
    """批量写入配置，一次数据库往返搞定。

    原来保存设置页会对每个字段单独调 set_gateway_config，35 个字段
    就是 35 次借还连接 + 35 次往返。数据库在美国，单次往返约 25ms，
    加起来接近 800ms。这里合并成一条多值 INSERT。

    返回实际写入的条目数。传空字典时不碰数据库。
    """
    if not items:
        return 0

    keys = list(items.keys())
    values = [str(items[k]) for k in keys]

    # 构造 ($1,$2),($3,$4),... 的占位符。asyncpg 不支持展开列表，
    # 只能手动拼占位符，但参数仍然是绑定传入的，没有注入风险。
    placeholders = ", ".join(
        f"(${i * 2 + 1}, ${i * 2 + 2})" for i in range(len(keys))
    )
    params = []
    for k, v in zip(keys, values):
        params.append(k)
        params.append(v)

    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            INSERT INTO gateway_config (key, value)
            VALUES {placeholders}
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """,
            *params,
        )
    return len(keys)

async def get_all_gateway_config() -> dict:
    """获取所有配置项"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM gateway_config")
        return {r['key']: r['value'] for r in rows}


# ============================================================
# 对话历史读取（分区缓存用）
# ============================================================

async def get_conversation_messages(session_id: str, limit: int = 100):
    """按时间正序读取session的消息"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, role, content, metadata, created_at
            FROM conversations
            WHERE session_id = $1
            ORDER BY created_at ASC, id ASC
            LIMIT $2
        """, session_id, limit)
        return [dict(r) for r in rows]


async def get_conversation_messages_after_id(session_id: str, after_id: int = 0, limit: int = 10000):
    """读取永久分区边界之后的活跃消息；after_id 之前的消息不会重新进入活跃区。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, role, content, metadata, created_at
            FROM conversations
            WHERE session_id = $1 AND id > $2
            ORDER BY created_at ASC, id ASC
            LIMIT $3
        """, session_id, max(0, int(after_id or 0)), max(1, int(limit or 10000)))
        return [dict(r) for r in rows]


async def get_conversation_messages_by_date(event_date, start_hour: int = 0):
    """按本地时间窗口读取对话消息（用于日印象）。

    start_hour=0 表示当天 00:00 到次日 00:00；
    start_hour=7 表示当天 07:00 到次日 07:00。
    """
    try:
        start_hour = int(start_hour or 0)
    except (TypeError, ValueError):
        start_hour = 0
    start_hour = max(0, min(start_hour, 23))

    local_tz = dt_timezone(timedelta(hours=TIMEZONE_HOURS))
    start_local = datetime(event_date.year, event_date.month, event_date.day, start_hour, 0, 0, tzinfo=local_tz)
    end_local = start_local + timedelta(days=1)
    start_utc = start_local.astimezone(dt_timezone.utc)
    end_utc = end_local.astimezone(dt_timezone.utc)

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT session_id, role, content, created_at
            FROM conversations
            WHERE content IS NOT NULL AND content <> ''
            AND created_at >= $1 AND created_at < $2
            ORDER BY created_at ASC
        """, start_utc, end_utc)
        return [dict(r) for r in rows]


# ============================================================
# 分区缓存状态管理
# ============================================================

async def get_session_cache_state(session_id: str) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT summary, a_start_round, retained_tool_chains, keep_a_tools_enabled, evicted_through_message_id, updated_at FROM session_cache_state WHERE session_id = $1",
            session_id
        )
        if row:
            raw_summary = row['summary'] or ''
            summary_parts = []
            if raw_summary:
                try:
                    import json
                    parsed = json.loads(raw_summary)
                    if isinstance(parsed, list):
                        summary_parts = parsed
                    else:
                        summary_parts = [raw_summary]
                except (json.JSONDecodeError, ValueError):
                    summary_parts = [raw_summary]
            raw_retained = row['retained_tool_chains']
            retained_tool_chains = []
            if isinstance(raw_retained, str):
                try:
                    parsed_retained = json.loads(raw_retained)
                    if isinstance(parsed_retained, list):
                        retained_tool_chains = parsed_retained
                except (json.JSONDecodeError, ValueError, TypeError):
                    retained_tool_chains = []
            elif isinstance(raw_retained, list):
                retained_tool_chains = raw_retained

            return {
                'summary_parts': summary_parts,
                'a_start_round': row['a_start_round'] or 0,
                'retained_tool_chains': retained_tool_chains,
                'keep_a_tools_enabled': bool(row['keep_a_tools_enabled']),
                'evicted_through_message_id': int(row['evicted_through_message_id'] or 0),
                'updated_at': row['updated_at'],
            }
        return {'summary_parts': [], 'a_start_round': 0, 'retained_tool_chains': [], 'keep_a_tools_enabled': False, 'evicted_through_message_id': 0, 'updated_at': None}


async def save_session_cache_state(
    session_id: str,
    summary_parts: list,
    a_start_round: int,
    retained_tool_chains: list = None,
    keep_a_tools_enabled: bool = None,
    evicted_through_message_id: int = None,
):
    import json
    summary_json = json.dumps(summary_parts, ensure_ascii=False)
    retained_json = json.dumps(retained_tool_chains, ensure_ascii=False) if retained_tool_chains is not None else None
    keep_enabled = bool(keep_a_tools_enabled) if keep_a_tools_enabled is not None else None
    evicted_through = int(evicted_through_message_id) if evicted_through_message_id is not None else None
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO session_cache_state (
                session_id, summary, a_start_round, retained_tool_chains, keep_a_tools_enabled,
                evicted_through_message_id, updated_at
            )
            VALUES ($1, $2, $3, COALESCE($4::jsonb, '[]'::jsonb), COALESCE($5, FALSE), COALESCE($6, 0), NOW())
            ON CONFLICT (session_id)
            DO UPDATE SET summary = $2, a_start_round = $3,
                          retained_tool_chains = COALESCE($4::jsonb, session_cache_state.retained_tool_chains),
                          keep_a_tools_enabled = COALESCE($5, session_cache_state.keep_a_tools_enabled),
                          evicted_through_message_id = CASE
                              WHEN $6::bigint IS NULL THEN session_cache_state.evicted_through_message_id
                              ELSE GREATEST(session_cache_state.evicted_through_message_id, $6::bigint)
                          END,
                          updated_at = NOW()
        """, session_id, summary_json, a_start_round, retained_json, keep_enabled, evicted_through)




# ============================================================
# 用户画像 / 印象档案（User Impression）阶段 1：基础存取
# ============================================================

def _ui_to_string(value, fallback: str = "") -> str:
    return value if isinstance(value, str) else fallback


def _ui_to_number(value, fallback):
    try:
        if isinstance(value, bool):
            return fallback
        if isinstance(value, (int, float)) and value == value:
            return value
    except Exception:
        pass
    return fallback


def _ui_to_string_list(value) -> list:
    if not isinstance(value, list):
        return []
    result = []
    for item in value:
        text = ""
        if isinstance(item, str):
            text = item.strip()
        elif isinstance(item, dict):
            desc = _ui_to_string(item.get("description")).strip()
            period = _ui_to_string(item.get("period")).strip()
            if desc:
                text = f"[{period}] {desc}" if period else desc
        if text:
            result.append(text)
    return result


def normalize_user_impression(raw):
    """v4.0 用户画像归一化。必填核心(summary/current_state) + 自选标签 + observed_changes。"""
    if not isinstance(raw, dict):
        return None

    # 必填核心
    summary = _ui_to_string(raw.get("summary")).strip()
    current_state = _ui_to_string(raw.get("current_state")).strip()

    # 自选标签区
    TAG_WHITELIST = {
        # A组 价值与喜恶
        "core_values", "likes", "dislikes", "money_attitude", "aesthetic",
        # B组 思维与能力
        "decision_style", "knowledge_map", "thinking_pattern", "humor_style", "learning_style",
        # C组 情绪与相处
        "comfort_zone", "stress_signals", "emotional_triggers", "soothing_methods", "expression_habit",
        # D组 生活与关注
        "life_rhythm", "current_focus", "social_pattern", "attitude_to_me",
        # 可选：兼容旧字段降级成标签
        "mbti_sketch",
        # 白名单外内容的归集标签
        "others",
    }

    def _clean_value(v):
        if isinstance(v, list):
            return _ui_to_string_list(v)
        return _ui_to_string(v).strip()

    raw_tags = raw.get("tags") if isinstance(raw.get("tags"), dict) else {}
    tags_clean = {}
    others = []
    for k, v in raw_tags.items():
        cleaned = _clean_value(v)
        if not cleaned:
            continue
        if k in TAG_WHITELIST:
            if k == "others":
                # LLM 直接输出的 others：统一并入 others 列表
                if isinstance(cleaned, list):
                    others.extend(cleaned)
                else:
                    others.append(cleaned)
            else:
                tags_clean[k] = cleaned
        else:
            # 白名单外的标签内容归入"其他"
            if isinstance(cleaned, list):
                others.append(f"{k}: " + "; ".join(cleaned))
            else:
                others.append(f"{k}: {cleaned}")
    if others:
        tags_clean["others"] = others

    # 变化记录
    observed_changes = _ui_to_string_list(raw.get("observed_changes"))

    # 如果核心和标签都空，返回None
    if not summary and not current_state and not tags_clean:
        return None

    return {
        "version": 4.0,
        "summary": summary,
        "current_state": current_state,
        "tags": tags_clean,
        "observed_changes": observed_changes,
    }


def _serialize_user_impression_row(row):
    if not row:
        return None
    impression = row.get("impression")
    if isinstance(impression, str):
        try:
            impression = json.loads(impression)
        except Exception:
            impression = {}
    return {
        "character_id": row.get("character_id") or "default",
        "version": row.get("version"),
        "impression": impression or {},
        "source_mode": row.get("source_mode") or "initial",
        "source_message_count": int(row.get("source_message_count") or 0),
        "last_consumed_node_id": row.get("last_consumed_node_id"),
        "created_at": row["created_at"].strftime("%Y-%m-%d %H:%M") if row.get("created_at") else None,
        "updated_at": row["updated_at"].strftime("%Y-%m-%d %H:%M") if row.get("updated_at") else None,
    }


async def get_user_impression(character_id: str = "default"):
    character_id = character_id or "default"
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT character_id, version, impression, source_mode, source_message_count, created_at, updated_at, last_consumed_node_id
            FROM user_impressions
            WHERE character_id = $1
        """, character_id)
    pending_count = 0
    if row:
        last_cn = row.get("last_consumed_node_id")
        if last_cn:
            pool2 = await get_pool()
            async with pool2.acquire() as conn2:
                pending_count = await conn2.fetchval(
                    "SELECT COUNT(*) FROM memory_palace_nodes WHERE id > $1 AND archived = FALSE",
                    last_cn
                )
    result = _serialize_user_impression_row(row)
    if result:
        result["pending_memory_count"] = pending_count
    return result


async def upsert_user_impression(character_id: str, impression: dict, source_mode: str = "initial", source_message_count: int = 0, last_consumed_node_id: str = None):
    character_id = character_id or "default"
    source_mode = source_mode if source_mode in ("initial", "update", "manual") else "manual"
    normalized = normalize_user_impression(impression)
    if not normalized:
        raise ValueError("画像内容不完整")
    version = float(normalized.get("version") or 3.0)
    payload = json.dumps(normalized, ensure_ascii=False)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO user_impressions (
                character_id, version, impression, source_mode, source_message_count, updated_at, last_consumed_node_id
            )
            VALUES ($1, $2, $3::jsonb, $4, $5, NOW(), $6)
            ON CONFLICT (character_id) DO UPDATE SET
                version = EXCLUDED.version,
                impression = EXCLUDED.impression,
                source_mode = EXCLUDED.source_mode,
                source_message_count = EXCLUDED.source_message_count,
                updated_at = NOW(),
                last_consumed_node_id = EXCLUDED.last_consumed_node_id
            RETURNING character_id, version, impression, source_mode, source_message_count, created_at, updated_at, last_consumed_node_id
        """, character_id, version, payload, source_mode, int(source_message_count or 0), last_consumed_node_id or None)
    result = _serialize_user_impression_row(row)
    if result:
        last_cn = result.get("last_consumed_node_id")
        pc = 0
        if last_cn:
            pool2 = await get_pool()
            async with pool2.acquire() as conn2:
                pc = await conn2.fetchval(
                    "SELECT COUNT(*) FROM memory_palace_nodes WHERE id > $1 AND archived = FALSE",
                    last_cn
                )
        result["pending_memory_count"] = pc
    return result


async def delete_user_impression(character_id: str = "default"):
    character_id = character_id or "default"
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM user_impressions WHERE character_id = $1", character_id)
    return result


# ============================================================
# Token 使用记录
# ============================================================

async def ensure_token_usage_table():
    """确保token_usage表存在（在init_tables里调用）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS token_usage (
                id              SERIAL PRIMARY KEY,
                session_id      TEXT,
                model           TEXT,
                prompt_tokens   INTEGER DEFAULT 0,
                completion_tokens INTEGER DEFAULT 0,
                total_tokens    INTEGER DEFAULT 0,
                usage_type      TEXT DEFAULT 'chat',
                created_at      TIMESTAMPTZ DEFAULT NOW()
            );
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_token_usage_created ON token_usage (created_at DESC);
        """)


async def save_token_usage(session_id: str, model: str, prompt_tokens: int, completion_tokens: int, total_tokens: int, usage_type: str = "chat"):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO token_usage (session_id, model, prompt_tokens, completion_tokens, total_tokens, usage_type)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, session_id, model, prompt_tokens, completion_tokens, total_tokens, usage_type)


# ============================================================
# 对话记录管理
# ============================================================

async def get_conversations_paginated(page: int = 1, per_page: int = 20):
    offset = (page - 1) * per_page
    pool = await get_pool()
    async with pool.acquire() as conn:
        total_row = await conn.fetchrow("""
            SELECT
                (SELECT COUNT(DISTINCT session_id) FROM conversations)
                +
                (SELECT COUNT(*) FROM session_cache_state scs
                 WHERE NOT EXISTS (SELECT 1 FROM conversations c WHERE c.session_id = scs.session_id))
            AS total
        """)
        total = total_row['total'] if total_row else 0

        rows = await conn.fetch("""
            WITH conv_sessions AS (
                SELECT c.session_id,
                       MIN(c.created_at) as first_time,
                       MAX(c.created_at) as last_time,
                       COUNT(c.id) as message_count
                FROM conversations c
                GROUP BY c.session_id
            ),
            empty_sessions AS (
                SELECT scs.session_id,
                       scs.updated_at as first_time,
                       scs.updated_at as last_time,
                       0 as message_count
                FROM session_cache_state scs
                WHERE NOT EXISTS (SELECT 1 FROM conversations c WHERE c.session_id = scs.session_id)
            ),
            all_sessions AS (
                SELECT * FROM conv_sessions
                UNION ALL
                SELECT * FROM empty_sessions
            ),
            session_info AS (
                SELECT s.session_id,
                       s.first_time,
                       s.last_time,
                       s.message_count
                FROM all_sessions s
                ORDER BY s.last_time DESC
                LIMIT $1 OFFSET $2
            ),
            first_user AS (
                SELECT DISTINCT ON (c.session_id) c.session_id, c.content AS preview
                FROM conversations c
                JOIN session_info si ON si.session_id = c.session_id
                WHERE c.role = 'user'
                ORDER BY c.session_id, c.created_at ASC, c.id ASC
            ),
            usage AS (
                SELECT tu.session_id, SUM(tu.total_tokens) as total_all
                FROM token_usage tu
                JOIN session_info si ON si.session_id = tu.session_id
                WHERE tu.usage_type = 'chat'
                GROUP BY tu.session_id
            )
            SELECT si.*,
                   COALESCE(u.total_all, 0) as total_tokens,
                   COALESCE(fu.preview, '') as preview
            FROM session_info si
            LEFT JOIN usage u ON si.session_id = u.session_id
            LEFT JOIN first_user fu ON si.session_id = fu.session_id
            ORDER BY si.last_time DESC
        """, per_page, offset)
        
        results = []
        for r in rows:
            preview = (r['preview'] or '')[:80]
            title = (preview[:30] + '...' if len(preview) > 30 else preview) or r['session_id']
            results.append({
                'session_id': r['session_id'],
                'title': title,
                'first_time': r['first_time'].isoformat() if r['first_time'] else None,
                'last_time': r['last_time'].isoformat() if r['last_time'] else None,
                'message_count': r['message_count'],
                'preview': preview,
                'total_tokens': r['total_tokens'],
            })
        return results, total


async def delete_conversation(session_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM conversations WHERE session_id = $1", session_id)
        await conn.execute("DELETE FROM session_cache_state WHERE session_id = $1", session_id)


async def batch_delete_conversations(session_ids: list):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM conversations WHERE session_id = ANY($1)", session_ids)
        await conn.execute("DELETE FROM session_cache_state WHERE session_id = ANY($1)", session_ids)


async def merge_sessions_to_target(source_ids: list, target_id: str) -> dict:
    if not source_ids:
        return {'merged_sessions': 0, 'merged_messages': 0, 'merged_token_records': 0}
    pool = await get_pool()
    async with pool.acquire() as conn:
        msg_count = await conn.fetchval("SELECT COUNT(*) FROM conversations WHERE session_id = ANY($1)", source_ids)
        await conn.execute("UPDATE conversations SET session_id = $1 WHERE session_id = ANY($2)", target_id, source_ids)
        token_count = await conn.fetchval("SELECT COUNT(*) FROM token_usage WHERE session_id = ANY($1)", source_ids)
        await conn.execute("UPDATE token_usage SET session_id = $1 WHERE session_id = ANY($2)", target_id, source_ids)
        await conn.execute("DELETE FROM session_cache_state WHERE session_id = ANY($1)", source_ids)
        return {'merged_sessions': len(source_ids), 'merged_messages': msg_count or 0, 'merged_token_records': token_count or 0}


async def list_all_session_cache_states() -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            WITH all_sessions AS (
                SELECT session_id FROM session_cache_state
                UNION
                SELECT DISTINCT session_id FROM conversations
            ),
            conv AS (
                SELECT session_id, COUNT(*) as message_count, MAX(created_at) as last_message_at
                FROM conversations
                GROUP BY session_id
            ),
            usage AS (
                SELECT session_id, SUM(total_tokens) as chat_tokens
                FROM token_usage
                WHERE usage_type = 'chat'
                GROUP BY session_id
            )
            SELECT s.session_id, scs.summary, scs.a_start_round, scs.updated_at,
                   COALESCE(conv.message_count, 0) as message_count,
                   COALESCE(usage.chat_tokens, 0) as chat_tokens,
                   conv.last_message_at
            FROM all_sessions s
            LEFT JOIN session_cache_state scs ON s.session_id = scs.session_id
            LEFT JOIN conv ON s.session_id = conv.session_id
            LEFT JOIN usage ON s.session_id = usage.session_id
            ORDER BY COALESCE(scs.updated_at, conv.last_message_at) DESC NULLS LAST
        """)
        results = []
        for r in rows:
            raw_summary = r['summary'] or ''
            try:
                import json
                parsed = json.loads(raw_summary)
                if isinstance(parsed, list):
                    summary_parts = parsed
                else:
                    summary_parts = [raw_summary] if raw_summary else []
            except (json.JSONDecodeError, ValueError):
                summary_parts = [raw_summary] if raw_summary else []
            results.append({
                'session_id': r['session_id'],
                'summary': '\n\n'.join(summary_parts),
                'summary_length': sum(len(p) for p in summary_parts),
                'summary_count': len(summary_parts),
                'a_start_round': r['a_start_round'] or 0,
                'updated_at': (r['updated_at'] or r['last_message_at']).isoformat() if (r['updated_at'] or r['last_message_at']) else None,
                'message_count': r['message_count'],
                'chat_tokens': r['chat_tokens'],
            })
        return results


async def delete_session_cache_state(session_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM session_cache_state WHERE session_id = $1", session_id)


async def rename_session_id(old_id: str, new_id: str) -> bool:
    """重命名对话线ID（事务内同时修改三个表）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            # 检查新ID是否已存在
            exists = await conn.fetchval(
                "SELECT 1 FROM session_cache_state WHERE session_id = $1", new_id
            )
            if exists:
                return False
            # session_cache_state
            await conn.execute(
                "UPDATE session_cache_state SET session_id = $1 WHERE session_id = $2",
                new_id, old_id
            )
            # conversations
            await conn.execute(
                "UPDATE conversations SET session_id = $1 WHERE session_id = $2",
                new_id, old_id
            )
            # token_usage
            await conn.execute(
                "UPDATE token_usage SET session_id = $1 WHERE session_id = $2",
                new_id, old_id
            )
            return True


def restore_multimodal_content(content):
    """把 DB 存的 image_ref JSON 还原成 OpenAI 多模态数组，供转发上游使用。

    - 纯文本 / 非 image_ref JSON：原样返回字符串
    - 含 image_ref：返回 [{"type":"text",...}, {"type":"image_url","image_url":{"url":...}}]
      时间戳等前缀会并入第一个 text 块，避免暴露在 JSON 外层。
    """
    if not isinstance(content, str):
        return content
    s = content.strip()
    if "image_ref" not in s:
        return content

    # 落库时 clean_user_message_for_log 可能把时间戳拼在 JSON 前面，先剥离
    prefix = ""
    start = s.find("[{")
    if start > 0:
        prefix = s[:start]
        s = s[start:]
    elif start < 0:
        return content
    if not s.endswith("]"):
        return content

    import json as _json
    try:
        blocks = _json.loads(s)
    except Exception:
        return content
    if not isinstance(blocks, list) or not blocks:
        return content
    if not any(isinstance(b, dict) and b.get("type") == "image_ref" for b in blocks):
        return content

    out = []
    prefix_used = False
    for b in blocks:
        if not isinstance(b, dict):
            continue
        btype = b.get("type")
        if btype == "text":
            text = b.get("text") or ""
            if prefix and not prefix_used:
                text = prefix + text
                prefix_used = True
            if text:
                out.append({"type": "text", "text": text})
        elif btype == "image_ref" and b.get("url"):
            out.append({"type": "image_url", "image_url": {"url": b["url"]}})
    if prefix and not prefix_used:
        out.insert(0, {"type": "text", "text": prefix})
    return out or content


def db_row_to_message(row: dict) -> dict:
    """
    把DB记录还原成API消息格式。
    
    普通消息: {"role": "user", "content": "你好"} 
    工具调用: {"role": "assistant", "content": null, "tool_calls": [...]}
    工具结果: {"role": "tool", "content": "结果", "tool_call_id": "call_xxx"}
    思维链:   {"role": "assistant", "content": "回答", "reasoning_content": "思维链"}
    """
    import json as _json
    msg = {"role": row["role"], "content": restore_multimodal_content(row.get("content") or "")}
    
    meta_str = row.get("metadata")

    # 兼容 XML 文本格式工具调用/工具结果：把历史纯文本还原为标准 tool_calls/tool 消息。
    if not meta_str and isinstance(msg.get("content"), str):
        _xml_text = msg.get("content", "").strip()
        _tool_call_match = re.match(r'^<tool\s+name="([^"]+)"\s*>([\s\S]*?)</tool>$', _xml_text)
        if _tool_call_match:
            _params = {}
            for _pm in re.finditer(r'<param\s+name="([^"]+)"\s*>([\s\S]*?)</param>', _tool_call_match.group(2) or ""):
                _params[_pm.group(1)] = _pm.group(2) or ""
            _row_id = str(row.get("id") or row.get("created_at") or int(time.time() * 1000))
            _call_id = "xml_tool_" + re.sub(r'[^\\w-]', "_", _row_id)
            _tool_calls = [{
                "id": _call_id,
                "type": "function",
                "function": {
                    "name": _tool_call_match.group(1),
                    "arguments": _json.dumps(_params, ensure_ascii=False, indent=2)
                }
            }]
            msg = {
                "role": "assistant",
                "content": None,
                "tool_calls": _tool_calls,
                "metadata": {"tool_calls": _tool_calls}
            }
        else:
            _tool_result_match = re.match(r'^<tool_result([\w-]*)\s+([^>]*)>([\s\S]*?)</tool_result[\w-]*>$', _xml_text)
            if _tool_result_match:
                _attrs = dict(re.findall(r'([A-Za-z_][\\w-]*)="([^"]*)"', _tool_result_match.group(2) or ""))
                _body = _tool_result_match.group(3) or ""
                _content_match = re.match(r'^<content>([\s\S]*?)</content>$', _body)
                if _content_match:
                    _body = _content_match.group(1) or ""
                _suffix = (_tool_result_match.group(1) or "").lstrip("_")
                _tool_call_id = _attrs.get("tool_call_id") or _attrs.get("id") or _suffix or ("xml_tool_result_" + re.sub(r'[^\\w-]', "_", str(row.get("id") or "")))
                _tool_name = _attrs.get("name") or "工具结果"
                msg = {
                    "role": "tool",
                    "content": _body,
                    "tool_call_id": _tool_call_id,
                    "name": _tool_name,
                    "metadata": {"tool_call_id": _tool_call_id, "name": _tool_name, "status": _attrs.get("status") or ""}
                }

    # 兼容历史坏数据：早期兜底曾把工具调用/工具结果降级成普通assistant文本。
    # - “工具调用: ...” 没有结构化id，不能可靠恢复为 tool_calls，只置为空格避免污染上游。
    # - “工具结果(call_xxx): ...” 带 tool_call_id，可以恢复为 OpenAI tool 消息。
    if row.get("role") == "assistant" and not meta_str and isinstance(msg.get("content"), str):
        content = msg.get("content", "")
        if content.startswith("工具调用:"):
            msg["content"] = " "
        elif content.startswith("工具结果("):
            end_idx = content.find("):")
            if end_idx > len("工具结果("):
                tool_call_id = content[len("工具结果("):end_idx]
                tool_content = content[end_idx + 2:].lstrip()
                msg = {
                    "role": "tool",
                    "content": tool_content,
                    "tool_call_id": tool_call_id,
                }
    if meta_str:
        try:
            meta = _json.loads(meta_str)
            # assistant 带 tool_calls
            if "tool_calls" in meta:
                msg["tool_calls"] = meta["tool_calls"]
                if not row.get("content"):
                    msg["content"] = None
            # assistant 带 reasoning_content（deepseek thinking mode）
            if "reasoning_content" in meta:
                msg["reasoning_content"] = meta["reasoning_content"]
            # tool 消息带 tool_call_id
            if "tool_call_id" in meta:
                msg["tool_call_id"] = meta["tool_call_id"]
            # 其他可能的字段（name 等）
            if "name" in meta:
                msg["name"] = meta["name"]
        except Exception:
            pass
    
    return msg


async def export_all_conversations():
    """导出所有对话记录（用于备份）"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT session_id, role, content, model, created_at
            FROM conversations
            ORDER BY session_id, created_at
        """)
        return [
            {
                'session_id': r['session_id'],
                'role': r['role'],
                'content': r['content'],
                'model': r['model'] or '',
                'created_at': r['created_at'].isoformat() if r['created_at'] else None,
            }
            for r in rows
        ]


async def import_conversations(records: list):
    """
    导入对话记录（自动去重）
    
    records: [{ session_id, role, content, model?, created_at? }, ...]
    按 session_id + role + created_at 三元组去重，已存在的跳过。
    返回 (导入数量, 跳过数量)
    """
    if not records:
        return 0, 0
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        imported = 0
        skipped = 0
        for r in records:
            session_id = r.get('session_id')
            role = r.get('role')
            content = r.get('content')
            
            if not all([session_id, role, content]):
                continue
            
            model = r.get('model', '')
            created_at = r.get('created_at')
            
            # 解析时间
            from datetime import datetime
            if created_at and isinstance(created_at, str):
                try:
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                except:
                    created_at = None
            
            # 去重检查
            if created_at:
                existing = await conn.fetchrow("""
                    SELECT id FROM conversations
                    WHERE session_id = $1 AND role = $2 AND created_at = $3
                    LIMIT 1
                """, session_id, role, created_at)
                
                if existing:
                    skipped += 1
                    continue
                
                await conn.execute("""
                    INSERT INTO conversations (session_id, role, content, model, created_at)
                    VALUES ($1, $2, $3, $4, $5)
                """, session_id, role, content, model, created_at)
            else:
                await conn.execute("""
                    INSERT INTO conversations (session_id, role, content, model)
                    VALUES ($1, $2, $3, $4)
                """, session_id, role, content, model)
            
            imported += 1
        
        if skipped:
            print(f"📥 导入对话: {imported} 条新增, {skipped} 条已存在跳过")
        else:
            print(f"📥 导入对话: {imported} 条新增")
        
        return imported, skipped


# ============================================================
# 日印象（Daily Impression）
# ============================================================

async def _ensure_daily_impressions_schema(conn):
    """按需确保日印象表结构为新版 tags 字段，避免旧表查询 500。"""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_impressions (
            impression_date     DATE PRIMARY KEY,
            summary             TEXT NOT NULL DEFAULT '',
            tags                TEXT DEFAULT '',
            mood                TEXT DEFAULT '',
            source_fragment_ids INTEGER[] DEFAULT NULL,
            created_at          TIMESTAMPTZ DEFAULT NOW(),
            updated_at          TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    await conn.execute("""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'daily_impressions' AND column_name = 'date'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'daily_impressions' AND column_name = 'impression_date'
            ) THEN
                ALTER TABLE daily_impressions RENAME COLUMN date TO impression_date;
            END IF;
        END $$;
    """)
    await conn.execute("""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'daily_impressions' AND column_name = 'topics'
            ) AND NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'daily_impressions' AND column_name = 'tags'
            ) THEN
                ALTER TABLE daily_impressions RENAME COLUMN topics TO tags;
            END IF;
        END $$;
    """)
    await conn.execute("""
        ALTER TABLE daily_impressions
        ADD COLUMN IF NOT EXISTS impression_date DATE,
        ADD COLUMN IF NOT EXISTS summary TEXT DEFAULT '',
        ADD COLUMN IF NOT EXISTS tags TEXT DEFAULT '',
        ADD COLUMN IF NOT EXISTS mood TEXT DEFAULT '',
        ADD COLUMN IF NOT EXISTS source_fragment_ids INTEGER[] DEFAULT NULL,
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW(),
        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
    """)
    await conn.execute("""
        DO $$ BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'daily_impressions'
                  AND column_name = 'tags'
                  AND data_type = 'ARRAY'
            ) THEN
                ALTER TABLE daily_impressions
                ALTER COLUMN tags TYPE TEXT
                USING array_to_string(tags, '、');
            END IF;
        END $$;
    """)
    await conn.execute("""
        UPDATE daily_impressions
        SET impression_date = COALESCE(impression_date, created_at::date)
        WHERE impression_date IS NULL;
    """)
    await conn.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_impressions_date_unique
        ON daily_impressions (impression_date);
    """)
    await conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_impressions_updated
        ON daily_impressions (updated_at DESC);
    """)


async def upsert_daily_impression(impression_date, summary: str, tags: str = "", mood: str = "", source_fragment_ids: list = None):
    """创建或更新指定日期的日印象。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_daily_impressions_schema(conn)
        row = await conn.fetchrow("""
            INSERT INTO daily_impressions (impression_date, summary, tags, mood, source_fragment_ids, updated_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (impression_date) DO UPDATE SET
                summary = EXCLUDED.summary,
                tags = EXCLUDED.tags,
                mood = EXCLUDED.mood,
                source_fragment_ids = EXCLUDED.source_fragment_ids,
                updated_at = NOW()
            RETURNING impression_date, summary, tags, mood, source_fragment_ids, created_at, updated_at
        """, impression_date, summary, tags or "", mood or "", source_fragment_ids)
        return dict(row) if row else None


async def get_daily_impression(impression_date):
    """读取指定日期的日印象。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_daily_impressions_schema(conn)
        row = await conn.fetchrow("""
            SELECT impression_date, summary, tags, mood, source_fragment_ids, created_at, updated_at
            FROM daily_impressions
            WHERE impression_date = $1
        """, impression_date)
        return dict(row) if row else None


async def list_daily_impressions(limit: int = 30):
    """按日期倒序列出日印象。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _ensure_daily_impressions_schema(conn)
        rows = await conn.fetch("""
            SELECT impression_date, summary, tags, mood, source_fragment_ids, created_at, updated_at
            FROM daily_impressions
            ORDER BY impression_date DESC
            LIMIT $1
        """, max(1, min(int(limit or 30), 100)))
        return [dict(r) for r in rows]


# ============================================================
# 记忆宫殿（Memory Palace）阶段 1：基础 CRUD / 房间统计
# ============================================================

MEMORY_PALACE_ROOMS = [
    {
        "room": "living_room",
        "label": "客厅",
        "description": "日常闲聊、近期互动",
        "capacity": 200,
        "decay_rate": 0.9972,
        "color": "#f59e0b",
    },
    {
        "room": "bedroom",
        "label": "卧室",
        "description": "亲密情感、深层羁绊",
        "capacity": None,
        "decay_rate": 0.9995,
        "color": "#e11d48",
    },
    {
        "room": "study",
        "label": "书房",
        "description": "工作学习、技能成长",
        "capacity": None,
        "decay_rate": 0.9995,
        "color": "#4f46e5",
    },
    {
        "room": "user_room",
        "label": "用户房间",
        "description": "用户个人信息、习惯",
        "capacity": None,
        "decay_rate": 0.9995,
        "color": "#059669",
    },
    {
        "room": "self_room",
        "label": "自我房间",
        "description": "角色自我认同、演变",
        "capacity": None,
        "decay_rate": None,
        "color": "#7c3aed",
    },
    {
        "room": "attic",
        "label": "阁楼",
        "description": "未消化的困惑、潜意识",
        "capacity": None,
        "decay_rate": None,
        "color": "#78716c",
    },
    {
        "room": "windowsill",
        "label": "窗台",
        "description": "期盼、目标、憧憬",
        "capacity": None,
        "decay_rate": None,
        "color": "#0ea5e9",
    },
]

_MEMORY_PALACE_ROOM_SET = {r["room"] for r in MEMORY_PALACE_ROOMS}


def _parse_memory_palace_date(value):
    if not value:
        return None
    if hasattr(value, "toordinal"):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            from datetime import date as date_type
            return date_type.fromisoformat(text[:10])
        except Exception:
            return None
    return None


def _serialize_memory_palace_node(row):
    if not row:
        return None
    data = dict(row)
    for key in ("created_at", "updated_at", "last_accessed_at", "pinned_until"):
        if data.get(key):
            data[key] = data[key].isoformat()
    if data.get("date"):
        data["date"] = data["date"].isoformat()
    return data




def _parse_memory_palace_pinned_until(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            try:
                return datetime.strptime(text[:10], "%Y-%m-%d")
            except Exception:
                return None
    return value

async def clear_expired_memory_palace_pins(character_id: str = None):
    """到期自动摘掉便利贴：清空 pinned_until，保留记忆本体。"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if character_id:
            result = await conn.execute(
                """
                UPDATE memory_palace_nodes
                SET pinned_until = NULL, updated_at = NOW()
                WHERE character_id = $1
                  AND pinned_until IS NOT NULL
                  AND pinned_until <= NOW()
                """,
                character_id,
            )
        else:
            result = await conn.execute(
                """
                UPDATE memory_palace_nodes
                SET pinned_until = NULL, updated_at = NOW()
                WHERE pinned_until IS NOT NULL
                  AND pinned_until <= NOW()
                """
            )
    try:
        return int(str(result).split()[-1])
    except Exception:
        return 0


async def list_memory_palace_rooms(character_id: str = "default"):
    await clear_expired_memory_palace_pins(character_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT room, COUNT(*) AS count
            FROM memory_palace_nodes
            WHERE character_id = $1 AND archived = FALSE
            GROUP BY room
        """, character_id)
    counts = {r["room"]: r["count"] for r in rows}
    result = []
    for room in MEMORY_PALACE_ROOMS:
        item = dict(room)
        item["count"] = int(counts.get(room["room"], 0))
        result.append(item)
    return result


async def list_memory_palace_nodes(
    room: str = None,
    character_id: str = "default",
    archived: bool = False,
    limit: int = 100,
    offset: int = 0,
):
    limit = max(1, min(int(limit or 100), 500))
    offset = max(0, int(offset or 0))
    await clear_expired_memory_palace_pins(character_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        conditions = ["character_id = $1", "archived = $2"]
        params = [character_id, archived]
        if room:
            conditions.append(f"room = ${len(params) + 1}")
            params.append(room)
        params.extend([limit, offset])
        sql = f"""
            SELECT id, character_id, content, room, tags, importance, mood, valence, arousal,
                   date, created_at, updated_at, last_accessed_at, access_count,
                   origin, source_id, pinned_until, session_id, event_box_id,
                   archived, is_box_summary
            FROM memory_palace_nodes
            WHERE {' AND '.join(conditions)}
            ORDER BY date DESC NULLS LAST, created_at DESC, updated_at DESC
            LIMIT ${len(params) - 1} OFFSET ${len(params)}
        """
        rows = await conn.fetch(sql, *params)
    return [_serialize_memory_palace_node(r) for r in rows]


async def get_memory_palace_node(node_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM memory_palace_nodes WHERE id = $1", node_id)
    return _serialize_memory_palace_node(row)


async def create_memory_palace_node(
    node_id: str,
    content: str,
    room: str,
    tags: str = "",
    importance: int = 5,
    mood: str = "neutral",
    valence=None,
    arousal=None,
    date=None,
    character_id: str = "default",
    session_id: str = None,
    origin: str = "manual",
    pinned_until=None,
    metadata=None,
):
    if room not in _MEMORY_PALACE_ROOM_SET:
        room = "living_room"
    importance = max(1, min(10, int(importance or 5)))
    date = _parse_memory_palace_date(date)
    pinned_until = _parse_memory_palace_pinned_until(pinned_until)
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO memory_palace_nodes (
                id, session_id, character_id, content, room, tags, importance, mood,
                valence, arousal, date, pinned_until, origin, metadata, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, COALESCE($11::date, CURRENT_DATE), $12, $13, COALESCE($14::jsonb, '{}'::jsonb), NOW())
            RETURNING *
        """, node_id, session_id, character_id, content, room, tags or "", importance, mood or "neutral",
             valence, arousal, date, pinned_until, origin or "manual", metadata)
    return _serialize_memory_palace_node(row)


async def update_memory_palace_node(node_id: str, data: dict):
    allowed = {
        "content", "room", "tags", "importance", "mood", "valence", "arousal",
        "pinned_until", "archived", "metadata", "date"
    }
    updates = []
    params = []
    for key in allowed:
        if key not in data:
            continue
        value = data.get(key)
        if key == "room" and value not in _MEMORY_PALACE_ROOM_SET:
            value = "living_room"
        if key == "importance":
            value = max(1, min(10, int(value or 5)))
        if key == "date":
            value = _parse_memory_palace_date(value)
        if key == "pinned_until":
            value = _parse_memory_palace_pinned_until(value)
        params.append(value)
        if key == "metadata":
            updates.append(f"{key} = ${len(params)}::jsonb")
        else:
            updates.append(f"{key} = ${len(params)}")
    if not updates:
        return await get_memory_palace_node(node_id)
    params.append(node_id)
    sql = f"""
        UPDATE memory_palace_nodes
        SET {', '.join(updates)}, embedded = FALSE, updated_at = NOW()
        WHERE id = ${len(params)}
        RETURNING *
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(sql, *params)
    return _serialize_memory_palace_node(row)


async def delete_memory_palace_node(node_id: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute("DELETE FROM memory_palace_nodes WHERE id = $1", node_id)
    return result