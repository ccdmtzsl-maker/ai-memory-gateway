"""图片归档模块 —— Cloudflare R2 / S3 兼容存储

将对话中的 base64 图片上传到 R2，数据库只存引用链接。
提供上传、删除、引用计数、缓存区清理等功能。
"""

import os
import re
import json
import time
import hashlib
import hmac
import base64
import asyncio
from urllib.parse import quote, urlparse
from datetime import datetime, timezone
# httpx 延迟导入，减少模块加载开销

from database import get_pool, update_message_content

# 由 main.py 启动时注入，避免循环导入
_get_partition_enabled = lambda: False

def set_partition_enabled_getter(fn):
    """main.py 调用此函数注入 CACHE_PARTITION_ENABLED 的读取方式。"""
    global _get_partition_enabled
    _get_partition_enabled = fn

IMAGE_ARCHIVE_ENABLED = os.getenv("IMAGE_ARCHIVE_ENABLED", "false").lower() == "true"
R2_ENDPOINT = os.getenv("R2_ENDPOINT", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "")
R2_BUCKET = os.getenv("R2_BUCKET", "")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")

_IMAGE_DATA_URI_RE = re.compile(r"^data:(image/[a-z0-9.+-]+);base64,(.+)$", re.I | re.S)

_IMAGE_EXT_MAP = {
    "image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png",
    "image/gif": "gif", "image/webp": "webp", "image/bmp": "bmp",
    "image/heic": "heic", "image/heif": "heif", "image/avif": "avif",
}

_r2_upload_cache = {}


def image_archive_ready() -> bool:
    """图片归档是否可用：开关打开且配置齐全。"""
    return bool(IMAGE_ARCHIVE_ENABLED and R2_ENDPOINT and R2_ACCESS_KEY
                and R2_SECRET_KEY and R2_BUCKET and R2_PUBLIC_URL)


def _r2_sigv4_headers(method: str, url: str, payload: bytes, content_type: str) -> dict:
    """为 S3 兼容接口生成 AWS SigV4 签名头（零额外依赖）。"""
    parsed = urlparse(url)
    host = parsed.netloc
    canonical_uri = quote(parsed.path, safe="/~")
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(payload).hexdigest()

    canonical_headers = (f"content-type:{content_type}\n" f"host:{host}\n"
                         f"x-amz-content-sha256:{payload_hash}\n" f"x-amz-date:{amz_date}\n")
    signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join([method, canonical_uri, parsed.query or "",
                                   canonical_headers, signed_headers, payload_hash])

    credential_scope = f"{date_stamp}/auto/s3/aws4_request"
    string_to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, credential_scope,
                                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()])

    def _sign(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = _sign(("AWS4" + R2_SECRET_KEY).encode("utf-8"), date_stamp)
    k_region = _sign(k_date, "auto")
    k_service = _sign(k_region, "s3")
    k_signing = _sign(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    return {
        "Authorization": (f"AWS4-HMAC-SHA256 Credential={R2_ACCESS_KEY}/{credential_scope}, "
                          f"SignedHeaders={signed_headers}, Signature={signature}"),
        "Content-Type": content_type,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }


async def upload_image_to_r2(raw_bytes: bytes, mime: str, session_id: str = "default"):
    """上传图片到 R2，返回 dict；失败返回 None（静默降级）。"""
    if not image_archive_ready() or not raw_bytes:
        return None

    sha = hashlib.sha256(raw_bytes).hexdigest()
    cached = _r2_upload_cache.get(sha)
    if cached:
        return {"url": cached, "sha256": sha, "mime": mime, "size": len(raw_bytes)}

    ext = _IMAGE_EXT_MAP.get((mime or "").lower(), "bin")
    # 内容寻址：key 只由 sha256 决定，同一张图无论何时重发都指向同一对象，
    # 保证 re-roll 生成的 content JSON 完全一致（跨进程重启也成立）。
    object_key = f"conversation-images/{sha[:2]}/{sha}.{ext}"
    url = f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{object_key}"

    try:
        headers = _r2_sigv4_headers("PUT", url, raw_bytes, mime or "application/octet-stream")
        import httpx
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.put(url, content=raw_bytes, headers=headers)
        if resp.status_code not in (200, 201, 204):
            print(f"⚠️ R2 上传失败 HTTP {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"⚠️ R2 上传异常: {e}")
        return None

    public_url = f"{R2_PUBLIC_URL.rstrip('/')}/{object_key}"
    _r2_upload_cache[sha] = public_url
    if len(_r2_upload_cache) > 512:
        for _k in list(_r2_upload_cache.keys())[:128]:
            _r2_upload_cache.pop(_k, None)
    print(f"🖼️ 图片已归档 R2: {object_key} ({len(raw_bytes) // 1024}KB)")
    return {"url": public_url, "sha256": sha, "mime": mime, "size": len(raw_bytes)}


def _extract_image_data_uri(item):
    """从 content 块取出 (mime, base64_body)；非 base64 图片块返回 None。"""
    if not isinstance(item, dict):
        return None
    itype = item.get("type")
    url_val = ""
    if itype == "image_url":
        iu = item.get("image_url")
        if isinstance(iu, dict):
            url_val = iu.get("url") or ""
        elif isinstance(iu, str):
            url_val = iu
    elif itype in ("input_image", "image"):
        url_val = item.get("image_url") or item.get("url") or item.get("data") or ""
    if not isinstance(url_val, str) or not url_val:
        return None
    m = _IMAGE_DATA_URI_RE.match(url_val.strip())
    if not m:
        return None
    return m.group(1), m.group(2)


def content_has_base64_image(content) -> bool:
    """content 里是否含 base64 图片块。"""
    if not isinstance(content, list):
        return False
    return any(_extract_image_data_uri(it) for it in content)


async def archive_images_in_content(content, session_id: str = "default"):
    """把 base64 图片块换成 image_ref 引用块。返回 (new_content, archived_count)。"""
    if not isinstance(content, list) or not image_archive_active():
        return content, 0
    new_items = []
    archived = 0
    for item in content:
        parsed = _extract_image_data_uri(item)
        if not parsed:
            new_items.append(item)
            continue
        mime, b64_body = parsed
        try:
            raw = base64.b64decode(b64_body, validate=False)
        except Exception as e:
            print(f"⚠️ 图片 base64 解码失败，保留原块: {e}")
            new_items.append(item)
            continue
        info = await upload_image_to_r2(raw, mime, session_id=session_id)
        if not info:
            new_items.append(item)
            continue
        new_items.append({
            "type": "image_ref",
            "url": info["url"],
            "mime": info["mime"],
            "sha256": info["sha256"],
            "size": info["size"],
        })
        archived += 1
    return new_items, archived


def content_to_text_with_image_placeholder(content) -> str:
    """content 转纯文本，图片块替换为占位符（供记忆提取/日志使用）。"""
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return str(content or "")
    parts = []
    for item in content:
        if not isinstance(item, dict):
            continue
        itype = item.get("type")
        if itype == "text":
            txt = item.get("text") or ""
            if txt:
                parts.append(txt)
        elif itype in ("image_ref", "image_url", "input_image", "image"):
            parts.append("[图片附件]")
    return " ".join(p for p in parts if p).strip()


def image_archive_active() -> bool:
    """图片归档是否真正生效：需要分区缓存开启 + 归档配置齐全。"""
    return bool(_get_partition_enabled() and image_archive_ready())


def _r2_object_key_from_url(url: str) -> str:
    """从公开 URL 反解出 object key。不属于本 bucket 的 URL 返回空串。"""
    if not url or not isinstance(url, str) or not R2_PUBLIC_URL:
        return ""
    prefix = R2_PUBLIC_URL.rstrip("/") + "/"
    if not url.startswith(prefix):
        return ""
    return url[len(prefix):].lstrip("/")


def extract_image_refs_from_stored_content(content):
    """从 DB 存的 content 里取出所有 image_ref 的 url 列表。"""
    if not isinstance(content, str):
        return []
    s = content.strip()
    if not (s.startswith("[") and s.endswith("]")) or "image_ref" not in s:
        return []
    try:
        blocks = json.loads(s)
    except Exception:
        return []
    if not isinstance(blocks, list):
        return []
    urls = []
    for b in blocks:
        if isinstance(b, dict) and b.get("type") == "image_ref" and b.get("url"):
            urls.append(b["url"])
    return urls


def strip_image_refs_from_stored_content(content):
    """把 content 里的 image_ref 块剔掉，只保留文字。

    返回 (new_content, removed_urls)。没有图片引用时原样返回。
    """
    urls = extract_image_refs_from_stored_content(content)
    if not urls:
        return content, []
    try:
        blocks = json.loads(content.strip())
    except Exception:
        return content, []
    text_parts = [
        b.get("text", "") for b in blocks
        if isinstance(b, dict) and b.get("type") == "text" and b.get("text")
    ]
    return ("\n".join(text_parts).strip(), urls)


async def delete_image_from_r2(url: str) -> bool:
    """从 R2 删除单个对象。成功或对象本就不存在返回 True。"""
    key = _r2_object_key_from_url(url)
    if not key or not image_archive_ready():
        return False
    endpoint = f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"
    try:
        headers = _r2_sigv4_headers("DELETE", endpoint, b"", "application/octet-stream")
        import httpx
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.delete(endpoint, headers=headers)
        if resp.status_code in (200, 202, 204, 404):
            _r2_upload_cache.pop(_sha_from_object_key(key), None)
            print(f"🗑️ R2 已删除: {key}")
            return True
        print(f"⚠️ R2 删除失败 HTTP {resp.status_code}: {resp.text[:160]}")
        return False
    except Exception as e:
        print(f"⚠️ R2 删除异常: {e}")
        return False


def _sha_from_object_key(key: str) -> str:
    """object key 形如 conversation-images/{sess}/{ts}_{sha16}.{ext}，取出 sha 前缀用于清缓存。"""
    try:
        fname = key.rsplit("/", 1)[-1]
        return fname.rsplit(".", 1)[0]
    except Exception:
        return ""


async def count_other_references(url: str, exclude_message_ids=None) -> int:
    """统计还有多少条消息引用同一个图片 URL（排除指定 id）。"""
    if not url:
        return 0
    ids = [int(i) for i in (exclude_message_ids or []) if str(i).isdigit()]
    pool = await get_pool()
    async with pool.acquire() as conn:
        if ids:
            row = await conn.fetchval(
                "SELECT COUNT(*) FROM conversations WHERE content LIKE $1 AND NOT (id = ANY($2::int[]))",
                f"%{url}%", ids,
            )
        else:
            row = await conn.fetchval(
                "SELECT COUNT(*) FROM conversations WHERE content LIKE $1",
                f"%{url}%",
            )
    return int(row or 0)


async def release_images_for_messages(rows, reason: str = "") -> int:
    """把给定消息里的图片引用剔掉并删除 R2 文件（仅当没有其他消息引用时）。

    rows: [{"id": int, "content": str}, ...]
    返回实际删除的 R2 对象数。
    """
    if not image_archive_active() or not rows:
        return 0

    targets = []
    for r in rows:
        content = r.get("content")
        urls = extract_image_refs_from_stored_content(content)
        if urls:
            targets.append((int(r["id"]), content, urls))
    if not targets:
        return 0

    all_ids = [t[0] for t in targets]
    deleted = 0
    for mid, content, urls in targets:
        new_content, removed = strip_image_refs_from_stored_content(content)
        try:
            await update_message_content(mid, new_content)
        except Exception as e:
            print(f"⚠️ 剔除图片引用失败 msg={mid}: {e}")
            continue
        for u in removed:
            try:
                others = await count_other_references(u, exclude_message_ids=all_ids)
            except Exception as e:
                print(f"⚠️ 图片引用计数失败，跳过删除 {u}: {e}")
                continue
            if others > 0:
                print(f"↩️ 图片仍被 {others} 条消息引用，保留: {u}")
                continue
            if await delete_image_from_r2(u):
                deleted += 1
    if deleted:
        print(f"🗑️ 图片清理完成({reason}): 删除 {deleted} 个 R2 对象，涉及 {len(targets)} 条消息")
    return deleted


async def release_images_outside_cache(session_id: str, boundary_id: int, reason: str = "轮转") -> int:
    """清理已离开缓存区(AB区)的消息里的图片：剔除引用 + 删除 R2 文件。"""
    if not image_archive_active() or not session_id or boundary_id <= 0:
        return 0
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT id, content FROM conversations
                WHERE session_id = $1 AND id <= $2
                  AND content LIKE '%image_ref%'
                ORDER BY id
                """,
                session_id, int(boundary_id),
            )
    except Exception as e:
        print(f"⚠️ 查询缓存区外图片消息失败 session={session_id}: {e}")
        return 0
    if not rows:
        return 0
    payload = [{"id": r["id"], "content": r["content"]} for r in rows]
    return await release_images_for_messages(payload, reason=f"{reason}/离开缓存区")


async def release_images_for_session(session_id: str, reason: str = "删除会话") -> int:
    """会话被删除前，清掉该会话所有图片文件（DB 行随后由调用方删除）。"""
    if not image_archive_active() or not session_id:
        return 0
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, content FROM conversations WHERE session_id = $1 AND content LIKE '%image_ref%'",
                session_id,
            )
    except Exception as e:
        print(f"⚠️ 查询会话图片失败 session={session_id}: {e}")
        return 0
    if not rows:
        return 0
    ids = [int(r["id"]) for r in rows]
    deleted = 0
    for r in rows:
        for url in extract_image_refs_from_stored_content(r["content"]):
            try:
                others = await count_other_references(url, exclude_message_ids=ids)
            except Exception as e:
                print(f"⚠️ 引用计数失败，跳过 {url}: {e}")
                continue
            if others > 0:
                print(f"↩️ 图片仍被 {others} 条消息引用，保留: {url}")
                continue
            if await delete_image_from_r2(url):
                deleted += 1
    if deleted:
        print(f"🗑️ 图片清理完成({reason}): 删除 {deleted} 个 R2 对象 session={session_id}")
    return deleted


async def release_images_for_message_id(message_id: int, reason: str = "删除消息") -> int:
    """单条消息被删除/编辑前，清掉它独占的图片文件。"""
    if not image_archive_active() or not message_id:
        return 0
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, content FROM conversations WHERE id = $1", int(message_id)
            )
    except Exception as e:
        print(f"⚠️ 查询消息图片失败 id={message_id}: {e}")
        return 0
    if not row:
        return 0
    urls = extract_image_refs_from_stored_content(row["content"])
    if not urls:
        return 0
    deleted = 0
    for url in urls:
        try:
            others = await count_other_references(url, exclude_message_ids=[int(message_id)])
        except Exception as e:
            print(f"⚠️ 引用计数失败，跳过 {url}: {e}")
            continue
        if others > 0:
            print(f"↩️ 图片仍被 {others} 条消息引用，保留: {url}")
            continue
        if await delete_image_from_r2(url):
            deleted += 1
    if deleted:
        print(f"🗑️ 图片清理完成({reason}): 删除 {deleted} 个 R2 对象 msg={message_id}")
    return deleted


async def release_images_removed_by_edit(message_id: int, new_content: str) -> int:
    """编辑消息时，删除那些在新内容里已不再引用的图片。"""
    if not image_archive_active() or not message_id:
        return 0
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT content FROM conversations WHERE id = $1", int(message_id)
            )
    except Exception as e:
        print(f"⚠️ 编辑前读取消息失败 id={message_id}: {e}")
        return 0
    if not row:
        return 0
    before = set(extract_image_refs_from_stored_content(row["content"]))
    after = set(extract_image_refs_from_stored_content(new_content))
    gone = before - after
    if not gone:
        return 0
    deleted = 0
    for url in gone:
        try:
            others = await count_other_references(url, exclude_message_ids=[int(message_id)])
        except Exception as e:
            print(f"⚠️ 引用计数失败，跳过 {url}: {e}")
            continue
        if others > 0:
            continue
        if await delete_image_from_r2(url):
            deleted += 1
    if deleted:
        print(f"🗑️ 编辑移除图片: 删除 {deleted} 个 R2 对象 msg={message_id}")
    return deleted


def normalize_stored_content_for_text(content):
    """把 DB 里存的 content 还原成纯文本；含 image_ref 的 JSON 转成占位符。

    用于记忆提取、查询构建等只需要文本的场景，避免 JSON 结构污染。
    """
    if not isinstance(content, str):
        return content
    s = content.strip()
    if not (s.startswith("[") and s.endswith("]")):
        return content
    if "image_ref" not in s:
        return content
    try:
        blocks = json.loads(s)
    except Exception:
        return content
    if not isinstance(blocks, list):
        return content
    return content_to_text_with_image_placeholder(blocks) or content

