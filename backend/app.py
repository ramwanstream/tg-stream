#!/usr/bin/env python3
"""
TG Stream backend — private Telegram channel -> public video website.

Single-file backend (beginner-friendly):
  * FastAPI REST API + static frontend hosting
  * Video streaming proxy with HTTP Range (seek) support.
    - Production source: MTProto via Telethon (any file size up to Telegram's
      2GB/file cap; chunks streamed, never fully buffered in RAM).
    - Test source: local file (lets us verify the whole Range pipeline
      without Telegram credentials).
  * SQLite now (Postgres-ready schema); JWT auth; likes/playlists/history.
  * Admin API: channel auto-sync, publish/unpublish, Telegram login flow.

Secrets live ONLY in env vars or files under DATA_DIR (chmod 600).
Nothing secret is ever sent to the browser.

Run:  ADMIN_TOKEN=... ./venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
"""
import asyncio
import hashlib
import hmac
import json
import os
import re
import secrets
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import AsyncIterator, Optional

import jwt as pyjwt
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------- config

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / ".." / "data")).resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
THUMB_DIR = DATA_DIR / "thumbs"
THUMB_DIR.mkdir(exist_ok=True)

DB_PATH = DATA_DIR / "app.db"
SESSION_PATH = DATA_DIR / "tg_session.session"
TG_CONFIG_PATH = DATA_DIR / "tg_config.json"
JWT_SECRET_PATH = DATA_DIR / "jwt_secret"
ADMIN_TOKEN_PATH = DATA_DIR / "admin_token"


def _load_or_create_secret(path: Path, env_val: Optional[str], name: str) -> str:
    if env_val:
        return env_val
    if path.exists():
        return path.read_text().strip()
    val = secrets.token_urlsafe(32)
    path.write_text(val)
    os.chmod(path, 0o600)
    print(f"[config] generated {name}, saved to {path} (keep it private)")
    return val


JWT_SECRET = _load_or_create_secret(JWT_SECRET_PATH, os.environ.get("JWT_SECRET"), "JWT_SECRET")
ADMIN_TOKEN = _load_or_create_secret(ADMIN_TOKEN_PATH, os.environ.get("ADMIN_TOKEN"), "ADMIN_TOKEN")
TG_API_ID = os.environ.get("TG_API_ID")
TG_API_HASH = os.environ.get("TG_API_HASH")
if TG_CONFIG_PATH.exists():
    try:
        cfg = json.loads(TG_CONFIG_PATH.read_text())
        TG_API_ID = TG_API_ID or str(cfg.get("api_id") or "")
        TG_API_HASH = TG_API_HASH or cfg.get("api_hash")
    except Exception:
        pass

CHUNK_SIZE = 1024 * 1024  # 1 MB proxy chunks
VIEW_DEDUP_SECONDS = 3600


def _tg_proxy():
    """Build Telethon proxy tuple from env.

    This VM reaches the internet only through an egress HTTP proxy, and
    Telethon's raw TCP to Telegram DCs must go through it (HTTP CONNECT).
    On hosts with direct internet (e.g. Render) the env var is absent and
    this returns None -> direct connection.
    """
    url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or ""
    if not url:
        return None
    try:
        from urllib.parse import urlparse
        u = urlparse(url)
        if not u.hostname or u.scheme not in ("http", "https"):
            return None
        # python_socks order: (protocol, host, port, username, password, rdns)
        return ("http", u.hostname, u.port or 3128, u.username, u.password, True)
    except Exception:
        return None


TG_PROXY = _tg_proxy()

# ---------------------------------------------------------------- db

def db() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    return con


def init_db():
    schema = (BASE_DIR / "schema.sql").read_text()
    con = db()
    con.executescript(schema)
    con.commit()
    con.close()


def row_to_dict(r: sqlite3.Row) -> dict:
    return dict(r) if r is not None else None


# ---------------------------------------------------------------- auth

def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000)
    return f"pbkdf2${salt}${dk.hex()}"


def check_password(password: str, stored: str) -> bool:
    try:
        _, salt, hexdk = stored.split("$")
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000)
        return hmac.compare_digest(dk.hex(), hexdk)
    except Exception:
        return False


def make_token(user_id: int) -> str:
    payload = {
        "uid": user_id,
        "exp": datetime.now(timezone.utc) + timedelta(days=7),
        "iat": datetime.now(timezone.utc),
    }
    return pyjwt.encode(payload, JWT_SECRET, algorithm="HS256")


def get_user_from_header(authorization: Optional[str]) -> Optional[dict]:
    if not authorization or not authorization.startswith("Bearer "):
        return None
    try:
        payload = pyjwt.decode(authorization[7:], JWT_SECRET, algorithms=["HS256"])
        con = db()
        r = con.execute("SELECT id, name, email, created_at FROM users WHERE id=?",
                        (payload["uid"],)).fetchone()
        con.close()
        return row_to_dict(r)
    except Exception:
        return None


def require_user(authorization: Optional[str] = Header(None)) -> dict:
    user = get_user_from_header(authorization)
    if not user:
        raise HTTPException(401, "Login required")
    return user


def optional_user(authorization: Optional[str] = Header(None)) -> Optional[dict]:
    return get_user_from_header(authorization)


def require_admin(x_admin_token: Optional[str] = Header(None)):
    if not x_admin_token or not hmac.compare_digest(x_admin_token, ADMIN_TOKEN):
        raise HTTPException(403, "Admin only")


# ---------------------------------------------------------------- app

app = FastAPI(title="TG Stream")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()

# ============================================================ media sources
# A source exposes: total byte size + an async byte-range reader.
# The /api/stream endpoint is identical for every source, so the whole
# Range/seek pipeline can be tested with a local file before Telegram
# credentials exist.

class MediaSource:
    mime = "video/mp4"

    async def size(self) -> int:
        raise NotImplementedError

    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        """Yield bytes [start, end] inclusive, in chunks. Never buffer whole file."""
        raise NotImplementedError


class LocalFileSource(MediaSource):
    """Test-only source: streams a file from disk through the same pipeline."""

    def __init__(self, path: str, mime: str = "video/mp4"):
        p = Path(path)
        if not p.is_absolute():
            # resolve relative paths against the project root (tg-stream/)
            p = (BASE_DIR.parent / p).resolve()
        self.path = str(p)
        self.mime = mime

    async def size(self) -> int:
        return os.path.getsize(self.path)

    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        loop = asyncio.get_event_loop()

        def _read():
            out = []
            with open(self.path, "rb") as f:
                f.seek(start)
                remaining = end - start + 1
                while remaining > 0:
                    chunk = f.read(min(CHUNK_SIZE, remaining))
                    if not chunk:
                        break
                    out.append(chunk)
                    remaining -= len(chunk)
            return out

        for chunk in await loop.run_in_executor(None, _read):
            yield chunk


class TelegramSource(MediaSource):
    """Production source: MTProto via Telethon. Streams chunks on demand,
    so a 2 GB file never sits fully in server RAM."""

    def __init__(self, manager: "TelegramManager", channel_id: int, message_id: int,
                 file_size: int, mime: str):
        self.mgr = manager
        self.channel_id = channel_id
        self.message_id = message_id
        self._size = file_size
        self.mime = mime or "video/mp4"

    async def size(self) -> int:
        return self._size

    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        client = await self.mgr.client()
        # Re-fetch the message every request: Telegram file references expire,
        # a fresh fetch always gives a valid one.
        msg = await client.get_messages(self.mgr.channel_id, ids=self.message_id)
        if not msg or not msg.media:
            raise FileNotFoundError("Telegram message/file no longer available")
        doc = msg.media.document if hasattr(msg.media, "document") else None
        if doc is None:
            # video messages: document lives under .document too
            doc = getattr(msg.media, "document", None)
        if doc is None:
            raise FileNotFoundError("No downloadable file in this message")
        length = end - start + 1
        # NOTE: iter_download's `limit` counts CHUNKS, not bytes.
        # Compute how many chunks cover `length` bytes, then trim the tail
        # so we yield exactly [start, end] — never more, never buffered whole.
        request_size = min(CHUNK_SIZE, length)
        limit = (length + request_size - 1) // request_size + 1
        remaining = length
        async for chunk in client.iter_download(
            doc, offset=start, limit=limit, request_size=request_size
        ):
            data = bytes(chunk)
            if len(data) > remaining:
                data = data[:remaining]
            if data:
                yield data
                remaining -= len(data)
            if remaining <= 0:
                break


# ------------------------------------------------------- telegram manager

class TelegramManager:
    """Lazy MTProto client. Connects only when credentials exist."""

    def __init__(self):
        self._client = None
        self._lock = asyncio.Lock()
        self.channel_id: Optional[int] = None
        self.last_error: Optional[str] = None
        self.last_sync: Optional[str] = None
        self._load_channel()

    def _load_channel(self):
        p = DATA_DIR / "channel.json"
        if p.exists():
            try:
                self.channel_id = json.loads(p.read_text()).get("channel_id")
            except Exception:
                pass

    def save_channel(self, channel_id: int):
        self.channel_id = channel_id
        p = DATA_DIR / "channel.json"
        p.write_text(json.dumps({"channel_id": channel_id}))
        os.chmod(p, 0o600)

    def configured(self) -> bool:
        return bool(TG_API_ID and TG_API_HASH)

    async def client(self):
        if not self.configured():
            raise RuntimeError("Telegram API credentials not configured (see /admin setup)")
        async with self._lock:
            if self._client is None:
                from telethon import TelegramClient
                from telethon.sessions import SQLiteSession
                self._client = TelegramClient(
                    SQLiteSession(str(SESSION_PATH)),
                    int(TG_API_ID), TG_API_HASH,
                    proxy=TG_PROXY,
                )
                await self._client.connect()
            if not await self._client.is_user_authorized():
                raise RuntimeError("Telegram session not logged in (see /admin setup)")
            return self._client

    async def status(self) -> dict:
        st = {"configured": self.configured(), "channel_id": self.channel_id,
              "last_sync": self.last_sync, "last_error": self.last_error}
        if not self.configured():
            st["connected"] = False
            return st
        try:
            c = await self.client()
            me = await c.get_me()
            st["connected"] = True
            st["account"] = getattr(me, "username", None) or str(me.id)
        except Exception as e:
            st["connected"] = False
            st["last_error"] = str(e)
        return st


tg = TelegramManager()


def get_source_for_video(video: dict) -> MediaSource:
    if video.get("local_path"):
        return LocalFileSource(video["local_path"], video.get("mime") or "video/mp4")
    if not tg.channel_id or not video.get("tg_message_id"):
        raise HTTPException(503, "Video source not configured yet")
    return TelegramSource(tg, tg.channel_id, video["tg_message_id"],
                          video["file_size"], video.get("mime"))


# ------------------------------------------------------- channel sync
# "jitne bhi video channel me honge, wo sare website pe show honge":
# every video/document in the private channel becomes a published listing
# automatically. Admin can unpublish individually; deleting a listing never
# touches the Telegram file.

def _video_attrs(msg) -> Optional[dict]:
    """Extract (unique_id, file_size, duration, mime, file_name) from a message."""
    doc = None
    if getattr(msg, "video", None):
        doc = msg.video
    elif getattr(msg, "document", None):
        doc = msg.document
        mt = (doc.mime_type or "")
        if not (mt.startswith("video/") or mt.startswith("audio/")):
            return None
    if doc is None:
        return None
    # NOTE: doc.file_reference is NOT stable (it can expire). Dedup uses
    # channel_id:message_id, set by the caller in sync_channel().
    unique_id = f"{doc.id}"
    duration, file_name, w, h = 0, "", 0, 0
    for attr in getattr(doc, "attributes", []):
        t = type(attr).__name__
        if t == "DocumentAttributeVideo":
            duration = int(getattr(attr, "duration", 0) or 0)
            w, h = int(getattr(attr, "w", 0) or 0), int(getattr(attr, "h", 0) or 0)
        elif t == "DocumentAttributeFilename":
            file_name = getattr(attr, "file_name", "") or ""
    return {
        "unique_id": unique_id,
        "file_size": int(getattr(doc, "size", 0) or 0),
        "duration": duration,
        "mime": getattr(doc, "mime_type", "video/mp4") or "video/mp4",
        "file_name": file_name,
    }


async def sync_channel(limit: int = 300) -> dict:
    """Import new videos from the private channel. Returns counts."""
    client = await tg.client()
    if not tg.channel_id:
        raise RuntimeError("Channel not set (see /admin setup)")
    added, skipped = 0, 0
    con = db()
    try:
        async for msg in client.iter_messages(tg.channel_id, limit=limit):
            attrs = _video_attrs(msg)
            if not attrs:
                continue
            # stable identity: a message's media never changes
            attrs["unique_id"] = f"{tg.channel_id}:{msg.id}"
            exists = con.execute(
                "SELECT id FROM videos WHERE tg_file_unique_id=?",
                (attrs["unique_id"],)).fetchone()
            if exists:
                skipped += 1
                continue
            title = (msg.text or "").strip().split("\n")[0][:120] or attrs["file_name"] or "Untitled video"
            con.execute(
                """INSERT INTO videos
                   (title, description, category_id, tg_channel_id, tg_message_id,
                    tg_file_unique_id, file_size, duration, mime, visibility,
                    published_at)
                   VALUES (?, ?, (SELECT id FROM categories WHERE slug='uncategorized'),
                           ?, ?, ?, ?, ?, ?, 'published', datetime('now'))""",
                (title, (msg.text or "").strip()[:2000], tg.channel_id,
                 msg.id, attrs["unique_id"], attrs["file_size"],
                 attrs["duration"], attrs["mime"]),
            )
            added += 1
        con.commit()
    finally:
        con.close()
    tg.last_sync = datetime.now(timezone.utc).isoformat()
    # thumbnails for the newly added ones (best-effort, background)
    asyncio.create_task(_fetch_thumbs())
    return {"added": added, "skipped": skipped}


async def _fetch_thumbs():
    """Download Telegram video thumbs for videos missing one."""
    try:
        client = await tg.client()
        con = db()
        rows = con.execute(
            "SELECT id, tg_message_id FROM videos WHERE thumb_path IS NULL "
            "AND tg_message_id IS NOT NULL LIMIT 25").fetchall()
        for r in rows:
            try:
                msg = await client.get_messages(tg.channel_id, ids=r["tg_message_id"])
                if not msg or not msg.media:
                    continue
                dest = str(THUMB_DIR / f"{r['id']}.jpg")
                got = await client.download_media(msg, file=dest, thumb=-1)
                if got:
                    con.execute("UPDATE videos SET thumb_path=? WHERE id=?", (dest, r["id"]))
                    con.commit()
            except Exception:
                continue
        con.close()
    except Exception:
        pass


async def _poller():
    """Background: re-check the channel every 5 minutes for new videos."""
    await asyncio.sleep(20)  # let startup finish
    while True:
        try:
            if tg.configured() and tg.channel_id:
                await sync_channel(limit=100)
                tg.last_error = None
        except Exception as e:
            tg.last_error = str(e)[:300]
        await asyncio.sleep(300)

# ============================================================ public API

def _public_video_row(r: sqlite3.Row, user_id: Optional[int] = None) -> dict:
    v = row_to_dict(r)
    out = {
        "id": v["id"], "title": v["title"], "description": v["description"],
        "category": v["cat_name"], "category_slug": v["cat_slug"],
        "duration": v["duration"], "file_size": v["file_size"],
        "view_count": v["view_count"], "status": v["status"],
        "published_at": v["published_at"],
        "thumb_url": f"/api/thumb/{v['id']}" if v["thumb_path"] else None,
        "stream_url": f"/api/stream/{v['id']}",
    }
    con = db()
    out["like_count"] = con.execute(
        "SELECT COUNT(*) c FROM likes WHERE video_id=?", (v["id"],)).fetchone()["c"]
    out["liked"] = False
    out["resume_position"] = 0
    if user_id:
        out["liked"] = bool(con.execute(
            "SELECT 1 FROM likes WHERE user_id=? AND video_id=?",
            (user_id, v["id"])).fetchone())
        h = con.execute(
            "SELECT position FROM watch_history WHERE user_id=? AND video_id=?",
            (user_id, v["id"])).fetchone()
        if h:
            out["resume_position"] = h["position"]
    con.close()
    return out


VIDEO_SELECT = """SELECT v.*, c.name AS cat_name, c.slug AS cat_slug
                  FROM videos v LEFT JOIN categories c ON c.id = v.category_id"""


@app.get("/api/videos")
def list_videos(q: Optional[str] = None, category: Optional[str] = None,
                page: int = 1, per_page: int = 24,
                user: Optional[dict] = Depends(optional_user)):
    per_page = max(1, min(per_page, 60))
    page = max(1, page)
    where = ["v.visibility='published'"]
    params: list = []
    if q:
        where.append("(v.title LIKE ? OR v.description LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]
    if category and category != "all":
        where.append("c.slug = ?")
        params.append(category)
    sql = f"{VIDEO_SELECT} WHERE {' AND '.join(where)} ORDER BY v.published_at DESC NULLS LAST, v.id DESC"
    con = db()
    total = con.execute(
        f"SELECT COUNT(*) c FROM videos v LEFT JOIN categories c ON c.id=v.category_id "
        f"WHERE {' AND '.join(where)}", params).fetchone()["c"]
    rows = con.execute(sql + " LIMIT ? OFFSET ?",
                       params + [per_page, (page - 1) * per_page]).fetchall()
    con.close()
    uid = user["id"] if user else None
    return {"total": total, "page": page, "per_page": per_page,
            "items": [_public_video_row(r, uid) for r in rows]}


@app.get("/api/videos/{video_id}")
def video_detail(video_id: int, user: Optional[dict] = Depends(optional_user)):
    con = db()
    r = con.execute(f"{VIDEO_SELECT} WHERE v.id=?", (video_id,)).fetchone()
    con.close()
    if not r or row_to_dict(r)["visibility"] != "published":
        raise HTTPException(404, "Video not found")
    uid = user["id"] if user else None
    detail = _public_video_row(r, uid)
    # related: same category first, then latest
    con = db()
    rel = con.execute(
        f"""{VIDEO_SELECT} WHERE v.visibility='published' AND v.id != ?
            ORDER BY (c.slug = ?) DESC, v.published_at DESC LIMIT 8""",
        (video_id, detail["category_slug"])).fetchall()
    con.close()
    detail["related"] = [_public_video_row(x, uid) for x in rel]
    return detail


@app.get("/api/categories")
def list_categories():
    con = db()
    rows = con.execute(
        """SELECT c.slug, c.name, COUNT(v.id) AS count FROM categories c
           LEFT JOIN videos v ON v.category_id=c.id AND v.visibility='published'
           GROUP BY c.id ORDER BY count DESC, c.name""").fetchall()
    con.close()
    return [dict(r) for r in rows]


@app.get("/api/thumb/{video_id}")
def thumb(video_id: int):
    con = db()
    r = con.execute("SELECT thumb_path FROM videos WHERE id=?", (video_id,)).fetchone()
    con.close()
    if r and r["thumb_path"]:
        p = Path(r["thumb_path"])
        if not p.is_absolute():
            p = (BASE_DIR.parent / p).resolve()
        if p.exists():
            return FileResponse(str(p), media_type="image/jpeg")
    raise HTTPException(404, "No thumbnail")


_view_seen: dict = {}


@app.post("/api/videos/{video_id}/view")
def register_view(video_id: int, request: Request):
    key = (request.client.host if request.client else "?", video_id)
    now = time.time()
    if _view_seen.get(key, 0) > now - VIEW_DEDUP_SECONDS:
        return {"counted": False}
    _view_seen[key] = now
    con = db()
    con.execute("UPDATE videos SET view_count = view_count + 1 WHERE id=?", (video_id,))
    con.commit()
    n = con.execute("SELECT view_count FROM videos WHERE id=?", (video_id,)).fetchone()
    con.close()
    return {"counted": True, "view_count": n["view_count"] if n else 0}


# ------------------------------------------------------- streaming
# HTTP Range support: the browser asks for "bytes=start-end", we answer
# 206 Partial Content. This is what makes seeking work.

RANGE_RE = re.compile(r"bytes=(\d*)-(\d*)")


def parse_range(header: Optional[str], size: int) -> Optional[tuple]:
    """Return (start, end) inclusive, or None for 'send everything'."""
    if not header:
        return None
    m = RANGE_RE.match(header.strip())
    if not m:
        raise HTTPException(416, "Invalid Range header")
    s, e = m.groups()
    if s == "" and e == "":
        raise HTTPException(416, "Invalid Range header")
    if s == "":  # suffix: last N bytes
        n = int(e)
        if n == 0:
            raise HTTPException(416, "Invalid Range header")
        start, end = max(0, size - n), size - 1
    else:
        start = int(s)
        end = int(e) if e != "" else size - 1
    if start >= size or end < start:
        raise HTTPException(416, "Range not satisfiable",
                            headers={"Content-Range": f"bytes */{size}"})
    return start, min(end, size - 1)


@app.get("/api/stream/{video_id}")
async def stream_video(video_id: int, request: Request):
    con = db()
    r = con.execute("SELECT * FROM videos WHERE id=? AND visibility='published'",
                    (video_id,)).fetchone()
    con.close()
    if not r:
        raise HTTPException(404, "Video not found")
    v = row_to_dict(r)
    if v["status"] == "unavailable":
        raise HTTPException(410, "This video is no longer available")
    try:
        source = get_source_for_video(v)
        size = await source.size()
    except HTTPException:
        raise
    except RuntimeError as e:
        raise HTTPException(503, str(e))
    except FileNotFoundError:
        con = db()
        con.execute("UPDATE videos SET status='unavailable' WHERE id=?", (video_id,))
        con.commit()
        con.close()
        raise HTTPException(410, "This video is no longer available on Telegram")
    except Exception as e:
        raise HTTPException(502, f"Could not reach video source: {str(e)[:200]}")

    if size <= 0:
        raise HTTPException(502, "Video source returned an empty file")

    rng = parse_range(request.headers.get("range"), size)
    if rng is None:
        start, end, status = 0, size - 1, 200
    else:
        start, end, status = rng[0], rng[1], 206
    length = end - start + 1
    headers = {
        "Content-Type": source.mime,
        "Accept-Ranges": "bytes",
        "Content-Length": str(length),
        "Cache-Control": "no-cache",
    }
    if status == 206:
        headers["Content-Range"] = f"bytes {start}-{end}/{size}"
    return StreamingResponse(source.read_range(start, end),
                             status_code=status, headers=headers)


# ============================================================ auth + user features

@app.post("/api/auth/signup")
def signup(body: dict):
    name = (body.get("name") or "").strip()[:60]
    email = (body.get("email") or "").strip().lower()[:120]
    password = body.get("password") or ""
    if not name or "@" not in email or len(password) < 6:
        raise HTTPException(400, "Name, valid email and 6+ character password required")
    con = db()
    try:
        cur = con.execute(
            "INSERT INTO users (name, email, password_hash) VALUES (?,?,?)",
            (name, email, hash_password(password)))
        con.commit()
        uid = cur.lastrowid
    except sqlite3.IntegrityError:
        con.close()
        raise HTTPException(400, "Email already registered")
    r = con.execute("SELECT id, name, email, created_at FROM users WHERE id=?", (uid,)).fetchone()
    con.close()
    return {"token": make_token(uid), "user": row_to_dict(r)}


@app.post("/api/auth/login")
def login(body: dict):
    email = (body.get("email") or "").strip().lower()
    password = body.get("password") or ""
    con = db()
    r = con.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    con.close()
    if not r or not check_password(password, r["password_hash"]):
        raise HTTPException(401, "Wrong email or password")
    u = row_to_dict(r)
    return {"token": make_token(u["id"]),
            "user": {k: u[k] for k in ("id", "name", "email", "created_at")}}


@app.get("/api/auth/me")
def me(user: dict = Depends(require_user)):
    return user


@app.post("/api/videos/{video_id}/like")
def toggle_like(video_id: int, user: dict = Depends(require_user)):
    con = db()
    if not con.execute("SELECT id FROM videos WHERE id=?", (video_id,)).fetchone():
        con.close()
        raise HTTPException(404, "Video not found")
    existing = con.execute("SELECT 1 FROM likes WHERE user_id=? AND video_id=?",
                           (user["id"], video_id)).fetchone()
    if existing:
        con.execute("DELETE FROM likes WHERE user_id=? AND video_id=?", (user["id"], video_id))
        liked = False
    else:
        con.execute("INSERT INTO likes (user_id, video_id) VALUES (?,?)", (user["id"], video_id))
        liked = True
    con.commit()
    count = con.execute("SELECT COUNT(*) c FROM likes WHERE video_id=?", (video_id,)).fetchone()["c"]
    con.close()
    return {"liked": liked, "like_count": count}


@app.get("/api/likes")
def my_likes(user: dict = Depends(require_user)):
    con = db()
    rows = con.execute(
        f"""{VIDEO_SELECT} JOIN likes l ON l.video_id = v.id
            WHERE l.user_id=? AND v.visibility='published'
            ORDER BY l.created_at DESC LIMIT 100""", (user["id"],)).fetchall()
    con.close()
    return [_public_video_row(r, user["id"]) for r in rows]


@app.get("/api/playlists")
def my_playlists(user: dict = Depends(require_user)):
    con = db()
    rows = con.execute(
        """SELECT p.*, COUNT(i.id) AS items FROM playlists p
           LEFT JOIN playlist_items i ON i.playlist_id=p.id
           WHERE p.user_id=? GROUP BY p.id ORDER BY p.created_at DESC""",
        (user["id"],)).fetchall()
    con.close()
    return [dict(r) for r in rows]


@app.post("/api/playlists")
def create_playlist(body: dict, user: dict = Depends(require_user)):
    title = (body.get("title") or "").strip()[:80] or "New playlist"
    con = db()
    cur = con.execute("INSERT INTO playlists (user_id, title) VALUES (?,?)",
                      (user["id"], title))
    con.commit()
    pid = cur.lastrowid
    con.close()
    return {"id": pid, "title": title}


@app.delete("/api/playlists/{pid}")
def delete_playlist(pid: int, user: dict = Depends(require_user)):
    con = db()
    r = con.execute("SELECT user_id FROM playlists WHERE id=?", (pid,)).fetchone()
    if not r:
        con.close()
        raise HTTPException(404, "Playlist not found")
    if r["user_id"] != user["id"]:
        con.close()
        raise HTTPException(403, "Not your playlist")
    con.execute("DELETE FROM playlists WHERE id=?", (pid,))
    con.commit()
    con.close()
    return {"ok": True}


@app.get("/api/playlists/{pid}")
def playlist_detail(pid: int, user: dict = Depends(require_user)):
    con = db()
    p = con.execute("SELECT * FROM playlists WHERE id=?", (pid,)).fetchone()
    if not p or p["user_id"] != user["id"]:
        con.close()
        raise HTTPException(404, "Playlist not found")
    items = con.execute(
        f"""{VIDEO_SELECT} JOIN playlist_items i ON i.video_id = v.id
            WHERE i.playlist_id=? AND v.visibility='published'
            ORDER BY i.position, i.added_at""", (pid,)).fetchall()
    con.close()
    d = dict(p)
    d["items"] = [_public_video_row(r, user["id"]) for r in items]
    return d


@app.post("/api/playlists/{pid}/items")
def playlist_add(pid: int, body: dict, user: dict = Depends(require_user)):
    video_id = body.get("video_id")
    con = db()
    p = con.execute("SELECT user_id FROM playlists WHERE id=?", (pid,)).fetchone()
    if not p or p["user_id"] != user["id"]:
        con.close()
        raise HTTPException(404, "Playlist not found")
    mx = con.execute("SELECT MAX(position) m FROM playlist_items WHERE playlist_id=?",
                     (pid,)).fetchone()["m"]
    try:
        con.execute(
            "INSERT INTO playlist_items (playlist_id, video_id, position) VALUES (?,?,?)",
            (pid, video_id, (mx or 0) + 1))
        con.commit()
    except sqlite3.IntegrityError:
        pass
    con.close()
    return {"ok": True}


@app.delete("/api/playlists/{pid}/items/{video_id}")
def playlist_remove(pid: int, video_id: int, user: dict = Depends(require_user)):
    con = db()
    p = con.execute("SELECT user_id FROM playlists WHERE id=?", (pid,)).fetchone()
    if not p or p["user_id"] != user["id"]:
        con.close()
        raise HTTPException(404, "Playlist not found")
    con.execute("DELETE FROM playlist_items WHERE playlist_id=? AND video_id=?",
                (pid, video_id))
    con.commit()
    con.close()
    return {"ok": True}


@app.post("/api/history")
def save_history(body: dict, user: dict = Depends(require_user)):
    video_id, position = body.get("video_id"), int(body.get("position", 0) or 0)
    con = db()
    con.execute(
        """INSERT INTO watch_history (user_id, video_id, position, updated_at)
           VALUES (?,?,?,datetime('now'))
           ON CONFLICT(user_id, video_id)
           DO UPDATE SET position=excluded.position, updated_at=datetime('now')""",
        (user["id"], video_id, max(0, position)))
    con.commit()
    con.close()
    return {"ok": True}


@app.get("/api/history")
def get_history(user: dict = Depends(require_user)):
    con = db()
    rows = con.execute(
        f"""{VIDEO_SELECT} JOIN watch_history h ON h.video_id = v.id
            WHERE h.user_id=? AND v.visibility='published'
            ORDER BY h.updated_at DESC LIMIT 50""", (user["id"],)).fetchall()
    con.close()
    items = []
    for r in rows:
        d = _public_video_row(r, user["id"])
        items.append(d)
    return items


# ============================================================ admin API

def _resolve_category(value: Optional[str]) -> int:
    con = db()
    if value:
        slug = re.sub(r"[^a-z0-9-]", "-", value.strip().lower())[:60].strip("-")
        r = con.execute("SELECT id FROM categories WHERE slug=?", (slug,)).fetchone()
        if not r:
            cur = con.execute("INSERT INTO categories (name, slug) VALUES (?,?)",
                              (value.strip()[:60] or slug, slug or "misc"))
            con.commit()
            cid = cur.lastrowid
        else:
            cid = r["id"]
    else:
        cid = con.execute("SELECT id FROM categories WHERE slug='uncategorized'").fetchone()["id"]
    con.close()
    return cid


@app.get("/api/admin/status")
async def admin_status(_: None = Depends(require_admin)):
    st = await tg.status()
    con = db()
    st["videos_total"] = con.execute("SELECT COUNT(*) c FROM videos").fetchone()["c"]
    st["videos_published"] = con.execute(
        "SELECT COUNT(*) c FROM videos WHERE visibility='published'").fetchone()["c"]
    st["users_total"] = con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
    con.close()
    return st


@app.get("/api/admin/videos")
def admin_videos(q: Optional[str] = None, _: None = Depends(require_admin)):
    con = db()
    sql = f"{VIDEO_SELECT}"
    params: list = []
    if q:
        sql += " WHERE v.title LIKE ?"
        params.append(f"%{q}%")
    sql += " ORDER BY v.id DESC LIMIT 200"
    rows = con.execute(sql, params).fetchall()
    con.close()
    return [_public_video_row(r) for r in rows]


@app.patch("/api/admin/videos/{video_id}")
def admin_edit(video_id: int, body: dict, _: None = Depends(require_admin)):
    fields, params = [], []
    if "title" in body:
        fields.append("title=?")
        params.append(body["title"][:200])
    if "description" in body:
        fields.append("description=?")
        params.append(body["description"][:3000])
    if "visibility" in body and body["visibility"] in ("published", "unpublished"):
        fields.append("visibility=?")
        params.append(body["visibility"])
        if body["visibility"] == "published":
            fields.append("published_at=COALESCE(published_at, datetime('now'))")
    if "category" in body:
        fields.append("category_id=?")
        params.append(_resolve_category(body["category"]))
    if not fields:
        raise HTTPException(400, "Nothing to update")
    con = db()
    con.execute(f"UPDATE videos SET {', '.join(fields)} WHERE id=?", params + [video_id])
    con.commit()
    con.close()
    return {"ok": True}


@app.delete("/api/admin/videos/{video_id}")
def admin_delete(video_id: int, _: None = Depends(require_admin)):
    """Remove the website listing only — the Telegram file is never touched."""
    con = db()
    r = con.execute("SELECT thumb_path FROM videos WHERE id=?", (video_id,)).fetchone()
    con.execute("DELETE FROM videos WHERE id=?", (video_id,))
    con.commit()
    con.close()
    if r and r["thumb_path"] and os.path.exists(r["thumb_path"]):
        try:
            os.remove(r["thumb_path"])
        except Exception:
            pass
    return {"ok": True, "note": "Listing removed. The file in your Telegram channel is untouched."}


@app.post("/api/admin/sync")
async def admin_sync(body: dict = None, _: None = Depends(require_admin)):
    limit = (body or {}).get("limit", 300)
    try:
        return await sync_channel(limit=int(limit))
    except Exception as e:
        raise HTTPException(502, f"Sync failed: {str(e)[:300]}")


# --- Telegram setup flow (all from the /admin page, no terminal needed) ---

def _raw_client():
    from telethon import TelegramClient
    from telethon.sessions import SQLiteSession
    return TelegramClient(SQLiteSession(str(SESSION_PATH)), int(TG_API_ID), TG_API_HASH,
                          proxy=TG_PROXY)


@app.post("/api/admin/tg-config")
def tg_config(body: dict, _: None = Depends(require_admin)):
    api_id = str(body.get("api_id") or "").strip()
    api_hash = (body.get("api_hash") or "").strip()
    if not api_id.isdigit() or len(api_hash) < 10:
        raise HTTPException(400, "api_id must be numeric and api_hash must look valid")
    TG_CONFIG_PATH.write_text(json.dumps({"api_id": api_id, "api_hash": api_hash}))
    os.chmod(TG_CONFIG_PATH, 0o600)
    global TG_API_ID, TG_API_HASH
    TG_API_ID, TG_API_HASH = api_id, api_hash
    return {"ok": True}


@app.post("/api/admin/tg-send-code")
async def tg_send_code(body: dict, _: None = Depends(require_admin)):
    phone = (body.get("phone") or "").strip().replace(" ", "")
    if len(phone) < 8:
        raise HTTPException(400, "Enter phone with country code, e.g. +9198XXXXXXXX")
    client = _raw_client()
    await client.connect()
    try:
        sent = await client.send_code_request(phone)
    except Exception as e:
        await client.disconnect()
        raise HTTPException(400, f"Could not send code: {str(e)[:200]}")
    p = DATA_DIR / "tg_login.json"
    p.write_text(json.dumps({"phone": phone, "phone_code_hash": sent.phone_code_hash}))
    os.chmod(p, 0o600)
    await client.disconnect()
    return {"ok": True, "note": "Code sent to your Telegram app"}


@app.post("/api/admin/tg-verify")
async def tg_verify(body: dict, _: None = Depends(require_admin)):
    from telethon.errors import SessionPasswordNeededError
    p = DATA_DIR / "tg_login.json"
    if not p.exists():
        raise HTTPException(400, "Send the code first")
    login = json.loads(p.read_text())
    code = (body.get("code") or "").strip()
    password = body.get("password") or ""
    client = _raw_client()
    await client.connect()
    try:
        if password and not code:
            await client.sign_in(password=password)
        else:
            await client.sign_in(login["phone"], code,
                                 phone_code_hash=login["phone_code_hash"])
        me = await client.get_me()
        p.unlink(missing_ok=True)
        await client.disconnect()
        # force manager to (re)connect with the new session
        tg._client = None
        return {"ok": True, "account": getattr(me, "username", None) or str(me.id)}
    except SessionPasswordNeededError:
        await client.disconnect()
        return {"ok": False, "need_password": True}
    except Exception as e:
        await client.disconnect()
        raise HTTPException(400, f"Login failed: {str(e)[:200]}")


@app.post("/api/admin/channel")
async def set_channel(body: dict, _: None = Depends(require_admin)):
    channel_input = (body.get("channel") or "").strip()
    if not channel_input:
        raise HTTPException(400, "Enter channel username, invite link or ID")
    try:
        client = await tg.client()
    except RuntimeError as e:
        raise HTTPException(400, f"Telegram login needed first: {e}")
    entity = None
    digits = re.sub(r"\D", "", channel_input)
    is_numeric = bool(digits) and channel_input.strip().lstrip("-+").isdigit()
    if is_numeric:
        # private channel pasted as bare ID ("3988054897") or full ("-1003988054897"):
        # find it in the account's dialogs (refreshes the entity cache too)
        bare = int(digits[-10:]) if len(digits) > 10 else int(digits)
        try:
            async for d in client.iter_dialogs(limit=100):
                if getattr(d.entity, "id", None) == bare:
                    entity = d.entity
                    break
        except Exception:
            entity = None
        if entity is None:
            raise HTTPException(
                400, "Cannot access that channel: not found in this account's chats")
    else:
        try:
            entity = await client.get_entity(channel_input)
        except Exception as e:
            raise HTTPException(400, f"Cannot access that channel: {str(e)[:200]}")
    tg.save_channel(entity.id)
    return {"ok": True, "channel_id": entity.id,
            "title": getattr(entity, "title", "")}


@app.get("/api/health")
def health():
    return {"ok": True, "time": datetime.now(timezone.utc).isoformat()}


@app.on_event("startup")
async def _startup():
    asyncio.create_task(_poller())


# serve the static frontend (../web) at / — mounted LAST so /api/* routes win
WEB_DIR = (BASE_DIR / ".." / "web").resolve()
if WEB_DIR.exists():
    app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")
