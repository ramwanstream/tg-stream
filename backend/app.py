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
UPLOAD_TMP_DIR = DATA_DIR / "upload_tmp"
UPLOAD_TMP_DIR.mkdir(exist_ok=True)

# v6 upload limits (env-overridable)
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_MB", "500")) * 1024 * 1024
ALLOWED_UPLOAD_MIME = {"video/mp4", "video/webm", "video/quicktime",
                       "video/x-matroska", "video/x-msvideo", "video/3gpp",
                       "video/x-flv", "application/octet-stream"}

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
# Telegram API credentials (official API, from https://my.telegram.org).
# Primary env names: TELEGRAM_APP_ID / TELEGRAM_APP_HASH
# (TG_API_ID / TG_API_HASH are kept as backwards-compatible aliases).
TG_API_ID = os.environ.get("TELEGRAM_APP_ID") or os.environ.get("TG_API_ID")
TG_API_HASH = os.environ.get("TELEGRAM_APP_HASH") or os.environ.get("TG_API_HASH")
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
    # v6: uploaded_by column (ALTER TABLE has no IF NOT EXISTS — check first)
    cols = [r["name"] for r in con.execute("PRAGMA table_info(videos)")]
    if "uploaded_by" not in cols:
        con.execute("ALTER TABLE videos ADD COLUMN uploaded_by INTEGER REFERENCES users(id)")
    # v7: user role + ban flag
    ucols = [r["name"] for r in con.execute("PRAGMA table_info(users)")]
    if "role" not in ucols:
        con.execute("ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'")
    if "banned" not in ucols:
        con.execute("ALTER TABLE users ADD COLUMN banned INTEGER NOT NULL DEFAULT 0")
    # v19 (security): token version — bumping it instantly revokes all of a
    # user's existing login tokens (password reset, ban).
    if "token_version" not in ucols:
        con.execute("ALTER TABLE users ADD COLUMN token_version INTEGER NOT NULL DEFAULT 0")
    # v8: comment replies
    ccols = [r["name"] for r in con.execute("PRAGMA table_info(comments)")]
    if "parent_id" not in ccols:
        con.execute("ALTER TABLE comments ADD COLUMN parent_id INTEGER")
    # v9: featured videos
    vcols = [r["name"] for r in con.execute("PRAGMA table_info(videos)")]
    if "featured" not in vcols:
        con.execute("ALTER TABLE videos ADD COLUMN featured INTEGER NOT NULL DEFAULT 0")
    # v16: many-to-many memberships (a video shows under every hashtag's
    # category); every video is at least a member of its primary category
    con.execute("""CREATE TABLE IF NOT EXISTS video_categories (
                     video_id INTEGER NOT NULL, category_id INTEGER NOT NULL,
                     PRIMARY KEY (video_id, category_id))""")
    # v20: one row per video already promoted to the public promo channel
    con.execute("""CREATE TABLE IF NOT EXISTS promo_posts (
                     video_id INTEGER PRIMARY KEY,
                     posted_at TEXT NOT NULL DEFAULT (datetime('now')))""")
    # v21: Telegram document id — the cross-channel file identity that lets
    # the site switch its source channel without duplicating the library
    vcols_now = [r[1] for r in con.execute("PRAGMA table_info(videos)").fetchall()]
    if "tg_doc_id" not in vcols_now:
        con.execute("ALTER TABLE videos ADD COLUMN tg_doc_id TEXT")
    con.execute("CREATE INDEX IF NOT EXISTS idx_videos_tg_doc_id ON videos(tg_doc_id)")
    con.execute("INSERT OR IGNORE INTO video_categories (video_id, category_id) "
                "SELECT id, category_id FROM videos WHERE category_id IS NOT NULL")
    con.commit()
    con.close()


def row_to_dict(r: sqlite3.Row) -> dict:
    return dict(r) if r is not None else None


# ------------------------------------------------------- simple rate limiter
# In-memory fixed-window limiter (per key). Guards brute force & upload abuse.
_RL: dict = {}

def rate_limit(key: str, limit: int, window_s: int) -> bool:
    """True if the action is allowed, False if over the limit."""
    now = time.time()
    bucket = _RL.get(key)
    if not bucket or now - bucket[0] >= window_s:
        _RL[key] = (now, 1)
        return True
    if bucket[1] >= limit:
        return False
    _RL[key] = (bucket[0], bucket[1] + 1)
    return True


# ---------------------------------------------------------------- auth

# OWASP-recommended cost for PBKDF2-HMAC-SHA256. The iteration count is
# stored inside the hash string so older hashes stay verifiable and can be
# upgraded transparently at login.
PBKDF2_ITERATIONS = 600_000
LEGACY_PBKDF2_ITERATIONS = 200_000


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), PBKDF2_ITERATIONS)
    return f"pbkdf2${PBKDF2_ITERATIONS}${salt}${dk.hex()}"


def check_password(password: str, stored: str) -> bool:
    try:
        parts = (stored or "").split("$")
        if len(parts) == 4:        # pbkdf2$<iters>$<salt>$<hex>
            _, iters, salt, hexdk = parts
            iterations = int(iters)
        elif len(parts) == 3:      # legacy pbkdf2$<salt>$<hex> (fixed 200k)
            _, salt, hexdk = parts
            iterations = LEGACY_PBKDF2_ITERATIONS
        else:
            return False
        dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), iterations)
        return hmac.compare_digest(dk.hex(), hexdk)
    except Exception:
        return False


def _hash_needs_upgrade(stored: str) -> bool:
    try:
        parts = (stored or "").split("$")
        return len(parts) != 4 or int(parts[1]) < PBKDF2_ITERATIONS
    except Exception:
        return True


def make_token(user_id: int, token_version: int = 0) -> str:
    payload = {
        "uid": user_id,
        "tv": token_version,
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
        r = con.execute("SELECT id, name, email, created_at, role, banned, token_version FROM users WHERE id=?",
                        (payload["uid"],)).fetchone()
        con.close()
        if r is None:
            return None
        # Tokens minted before token-versioning carry no "tv" (=0, matching
        # the column default), so existing sessions survive the upgrade.
        if int(payload.get("tv") or 0) != int(r["token_version"] or 0):
            return None
        return row_to_dict(r)
    except Exception:
        return None


def require_user(authorization: Optional[str] = Header(None)) -> dict:
    user = get_user_from_header(authorization)
    if not user:
        raise HTTPException(401, "Login required")
    if user.get("banned"):
        raise HTTPException(403, "This account has been suspended")
    return user


def optional_user(authorization: Optional[str] = Header(None)) -> Optional[dict]:
    u = get_user_from_header(authorization)
    if u and u.get("banned"):
        return None
    return u


# ------------------------------------------------------------- v7 helpers

def get_setting(key: str, default: str = "") -> str:
    try:
        con = db()
        r = con.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        con.close()
        return r["value"] if r else default
    except Exception:
        return default


def log_activity(action: str, target: str = "", detail: str = "", actor: str = "admin"):
    try:
        con = db()
        con.execute("INSERT INTO activity_log (actor, action, target, detail) VALUES (?,?,?,?)",
                    (actor, action, target[:120], detail[:300]))
        con.commit()
        con.close()
    except Exception:
        pass


def require_admin(x_admin_token: Optional[str] = Header(None)):
    if not x_admin_token or not hmac.compare_digest(x_admin_token, ADMIN_TOKEN):
        raise HTTPException(403, "Admin only")


# ---------------------------------------------------------------- app

app = FastAPI(title="TG Stream")

# Compress JSON API responses (faster page loads on slow networks)
from starlette.middleware.gzip import GZipMiddleware
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    # The only legitimate consumer is this site itself (same origin,
    # including inside the Telegram Mini App webview) — no wildcard.
    allow_origins=["https://ramwanstream.alwaysdata.net"],
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "X-Admin-Token"],
)


@app.middleware("http")
async def secure_headers(request: Request, call_next):
    """Hardened response headers (clickjacking, MIME sniffing, XSS)."""
    resp = await call_next(request)
    resp.headers["X-Content-Type-Options"] = "nosniff"
    # Framing: the site doubles as a Telegram Mini App, so Telegram's web
    # client must be allowed to embed it. X-Frame-Options has no allowlist,
    # so it is replaced by CSP frame-ancestors (self + Telegram origins);
    # every other origin stays blocked against clickjacking.
    resp.headers["Content-Security-Policy"] = (
        "frame-ancestors 'self' https://web.telegram.org https://*.telegram.org")
    resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    # HTTPS is forced at the host; pin browsers to it for a year so a
    # downgraded/plain-HTTP visit can never happen after the first load.
    resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    # The site never needs these device APIs (fullscreen / picture-in-picture
    # for the player are not gated by Permissions-Policy defaults).
    resp.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=(), payment=()"
    return resp


_MAINTENANCE_HTML = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Maintenance</title><style>body{background:#07070d;color:#f2f2f7;font-family:system-ui;
display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0;text-align:center}
.box{padding:24px}.big{font-size:52px}h1{font-size:22px}p{color:#9a9aad}</style></head>
<body><div class="box"><div class="big">🛠️</div><h1>We are upgrading the site</h1>
<p>StreamHub is under maintenance. Please check back in a little while.</p></div></body></html>"""


@app.middleware("http")
async def maintenance_gate(request: Request, call_next):
    """v9: when maintenance_mode is ON, only the admin can use the site."""
    path = request.url.path
    exempt = (path.startswith("/api/admin") or path == "/admin.html"
              or path.startswith("/css/") or path.startswith("/js/")
              or path == "/favicon.ico")
    if not exempt:
        try:
            if get_setting("maintenance_mode", "0") == "1":
                tok = request.headers.get("x-admin-token") or request.query_params.get("at")
                if not (tok and ADMIN_TOKEN and hmac.compare_digest(tok, ADMIN_TOKEN)):
                    from fastapi.responses import HTMLResponse, JSONResponse
                    if path.startswith("/api/"):
                        return JSONResponse({"detail": "Site is under maintenance"}, status_code=503)
                    return HTMLResponse(_MAINTENANCE_HTML, status_code=503)
        except Exception:
            pass
    return await call_next(request)

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

    # Short-lived cache of Telegram document references, keyed by message id.
    # A video player fires many Range requests back-to-back; without this each
    # one would cost an extra get_messages round-trip before downloading.
    _doc_cache: dict = {}

    def __init__(self, manager: "TelegramManager", channel_id: int, message_id: int,
                 file_size: int, mime: str):
        self.mgr = manager
        self.channel_id = channel_id
        self.message_id = message_id
        self._size = file_size
        self.mime = mime or "video/mp4"

    async def size(self) -> int:
        return self._size

    async def _get_doc(self, client):
        """Fresh document reference, cached ~90s. File references can expire;
        on expiry the caller drops the cache entry and retries once."""
        now = time.time()
        # v21: the video's OWN channel — listings can come from different
        # channels now (content source vs upload channel), and the doc
        # cache key must include the channel for the same reason.
        key = (self.channel_id, self.message_id)
        hit = self._doc_cache.get(key)
        if hit and hit[1] > now:
            return hit[0]
        # Re-fetch the message: Telegram file references expire,
        # a fresh fetch always gives a valid one.
        msg = await client.get_messages(self.channel_id, ids=self.message_id)
        if not msg or not msg.media:
            raise FileNotFoundError("Telegram message/file no longer available")
        doc = getattr(msg.media, "document", None)
        if doc is None:
            raise FileNotFoundError("No downloadable file in this message")
        self._doc_cache[key] = (doc, now + 90)
        return doc

    async def read_range(self, start: int, end: int) -> AsyncIterator[bytes]:
        from telethon.errors import FileReferenceExpiredError
        client = await self.mgr.client()
        try:
            doc = await self._get_doc(client)
        except FileNotFoundError:
            raise
        length = end - start + 1
        # NOTE: iter_download's `limit` counts CHUNKS, not bytes.
        # Compute how many chunks cover `length` bytes, then trim the tail
        # so we yield exactly [start, end] — never more, never buffered whole.
        request_size = min(CHUNK_SIZE, length)
        limit = (length + request_size - 1) // request_size + 1
        remaining = length
        try:
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
        except FileReferenceExpiredError:
            # Cached reference went stale mid-stream: drop it, re-fetch once.
            self._doc_cache.pop(self.message_id, None)
            doc = await self._get_doc(client)
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
        # v21: user uploads go to their OWN channel (kept separate from the
        # content source channel); falls back to channel_id when unset.
        self.upload_channel_id: Optional[int] = None
        self.last_error: Optional[str] = None
        self.last_sync: Optional[str] = None
        self._load_channel()

    def _load_channel(self):
        p = DATA_DIR / "channel.json"
        if p.exists():
            try:
                data = json.loads(p.read_text())
                self.channel_id = data.get("channel_id")
                self.upload_channel_id = data.get("upload_channel_id")
            except Exception:
                pass

    def save_channel(self, channel_id: int, upload_channel_id="keep"):
        self.channel_id = channel_id
        if upload_channel_id != "keep":
            self.upload_channel_id = upload_channel_id
        p = DATA_DIR / "channel.json"
        data = {"channel_id": channel_id}
        if self.upload_channel_id:
            data["upload_channel_id"] = self.upload_channel_id
        p.write_text(json.dumps(data))
        os.chmod(p, 0o600)

    def effective_upload_channel(self) -> Optional[int]:
        return self.upload_channel_id or self.channel_id

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
                # Instant import: when anything lands in the linked channel,
                # sync it to the website within seconds (poller stays as backup).
                if not getattr(self._client, "_sh_push_attached", False):
                    from telethon import events

                    async def _on_channel_post(event):
                        try:
                            if self.channel_id and event.chat_id == self.channel_id:
                                # debounce: at most one push-sync per 10s
                                now = time.time()
                                if now - getattr(self, "_last_push", 0) < 10:
                                    return
                                self._last_push = now
                                await sync_channel(limit=10)
                        except Exception:
                            pass

                    self._client.add_event_handler(
                        _on_channel_post, events.NewMessage())
                    self._client._sh_push_attached = True
            if not await self._client.is_user_authorized():
                raise RuntimeError("Telegram session not logged in (see /admin setup)")
            return self._client

    async def status(self) -> dict:
        st = {"configured": self.configured(), "channel_id": self.channel_id,
              "upload_channel_id": self.upload_channel_id,
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
    if not video.get("tg_channel_id") or not video.get("tg_message_id"):
        raise HTTPException(503, "Video source not configured yet")
    return TelegramSource(tg, video["tg_channel_id"], video["tg_message_id"],
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


_HASHTAG_RE = re.compile(r"#([\w]+)", re.UNICODE)


def _nice_cat_name(tag: str) -> str:
    """#travel_vlog -> 'Travel Vlog'; #4K / #HD stay as typed."""
    out = []
    for w in [w for w in tag.replace("_", " ").split() if w]:
        out.append(w if (w.isupper() or not w.islower()) else w.capitalize())
    return " ".join(out)[:60] or "Uncategorized"


def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9-]", "-", name.lower())[:60].strip("-") or "cat"


def _category_id_for_tag(con, tag: str) -> int:
    """Get-or-create the category for a hashtag (same slug rule as admin)."""
    name = _nice_cat_name(tag)
    slug = _slugify(name)
    row = con.execute("SELECT id FROM categories WHERE slug=?", (slug,)).fetchone()
    if row:
        return row["id"]
    return con.execute(
        "INSERT INTO categories (name, slug) VALUES (?,?)", (name, slug)).lastrowid


def _sync_memberships(con, video_id: int, tags) -> None:
    """A video is a member of every hashtag's category (+ its primary)."""
    ids = {_category_id_for_tag(con, t) for t in tags}
    row = con.execute("SELECT category_id FROM videos WHERE id=?", (video_id,)).fetchone()
    if row and row["category_id"]:
        ids.add(row["category_id"])
    for cid in ids:
        con.execute("INSERT OR IGNORE INTO video_categories (video_id, category_id) "
                    "VALUES (?,?)", (video_id, cid))


def _parse_caption(text: str):
    """Channel caption -> (title, hashtags).

    The caption's first text line becomes the video title (hashtags stripped
    out of it); hashtags anywhere in the caption drive the category.
    """
    raw = (text or "").strip()
    tags = _HASHTAG_RE.findall(raw)
    title = ""
    for line in raw.split("\n"):
        line = re.sub(r"\s+", " ", _HASHTAG_RE.sub("", line)).strip(" \t-\u2013\u2014|\u2022\u00b7")
        if line:
            title = line[:120]
            break
    return title, tags


def _ingest_one(con, channel_id: int, msg, *, allow_uploads: bool) -> str:
    """Import a single channel message into the library.

    Returns "added", "skipped" (already listed) or "ignored" (not a video).
    The caller owns the transaction (commit). v21: a listing's identity is
    BOTH channel:message AND the Telegram document id — the same file
    arriving from a different channel never creates a second listing.
    """
    attrs = _video_attrs(msg)
    if not attrs:
        return "ignored"
    doc_id = attrs["unique_id"]  # Telegram document id = the file itself
    # stable identity: a message's media never changes
    attrs["unique_id"] = f"{channel_id}:{msg.id}"
    exists = con.execute(
        "SELECT id, duration, file_size, mime, tg_doc_id FROM videos WHERE tg_file_unique_id=?",
        (attrs["unique_id"],)).fetchone()
    if exists:
        if not exists["tg_doc_id"] and doc_id:
            con.execute("UPDATE videos SET tg_doc_id=? WHERE id=?",
                        (doc_id, exists["id"]))
        # backfill: rows first imported without duration (e.g. sent as a
        # plain document) get their real length once Telegram reports it
        if (not exists["duration"]) and attrs["duration"]:
            con.execute(
                "UPDATE videos SET duration=?, file_size=?, mime=? WHERE id=?",
                (attrs["duration"], attrs["file_size"], attrs["mime"], exists["id"]))
        # caption rules for already-imported rows: hashtag category
        # applies while a video is still Uncategorized, and the title
        # is cleaned only when it still equals the raw first line the
        # old logic stored (an admin-edited title is never overwritten)
        cap_title, cap_tags = _parse_caption(msg.text or "")
        if cap_title or cap_tags:
            row = con.execute(
                "SELECT v.title, c.slug AS cat_slug FROM videos v "
                "LEFT JOIN categories c ON c.id = v.category_id WHERE v.id=?",
                (exists["id"],)).fetchone()
            if cap_tags and row["cat_slug"] in (None, "uncategorized"):
                con.execute("UPDATE videos SET category_id=? WHERE id=?",
                            (_category_id_for_tag(con, cap_tags[0]), exists["id"]))
            old_style = (msg.text or "").strip().split("\n")[0][:120]
            if cap_title and row["title"] == old_style and cap_title != row["title"]:
                con.execute("UPDATE videos SET title=? WHERE id=?",
                            (cap_title, exists["id"]))
            _sync_memberships(con, exists["id"], cap_tags)
        return "skipped"
    # v21: the same file is already listed from another channel (e.g. the
    # previous source channel) — never create a duplicate listing for it
    if doc_id:
        twin = con.execute("SELECT id FROM videos WHERE tg_doc_id=?",
                           (doc_id,)).fetchone()
        if twin:
            return "skipped"
    cap_title, cap_tags = _parse_caption(msg.text or "")
    title = cap_title or attrs["file_name"] or "Untitled video"
    # v6: if this message is a user upload (website -> channel), link it
    # to that user and hold it in the moderation queue (unpublished).
    pu = None
    if allow_uploads:
        pu = con.execute(
            "SELECT id, user_id, title, description, category_id FROM pending_uploads "
            "WHERE tg_message_id=? AND status='sent'", (msg.id,)).fetchone()
    if pu:
        # moderation ON  -> hold as unpublished for admin review
        # moderation OFF -> publish immediately
        moderate = get_setting("moderation_enabled", "1") == "1"
        vis = "unpublished" if moderate else "published"
        pub_at = "NULL" if moderate else "datetime('now')"
        con.execute(
            f"""INSERT INTO videos
               (title, description, category_id, tg_channel_id, tg_message_id,
                tg_file_unique_id, tg_doc_id, file_size, duration, mime, visibility,
                published_at, uploaded_by)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{vis}', {pub_at}, ?)""",
            (pu["title"] or title, pu["description"] or "",
             pu["category_id"] or None, channel_id,
             msg.id, attrs["unique_id"], doc_id, attrs["file_size"],
             attrs["duration"], attrs["mime"], pu["user_id"]),
        )
        con.execute("UPDATE pending_uploads SET status='done' WHERE id=?", (pu["id"],))
        # fall back to Uncategorized if the upload had no category
        con.execute(
            "UPDATE videos SET category_id=(SELECT id FROM categories WHERE slug='uncategorized') "
            "WHERE id IN (SELECT id FROM videos WHERE tg_message_id=?) AND category_id IS NULL",
            (msg.id,))
        vrow = con.execute("SELECT id FROM videos WHERE tg_file_unique_id=?",
                           (attrs["unique_id"],)).fetchone()
        if vrow:
            _sync_memberships(con, vrow["id"], cap_tags)
    else:
        cat_id = _category_id_for_tag(con, cap_tags[0]) if cap_tags else None
        cur = con.execute(
            """INSERT INTO videos
               (title, description, category_id, tg_channel_id, tg_message_id,
                tg_file_unique_id, tg_doc_id, file_size, duration, mime, visibility,
                published_at)
               VALUES (?, ?, COALESCE(?, (SELECT id FROM categories WHERE slug='uncategorized')),
                       ?, ?, ?, ?, ?, ?, ?, 'published', datetime('now'))""",
            (title, (msg.text or "").strip()[:2000], cat_id, channel_id,
             msg.id, attrs["unique_id"], doc_id, attrs["file_size"],
             attrs["duration"], attrs["mime"]),
        )
        _sync_memberships(con, cur.lastrowid, cap_tags)
    return "added"


async def sync_channel(limit: int = 300, channel_id: int = None) -> dict:
    """Import new videos from a channel (default: the content source).

    v21: the content source (@ph_vid) and the user-upload channel are
    separate — pending-upload matching only happens when syncing the
    channel uploads actually go to.
    """
    client = await tg.client()
    chan = channel_id or tg.channel_id
    if not chan:
        raise RuntimeError("Channel not set (see /admin setup)")
    allow_uploads = chan == tg.effective_upload_channel()
    added, skipped = 0, 0
    con = db()
    try:
        async for msg in client.iter_messages(chan, limit=limit):
            r = _ingest_one(con, chan, msg, allow_uploads=allow_uploads)
            if r == "added":
                added += 1
            elif r == "skipped":
                skipped += 1
        con.commit()
    finally:
        con.close()
    tg.last_sync = datetime.now(timezone.utc).isoformat()
    # thumbnails for the newly added ones (best-effort, background)
    asyncio.create_task(_fetch_thumbs())
    return {"added": added, "skipped": skipped}


async def _sync_pending_uploads() -> dict:
    """Import website uploads waiting in the upload channel (v21).

    Fetches exactly the messages the upload flow marked 'sent', so the
    upload channel can hold anything else without it reaching the site.
    """
    chan = tg.effective_upload_channel()
    if not chan:
        return {"added": 0}
    con = db()
    try:
        rows = con.execute(
            "SELECT tg_message_id FROM pending_uploads "
            "WHERE status='sent' AND tg_message_id IS NOT NULL").fetchall()
    finally:
        con.close()
    if not rows:
        return {"added": 0}
    client = await tg.client()
    msgs = await client.get_messages(chan, ids=[r["tg_message_id"] for r in rows])
    if not isinstance(msgs, list):
        msgs = [msgs]
    added = 0
    con = db()
    try:
        for msg in msgs:
            if msg and _ingest_one(con, chan, msg, allow_uploads=True) == "added":
                added += 1
        con.commit()
    finally:
        con.close()
    if added:
        asyncio.create_task(_fetch_thumbs())
    return {"added": added}


_thumbs_busy = False
thumb_last_error = None


async def _fetch_thumbs(limit: int = 40):
    """Download Telegram video thumbs for videos missing one.

    v18: newest first (the homepage fills in first), single-flight (sync
    triggers and the background worker never run two passes at once), and
    videos whose channel message has no downloadable thumb get a ''
    sentinel so they stop consuming batch slots on every pass.
    """
    global _thumbs_busy, thumb_last_error
    if _thumbs_busy:
        return
    _thumbs_busy = True
    try:
        client = await tg.client()
        con = db()
        rows = con.execute(
            "SELECT id, tg_message_id, tg_channel_id FROM videos WHERE thumb_path IS NULL "
            "AND tg_message_id IS NOT NULL ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        for r in rows:
            try:
                # v21: fetch from the video's OWN channel (listings are
                # multi-channel now — source channel + upload channel)
                msg = await client.get_messages(r["tg_channel_id"] or tg.channel_id,
                                                ids=r["tg_message_id"])
                if not msg or not msg.media:
                    con.execute("UPDATE videos SET thumb_path='' WHERE id=?", (r["id"],))
                    con.commit()
                    continue
                dest = str(THUMB_DIR / f"{r['id']}.jpg")
                got = await client.download_media(msg, file=dest, thumb=-1)
                if got:
                    con.execute("UPDATE videos SET thumb_path=? WHERE id=?", (dest, r["id"]))
                else:
                    con.execute("UPDATE videos SET thumb_path='' WHERE id=?", (r["id"],))
                con.commit()
            except Exception as e:
                thumb_last_error = f"{type(e).__name__}: {str(e)[:160]}"
                if type(e).__name__ == "FloodWaitError":
                    break  # let the worker retry after the cooldown
                continue
            await asyncio.sleep(0.15)
        con.close()
    except Exception:
        pass
    finally:
        _thumbs_busy = False


async def _thumb_worker():
    """Background: keep draining the thumbnail backlog between syncs."""
    await asyncio.sleep(45)  # let startup + first sync settle
    while True:
        try:
            if tg.configured() and tg.channel_id:
                await _fetch_thumbs(limit=40)
        except Exception:
            pass
        await asyncio.sleep(45)


async def _poller():
    """Background: re-check the channel every 5 minutes for new videos."""
    await asyncio.sleep(20)  # let startup finish
    while True:
        try:
            if tg.configured() and tg.channel_id:
                await sync_channel(limit=100)
                # v21: website uploads wait in their own channel — import
                # exactly the messages the upload flow marked 'sent'
                await _sync_pending_uploads()
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
        "visibility": v.get("visibility", "published"),
        "uploaded_by": v.get("uploaded_by"),
        "published_at": v["published_at"],
        "thumb_url": f"/api/thumb/{v['id']}" if v["thumb_path"] else None,
        "stream_url": f"/api/stream/{v['id']}",
        "like_count": v.get("like_count", 0) or 0,
        "comment_count": v.get("comment_count", 0) or 0,
        "featured": bool(v.get("featured")),
    }
    out["liked"] = False
    out["resume_position"] = 0
    if user_id:
        con = db()
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

# Same as VIDEO_SELECT but with like/comment counts baked in — avoids 2 extra
# queries per video on listing pages (was ~100 queries for a 24-video page).
VIDEO_SELECT_COUNTS = """SELECT v.*, c.name AS cat_name, c.slug AS cat_slug,
                  (SELECT COUNT(*) FROM likes l WHERE l.video_id = v.id) AS like_count,
                  (SELECT COUNT(*) FROM comments cm WHERE cm.video_id = v.id) AS comment_count
                  FROM videos v LEFT JOIN categories c ON c.id = v.category_id"""


@app.get("/api/videos")
def list_videos(q: Optional[str] = None, category: Optional[str] = None,
                page: int = 1, per_page: int = 24, sort: str = "new",
                featured: int = 0,
                user: Optional[dict] = Depends(optional_user)):
    per_page = max(1, min(per_page, 60))
    page = max(1, page)
    where = ["v.visibility='published'"]
    params: list = []
    if featured:
        where.append("v.featured=1")
    if q:
        where.append("(v.title LIKE ? OR v.description LIKE ?)")
        params += [f"%{q}%", f"%{q}%"]
    if category and category != "all":
        where.append("""v.id IN (SELECT vc.video_id FROM video_categories vc
                        JOIN categories cc ON cc.id = vc.category_id
                        WHERE cc.slug = ?)""")
        params.append(category)
    if sort == "popular":
        order = "v.view_count DESC, v.id DESC"
        join = ""
    elif sort == "trending":
        # most viewed in the last 7 days
        order = "COALESCE(w.recent, 0) DESC, v.view_count DESC"
        join = (" LEFT JOIN (SELECT video_id, COUNT(*) AS recent FROM view_events"
                " WHERE viewed_at > datetime('now', '-7 days') GROUP BY video_id) w"
                " ON w.video_id = v.id")
    else:
        order = "v.published_at DESC NULLS LAST, v.id DESC"
        join = ""
    sql = f"{VIDEO_SELECT_COUNTS}{join} WHERE {' AND '.join(where)} ORDER BY {order}"
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
def video_detail(video_id: int, user: Optional[dict] = Depends(optional_user),
                at: Optional[str] = None):
    con = db()
    r = con.execute(f"{VIDEO_SELECT_COUNTS} WHERE v.id=?", (video_id,)).fetchone()
    con.close()
    if not r:
        raise HTTPException(404, "Video not found")
    v = row_to_dict(r)
    uid = user["id"] if user else None
    # unpublished videos are visible to their uploader or the admin only
    if v["visibility"] != "published":
        is_admin = bool(ADMIN_TOKEN) and hmac.compare_digest(at or "", ADMIN_TOKEN)
        if not (is_admin or (uid and v.get("uploaded_by") == uid)):
            raise HTTPException(404, "Video not found")
    detail = _public_video_row(r, uid)
    # related: same category first, then latest
    con = db()
    rel = con.execute(
        f"""{VIDEO_SELECT_COUNTS} WHERE v.visibility='published' AND v.id != ?
            ORDER BY (c.slug = ?) DESC, v.published_at DESC LIMIT 8""",
        (video_id, detail["category_slug"])).fetchall()
    con.close()
    detail["related"] = [_public_video_row(x, uid) for x in rel]
    return detail


@app.get("/api/categories")
def list_categories():
    con = db()
    rows = con.execute(
        """SELECT c.id, c.slug, c.name, COUNT(v.id) AS count FROM categories c
           LEFT JOIN video_categories vc ON vc.category_id=c.id
           LEFT JOIN videos v ON v.id=vc.video_id AND v.visibility='published'
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
            # Thumbnails never change once fetched: cache aggressively in
            # browsers/CDN so grids load instantly on repeat visits.
            return FileResponse(str(p), media_type="image/jpeg", headers={
                "Cache-Control": "public, max-age=86400, immutable"})
    raise HTTPException(404, "No thumbnail")


_view_seen: dict = {}


@app.post("/api/videos/{video_id}/view")
def register_view(video_id: int, request: Request):
    key = (request.client.host if request.client else "?", video_id)
    now = time.time()
    if _view_seen.get(key, 0) > now - VIEW_DEDUP_SECONDS:
        return {"counted": False}
    _view_seen[key] = now
    ip = request.client.host if request.client else "?"
    iph = hashlib.sha256(ip.encode()).hexdigest()[:16]
    con = db()
    con.execute("UPDATE videos SET view_count = view_count + 1 WHERE id=?", (video_id,))
    con.execute("INSERT INTO view_events (video_id, ip_hash) VALUES (?, ?)", (video_id, iph))
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
async def stream_video(video_id: int, request: Request,
                     at: Optional[str] = None,
                     user: Optional[dict] = Depends(optional_user)):
    con = db()
    r = con.execute("SELECT * FROM videos WHERE id=?", (video_id,)).fetchone()
    con.close()
    if not r:
        raise HTTPException(404, "Video not found")
    v = row_to_dict(r)
    if v["visibility"] != "published":
        is_admin = bool(ADMIN_TOKEN) and hmac.compare_digest(at or "", ADMIN_TOKEN)
        uid = user["id"] if user else None
        if not (is_admin or (uid and v.get("uploaded_by") == uid)):
            raise HTTPException(404, "Video not found")
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
def signup(body: dict, request: Request):
    ip = (request.client.host if request.client else "?")
    if not rate_limit(f"signup:{ip}", 10, 3600):
        raise HTTPException(429, "Too many signups from this address, try later")
    name = (body.get("name") or "").strip()[:60]
    email = (body.get("email") or "").strip().lower()[:120]
    password = body.get("password") or ""
    if not name or "@" not in email or len(password) < 10:
        raise HTTPException(400, "Name, valid email and 10+ character password required")
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
def login(body: dict, request: Request):
    email = (body.get("email") or "").strip().lower()
    ip = (request.client.host if request.client else "?")
    if not rate_limit(f"login:{ip}:{email}", 15, 600):
        raise HTTPException(429, "Too many login attempts, wait a few minutes")
    # Global per-account bucket: slows password guessing even when the
    # attacker rotates IPs (per-IP limits alone miss that).
    if not rate_limit(f"loginmail:{email}", 40, 3600):
        raise HTTPException(429, "Too many login attempts for this account, try later")
    password = body.get("password") or ""
    con = db()
    r = con.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
    if not r or not check_password(password, r["password_hash"]):
        con.close()
        raise HTTPException(401, "Wrong email or password")
    if r["banned"]:
        con.close()
        raise HTTPException(403, "This account has been suspended")
    # Transparently upgrade legacy/weak password hashes to the current cost.
    if _hash_needs_upgrade(r["password_hash"]):
        con.execute("UPDATE users SET password_hash=? WHERE id=?",
                    (hash_password(password), r["id"]))
        con.commit()
    con.close()
    u = row_to_dict(r)
    return {"token": make_token(u["id"], int(u.get("token_version") or 0)),
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


@app.get("/api/videos/{video_id}/comments")
def list_comments(video_id: int, page: int = 1, per_page: int = 30,
                user: Optional[dict] = Depends(optional_user)):
    """Threaded comments: top-level newest-first, replies oldest-first."""
    per_page = max(1, min(per_page, 50))
    page = max(1, page)
    con = db()
    total = con.execute("SELECT COUNT(*) c FROM comments WHERE video_id=?",
                        (video_id,)).fetchone()["c"]
    tops = con.execute(
        """SELECT c.id, c.body, c.created_at, c.user_id, u.name AS user_name
           FROM comments c JOIN users u ON u.id = c.user_id
           WHERE c.video_id=? AND c.parent_id IS NULL
           ORDER BY c.created_at DESC LIMIT ? OFFSET ?""",
        (video_id, per_page, (page - 1) * per_page)).fetchall()
    items = []
    for r in tops:
        reps = con.execute(
            """SELECT c.id, c.body, c.created_at, c.user_id, u.name AS user_name
               FROM comments c JOIN users u ON u.id = c.user_id
               WHERE c.parent_id=? ORDER BY c.created_at ASC""",
            (r["id"],)).fetchall()
        def fmt(x):
            return {"id": x["id"], "body": x["body"], "user_name": x["user_name"],
                    "user_id": x["user_id"], "created_at": x["created_at"],
                    "mine": bool(user and x["user_id"] == user["id"])}
        item = fmt(r)
        item["replies"] = [fmt(x) for x in reps]
        items.append(item)
    con.close()
    return {"total": total, "items": items}


@app.post("/api/videos/{video_id}/comments")
async def add_comment(video_id: int, request: Request,
                      user: dict = Depends(require_user)):
    if not rate_limit(f"comment:{user['id']}", 20, 600):
        raise HTTPException(429, "You are commenting too fast")
    data = await request.json()
    body = (data.get("body") or "").strip()[:1000]
    if not body:
        raise HTTPException(400, "Comment is empty")
    parent_id = data.get("parent_id")
    con = db()
    if not con.execute("SELECT id FROM videos WHERE id=? AND visibility='published'",
                       (video_id,)).fetchone():
        con.close()
        raise HTTPException(404, "Video not found")
    if parent_id is not None:
        parent = con.execute("SELECT id, parent_id FROM comments WHERE id=? AND video_id=?",
                             (parent_id, video_id)).fetchone()
        if not parent:
            con.close()
            raise HTTPException(400, "Parent comment not found")
        # one level of nesting only: replies attach to the top-level comment
        if parent["parent_id"]:
            parent_id = parent["parent_id"]
    cur = con.execute("INSERT INTO comments (video_id, user_id, body, parent_id) VALUES (?,?,?,?)",
                      (video_id, user["id"], body, parent_id))
    cid = cur.lastrowid
    con.commit()
    r = con.execute(
        "SELECT c.id, c.body, c.created_at, c.parent_id, c.user_id, u.name AS user_name "
        "FROM comments c JOIN users u ON u.id=c.user_id WHERE c.id=?", (cid,)).fetchone()
    con.close()
    return {"id": r["id"], "body": r["body"], "user_name": r["user_name"],
            "user_id": r["user_id"], "parent_id": r["parent_id"],
            "created_at": r["created_at"], "mine": True}


@app.delete("/api/comments/{comment_id}")
def delete_my_comment(comment_id: int, user: dict = Depends(require_user)):
    """Delete your own comment (and its replies)."""
    con = db()
    c = con.execute("SELECT user_id FROM comments WHERE id=?", (comment_id,)).fetchone()
    if not c:
        con.close()
        raise HTTPException(404, "Comment not found")
    if c["user_id"] != user["id"]:
        con.close()
        raise HTTPException(403, "You can only delete your own comments")
    con.execute("DELETE FROM comments WHERE parent_id=?", (comment_id,))
    con.execute("DELETE FROM comments WHERE id=?", (comment_id,))
    con.commit()
    con.close()
    return {"ok": True}


@app.post("/api/reports")
async def create_report(request: Request, user: dict = Depends(require_user)):
    """Flag a video or a comment for the admin."""
    if not rate_limit(f"report:{user['id']}", 10, 3600):
        raise HTTPException(429, "Too many reports")
    data = await request.json()
    video_id = data.get("video_id")
    comment_id = data.get("comment_id")
    reason = (data.get("reason") or "").strip()[:300]
    if not video_id and not comment_id:
        raise HTTPException(400, "Nothing to report")
    con = db()
    if comment_id:
        cm = con.execute("SELECT video_id FROM comments WHERE id=?", (comment_id,)).fetchone()
        if not cm:
            con.close()
            raise HTTPException(404, "Comment not found")
        video_id = cm["video_id"]
    con.execute("INSERT INTO reports (reporter_id, video_id, comment_id, reason) VALUES (?,?,?,?)",
                (user["id"], video_id, comment_id, reason))
    con.commit()
    con.close()
    return {"ok": True, "message": "Reported. The admin will review it."}


@app.get("/api/likes")
def my_likes(user: dict = Depends(require_user)):
    con = db()
    rows = con.execute(
        f"""{VIDEO_SELECT_COUNTS} JOIN likes l ON l.video_id = v.id
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
        f"""{VIDEO_SELECT_COUNTS} JOIN playlist_items i ON i.video_id = v.id
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
        f"""{VIDEO_SELECT_COUNTS} JOIN watch_history h ON h.video_id = v.id
            WHERE h.user_id=? AND v.visibility='published'
            ORDER BY h.updated_at DESC LIMIT 50""", (user["id"],)).fetchall()
    con.close()
    items = []
    for r in rows:
        d = _public_video_row(r, user["id"])
        items.append(d)
    return items


# ============================================================ user uploads (v6)

async def _send_upload_to_telegram(upload_id: int):
    """Background: forward a saved upload file to the Telegram channel.

    Marks the pending_uploads row 'sent' with the Telegram message id; the
    channel sync then imports it as an unpublished (moderation) video.
    """
    con = db()
    row = con.execute("SELECT * FROM pending_uploads WHERE id=?", (upload_id,)).fetchone()
    con.close()
    if not row:
        return
    try:
        client = await tg.client()
        # v21: uploads go to the dedicated upload channel, NOT the source
        up_channel = tg.effective_upload_channel()
        if not up_channel:
            raise RuntimeError("Channel not set")
        con = db()
        con.execute("UPDATE pending_uploads SET status='sending' WHERE id=?", (upload_id,))
        con.commit()
        con.close()
        caption = row["title"]
        msg = await client.send_file(
            up_channel, row["temp_path"],
            caption=caption, supports_streaming=True,
            attributes=[])
        con = db()
        con.execute("UPDATE pending_uploads SET status='sent', tg_message_id=? WHERE id=?",
                    (msg.id, upload_id))
        con.commit()
        con.close()
        # nudge the import so the video appears quickly
        try:
            await _sync_pending_uploads()
        except Exception:
            pass
    except Exception as e:
        con = db()
        con.execute("UPDATE pending_uploads SET status='failed', error=? WHERE id=?",
                    (str(e)[:300], upload_id))
        con.commit()
        con.close()


@app.post("/api/upload")
async def upload_video(request: Request, user: dict = Depends(require_user)):
    """YouTube-style upload: video file + title -> Telegram channel (moderated)."""
    if get_setting("uploads_enabled", "1") != "1":
        raise HTTPException(403, "Uploads are currently disabled")
    if not tg.configured() or not tg.effective_upload_channel():
        raise HTTPException(503, "Uploads are not available right now")
    if not rate_limit(f"upload:{user['id']}", 5, 3600):
        raise HTTPException(429, "Upload limit reached (5/hour). Try again later.")
    form = await request.form()
    file = form.get("file")
    title = (form.get("title") or "").strip()[:120]
    description = (form.get("description") or "").strip()[:2000]
    category = (form.get("category") or "uncategorized").strip()
    if file is None or not getattr(file, "filename", ""):
        raise HTTPException(400, "No video file selected")
    if not title:
        raise HTTPException(400, "Title is required")
    mime = (file.content_type or "application/octet-stream").lower()
    ext = Path(file.filename).suffix.lower()
    if mime not in ALLOWED_UPLOAD_MIME and ext not in (".mp4", ".webm", ".mov", ".mkv", ".avi", ".3gp", ".flv"):
        raise HTTPException(400, "Only video files allowed (mp4, webm, mov, mkv)")
    con = db()
    cat = con.execute("SELECT id FROM categories WHERE slug=?", (category,)).fetchone()
    category_id = cat["id"] if cat else None
    # Stream the upload to a temp file (never buffer a big file in RAM).
    tmp = UPLOAD_TMP_DIR / f"u{user['id']}_{int(time.time())}_{secrets.token_hex(4)}{ext or '.mp4'}"
    size = 0
    try:
        with open(tmp, "wb") as fh:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                size += len(chunk)
                if size > MAX_UPLOAD_BYTES:
                    fh.close()
                    tmp.unlink(missing_ok=True)
                    raise HTTPException(413, f"File too large (max {MAX_UPLOAD_BYTES // (1024*1024)} MB)")
                fh.write(chunk)
    except HTTPException:
        raise
    except Exception:
        tmp.unlink(missing_ok=True)
        raise HTTPException(500, "Could not save the upload")
    if size == 0:
        tmp.unlink(missing_ok=True)
        raise HTTPException(400, "Empty file")
    cur = con.execute(
        """INSERT INTO pending_uploads
           (user_id, title, description, category_id, temp_path, file_size, mime, status)
           VALUES (?,?,?,?,?,?,?, 'uploading')""",
        (user["id"], title, description, category_id, str(tmp), size, mime))
    con.commit()
    upload_id = cur.lastrowid
    con.close()
    asyncio.create_task(_send_upload_to_telegram(upload_id))
    return {"ok": True, "upload_id": upload_id,
            "message": "Upload received. Your video will appear after review."}


@app.get("/api/me/uploads")
def my_uploads(user: dict = Depends(require_user)):
    """The signed-in user's own videos + in-flight uploads."""
    con = db()
    vids = con.execute(
        VIDEO_SELECT_COUNTS + " WHERE v.uploaded_by=? ORDER BY v.id DESC",
        (user["id"],)).fetchall()
    pend = con.execute(
        "SELECT id, title, status, error, created_at FROM pending_uploads "
        "WHERE user_id=? AND status != 'done' ORDER BY id DESC",
        (user["id"],)).fetchall()
    con.close()
    return {"videos": [_public_video_row(r, user["id"]) for r in vids],
            "pending": [row_to_dict(r) for r in pend]}


# ============================================================ admin API


@app.get("/api/admin/stats")
def admin_stats(_: dict = Depends(require_admin)):
    con = db()
    totals = {
        "videos": con.execute("SELECT COUNT(*) c FROM videos").fetchone()["c"],
        "published": con.execute(
            "SELECT COUNT(*) c FROM videos WHERE visibility='published'").fetchone()["c"],
        "views": con.execute(
            "SELECT COALESCE(SUM(view_count),0) s FROM videos").fetchone()["s"],
        "likes": con.execute("SELECT COUNT(*) c FROM likes").fetchone()["c"],
        "users": con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"],
        "comments": con.execute("SELECT COUNT(*) c FROM comments").fetchone()["c"],
    }
    # views per day, last 14 days (fill gaps with 0)
    days = [(datetime.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(13, -1, -1)]
    counts = {d: 0 for d in days}
    for r in con.execute(
            """SELECT date(viewed_at) d, COUNT(*) c FROM view_events
               WHERE viewed_at > datetime('now', '-14 days')
               GROUP BY date(viewed_at)""").fetchall():
        if r["d"] in counts:
            counts[r["d"]] = r["c"]
    views_by_day = [{"day": d, "views": counts[d]} for d in days]
    top_videos = [{
        "id": r["id"], "title": r["title"], "view_count": r["view_count"],
        "thumb_url": f"/api/thumb/{r['id']}" if r["thumb_path"] else None,
    } for r in con.execute(
        "SELECT id, title, view_count, thumb_path FROM videos "
        "ORDER BY view_count DESC LIMIT 5").fetchall()]
    recent_users = [{
        "name": r["name"], "email": r["email"], "created_at": r["created_at"],
    } for r in con.execute(
        "SELECT name, email, created_at FROM users ORDER BY id DESC LIMIT 5").fetchall()]
    recent_comments = [{
        "id": r["id"], "body": r["body"][:120], "user_name": r["user_name"],
        "video_id": r["video_id"], "video_title": r["video_title"],
        "created_at": r["created_at"],
    } for r in con.execute(
        """SELECT c.id, c.body, c.created_at, u.name AS user_name,
                  v.id AS video_id, v.title AS video_title
           FROM comments c JOIN users u ON u.id=c.user_id
           JOIN videos v ON v.id=c.video_id
           ORDER BY c.id DESC LIMIT 8""").fetchall()]
    con.close()
    return {"totals": totals, "views_by_day": views_by_day,
            "top_videos": top_videos, "recent_users": recent_users,
            "recent_comments": recent_comments}


@app.post("/api/admin/videos/bulk")
async def admin_bulk(request: Request, _: dict = Depends(require_admin)):
    data = await request.json()
    ids = [int(i) for i in (data.get("ids") or []) if str(i).isdigit()]
    action = data.get("action")
    if not ids or action not in ("publish", "unpublish", "delete"):
        raise HTTPException(400, "Provide ids[] and action=publish|unpublish|delete")
    con = db()
    ph = ",".join("?" * len(ids))
    if action == "delete":
        con.execute(f"DELETE FROM videos WHERE id IN ({ph})", ids)
    else:
        vis = "published" if action == "publish" else "unpublished"
        con.execute(f"UPDATE videos SET visibility=?, published_at=CASE WHEN ?='published' THEN COALESCE(published_at, datetime('now')) ELSE published_at END WHERE id IN ({ph})",
                    [vis, vis] + ids)
    con.commit()
    con.close()
    log_activity(f"videos_{action}", f"videos:{','.join(str(i) for i in ids[:10])}", f"count={len(ids)}")
    return {"ok": True, "action": action, "count": len(ids)}


@app.get("/api/admin/uploads")
def admin_uploads(_: None = Depends(require_admin)):
    """Moderation queue: user-uploaded videos waiting for review + pending sends."""
    con = db()
    queue = con.execute(
        """SELECT v.id, v.title, v.created_at, u.name AS uploader, u.email AS uploader_email
           FROM videos v JOIN users u ON u.id=v.uploaded_by
           WHERE v.visibility='unpublished' AND v.uploaded_by IS NOT NULL
           ORDER BY v.id DESC""").fetchall()
    sending = con.execute(
        """SELECT p.id, p.title, p.status, p.error, p.created_at, u.name AS uploader
           FROM pending_uploads p JOIN users u ON u.id=p.user_id
           WHERE p.status IN ('uploading','sending','sent','failed')
           ORDER BY p.id DESC""").fetchall()
    con.close()
    return {"queue": [row_to_dict(r) for r in queue],
            "sending": [row_to_dict(r) for r in sending]}


@app.delete("/api/admin/uploads/{upload_id}")
def admin_upload_discard(upload_id: int, _: None = Depends(require_admin)):
    """Discard a stuck/failed pending upload (frees its temp file)."""
    con = db()
    r = con.execute("SELECT temp_path FROM pending_uploads WHERE id=?", (upload_id,)).fetchone()
    if r:
        con.execute("DELETE FROM pending_uploads WHERE id=?", (upload_id,))
        con.commit()
    con.close()
    if r and r["temp_path"] and os.path.exists(r["temp_path"]):
        try:
            os.remove(r["temp_path"])
        except Exception:
            pass
    return {"ok": True}


@app.get("/api/admin/comments")
def admin_comments(page: int = 1, per_page: int = 30,
                   _: dict = Depends(require_admin)):
    per_page = max(1, min(per_page, 50))
    page = max(1, page)
    con = db()
    total = con.execute("SELECT COUNT(*) c FROM comments").fetchone()["c"]
    rows = con.execute(
        """SELECT c.id, c.body, c.created_at, u.name AS user_name,
                  v.id AS video_id, v.title AS video_title
           FROM comments c JOIN users u ON u.id=c.user_id
           JOIN videos v ON v.id=c.video_id
           ORDER BY c.id DESC LIMIT ? OFFSET ?""",
        (per_page, (page - 1) * per_page)).fetchall()
    con.close()
    return {"total": total, "items": [dict(r) for r in rows]}


@app.delete("/api/admin/comments/{comment_id}")
def admin_delete_comment(comment_id: int, _: dict = Depends(require_admin)):
    con = db()
    con.execute("DELETE FROM comments WHERE id=?", (comment_id,))
    con.commit()
    con.close()
    return {"ok": True}


@app.get("/api/admin/users")
def admin_users(page: int = 1, per_page: int = 30,
                _: dict = Depends(require_admin)):
    per_page = max(1, min(per_page, 50))
    page = max(1, page)
    con = db()
    total = con.execute("SELECT COUNT(*) c FROM users").fetchone()["c"]
    rows = con.execute(
        """SELECT u.id, u.name, u.email, u.created_at, u.role, u.banned,
                  (SELECT COUNT(*) FROM likes l WHERE l.user_id=u.id) AS likes,
                  (SELECT COUNT(*) FROM comments c WHERE c.user_id=u.id) AS comments,
                  (SELECT COUNT(*) FROM videos v WHERE v.uploaded_by=u.id) AS uploads
           FROM users u ORDER BY u.id DESC LIMIT ? OFFSET ?""",
        (per_page, (page - 1) * per_page)).fetchall()
    con.close()
    return {"total": total, "items": [dict(r) for r in rows]}


@app.patch("/api/admin/users/{user_id}")
async def admin_user_update(user_id: int, request: Request, _: None = Depends(require_admin)):
    """Ban/unban a user or change their role."""
    data = await request.json()
    con = db()
    u = con.execute("SELECT id, name, email FROM users WHERE id=?", (user_id,)).fetchone()
    if not u:
        con.close()
        raise HTTPException(404, "User not found")
    if "banned" in data:
        # Banning also revokes every existing session token immediately.
        con.execute("UPDATE users SET banned=?, token_version=token_version+1 WHERE id=?",
                    (1 if data["banned"] else 0, user_id))
        log_action = "user_ban" if data["banned"] else "user_unban"
        log_activity(log_action, f"user:{user_id}", f"{u['name']} <{u['email']}>")
    if "role" in data and data["role"] in ("user", "admin"):
        con.execute("UPDATE users SET role=? WHERE id=?", (data["role"], user_id))
        log_activity("user_role", f"user:{user_id}", f"role -> {data['role']}")
    if data.get("password"):
        pw = str(data["password"])
        if len(pw) < 10:
            con.close()
            raise HTTPException(400, "Password must be 10+ characters")
        # A password reset revokes every existing session token immediately.
        con.execute("UPDATE users SET password_hash=?, token_version=token_version+1 WHERE id=?",
                    (hash_password(pw), user_id))
        log_activity("user_pw_reset", f"user:{user_id}", f"{u['name']} <{u['email']}>")
    con.commit()
    con.close()
    return {"ok": True}


@app.get("/api/admin/reports")
def admin_reports(_: None = Depends(require_admin)):
    """Open reports queue (videos + comments flagged by users)."""
    con = db()
    rows = con.execute(
        """SELECT r.id, r.reason, r.created_at, r.video_id, r.comment_id,
                  u.name AS reporter, v.title AS video_title, cm.body AS comment_body
           FROM reports r
           LEFT JOIN users u ON u.id=r.reporter_id
           LEFT JOIN videos v ON v.id=r.video_id
           LEFT JOIN comments cm ON cm.id=r.comment_id
           WHERE r.status='open' ORDER BY r.id DESC LIMIT 100""").fetchall()
    con.close()
    return {"items": [row_to_dict(r) for r in rows]}


@app.patch("/api/admin/reports/{report_id}")
async def admin_report_update(report_id: int, request: Request, _: None = Depends(require_admin)):
    data = await request.json()
    con = db()
    if data.get("status") == "dismissed":
        con.execute("UPDATE reports SET status='dismissed' WHERE id=?", (report_id,))
        con.commit()
    con.close()
    return {"ok": True}


@app.get("/api/settings")
def public_settings():
    """Public site settings the frontend needs (name, uploads on/off)."""
    return {"site_name": get_setting("site_name", "StreamHub"),
            "uploads_enabled": get_setting("uploads_enabled", "1") == "1",
            "announcement": get_setting("announcement", "")}


@app.post("/api/admin/categories")
async def admin_category_create(request: Request, _: None = Depends(require_admin)):
    data = await request.json()
    name = (data.get("name") or "").strip()[:60]
    if not name:
        raise HTTPException(400, "Name required")
    slug = re.sub(r"[^a-z0-9-]", "-", name.lower())[:60].strip("-") or "cat"
    con = db()
    if con.execute("SELECT id FROM categories WHERE slug=?", (slug,)).fetchone():
        con.close()
        raise HTTPException(400, "A category with this name already exists")
    cur = con.execute("INSERT INTO categories (name, slug) VALUES (?,?)", (name, slug))
    con.commit()
    cid = cur.lastrowid
    con.close()
    log_activity("category_add", f"category:{cid}", name)
    return {"id": cid, "name": name, "slug": slug}


@app.patch("/api/admin/categories/{cat_id}")
async def admin_category_rename(cat_id: int, request: Request, _: None = Depends(require_admin)):
    data = await request.json()
    name = (data.get("name") or "").strip()[:60]
    if not name:
        raise HTTPException(400, "Name required")
    con = db()
    if not con.execute("SELECT id FROM categories WHERE id=?", (cat_id,)).fetchone():
        con.close()
        raise HTTPException(404, "Category not found")
    con.execute("UPDATE categories SET name=? WHERE id=?", (name, cat_id))
    con.commit()
    con.close()
    log_activity("category_rename", f"category:{cat_id}", name)
    return {"ok": True}


@app.delete("/api/admin/categories/{cat_id}")
def admin_category_delete(cat_id: int, _: None = Depends(require_admin)):
    con = db()
    cat = con.execute("SELECT slug, name FROM categories WHERE id=?", (cat_id,)).fetchone()
    if not cat:
        con.close()
        raise HTTPException(404, "Category not found")
    if cat["slug"] == "uncategorized":
        con.close()
        raise HTTPException(400, "The Uncategorized category cannot be deleted")
    unc = con.execute("SELECT id FROM categories WHERE slug='uncategorized'").fetchone()["id"]
    con.execute("UPDATE videos SET category_id=? WHERE category_id=?", (unc, cat_id))
    con.execute("DELETE FROM video_categories WHERE category_id=?", (cat_id,))
    con.execute("INSERT OR IGNORE INTO video_categories (video_id, category_id) "
                "SELECT id, ? FROM videos WHERE category_id=?", (unc, unc))
    con.execute("DELETE FROM categories WHERE id=?", (cat_id,))
    con.commit()
    con.close()
    log_activity("category_delete", f"category:{cat_id}", cat["name"])
    return {"ok": True}


@app.get("/api/admin/system")
def admin_system(_: None = Depends(require_admin)):
    """System info for the Super Admin dashboard."""
    import shutil as _sh
    db_path = DATA_DIR / "app.db"
    usage = _sh.disk_usage(str(DATA_DIR))
    con = db()
    pending_review = con.execute(
        "SELECT COUNT(*) c FROM videos WHERE visibility='unpublished' AND uploaded_by IS NOT NULL"
    ).fetchone()["c"]
    open_reports = con.execute(
        "SELECT COUNT(*) c FROM reports WHERE status='open'").fetchone()["c"]
    signups = {r["d"]: r["c"] for r in con.execute(
        """SELECT date(created_at) d, COUNT(*) c FROM users
           WHERE created_at >= datetime('now','-13 days') GROUP BY date(created_at)""")}
    con.close()
    days = [(datetime.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(13, -1, -1)]
    return {
        "db_size_mb": round(db_path.stat().st_size / 1048576, 2) if db_path.exists() else 0,
        "disk_total_gb": round(usage.total / 1073741824, 1),
        "disk_free_gb": round(usage.free / 1073741824, 2),
        "pending_review": pending_review,
        "open_reports": open_reports,
        "signups_by_day": [{"day": d, "signups": signups.get(d, 0)} for d in days],
    }


@app.get("/api/admin/backup")
def admin_backup(_: None = Depends(require_admin)):
    """Download the full SQLite database (site backup)."""
    from fastapi.responses import FileResponse
    db_path = DATA_DIR / "app.db"
    if not db_path.exists():
        raise HTTPException(404, "Database not found")
    log_activity("backup", "", "database downloaded")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M")
    return FileResponse(str(db_path), media_type="application/octet-stream",
                        filename=f"streamhub-backup-{stamp}.db")


@app.get("/api/admin/export/{kind}")
def admin_export(kind: str, _: None = Depends(require_admin)):
    """CSV export of users or videos."""
    import csv as _csv
    import io as _io
    from fastapi.responses import PlainTextResponse
    con = db()
    if kind == "users":
        rows = con.execute("SELECT id, name, email, role, banned, created_at FROM users ORDER BY id").fetchall()
        cols = ["id", "name", "email", "role", "banned", "created_at"]
    elif kind == "videos":
        rows = con.execute(
            """SELECT v.id, v.title, c.name AS category, v.visibility, v.view_count,
                      v.file_size, v.created_at FROM videos v
               LEFT JOIN categories c ON c.id=v.category_id ORDER BY v.id""").fetchall()
        cols = ["id", "title", "category", "visibility", "view_count", "file_size", "created_at"]
    else:
        con.close()
        raise HTTPException(404, "Unknown export")
    con.close()
    buf = _io.StringIO()
    w = _csv.writer(buf)
    w.writerow(cols)
    for r in rows:
        w.writerow([r[c] for c in cols])
    return PlainTextResponse(buf.getvalue(), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={kind}.csv"})


@app.get("/api/admin/settings")
def admin_settings_get(_: None = Depends(require_admin)):
    con = db()
    rows = con.execute("SELECT key, value FROM settings").fetchall()
    con.close()
    return {r["key"]: r["value"] for r in rows}


@app.patch("/api/admin/settings")
async def admin_settings_update(request: Request, _: None = Depends(require_admin)):
    data = await request.json()
    allowed = {"site_name", "uploads_enabled", "moderation_enabled",
               "announcement", "maintenance_mode"}
    con = db()
    for k, v in data.items():
        if k in allowed:
            con.execute("INSERT INTO settings (key, value) VALUES (?,?) "
                        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                        (k, str(v)[:120]))
    con.commit()
    con.close()
    log_activity("settings", "", ", ".join(f"{k}={v}" for k, v in data.items() if k in allowed))
    return {"ok": True}


@app.get("/api/admin/activity")
def admin_activity(page: int = 1, per_page: int = 40, _: None = Depends(require_admin)):
    per_page = max(1, min(per_page, 100))
    page = max(1, page)
    con = db()
    total = con.execute("SELECT COUNT(*) c FROM activity_log").fetchone()["c"]
    rows = con.execute(
        "SELECT * FROM activity_log ORDER BY id DESC LIMIT ? OFFSET ?",
        (per_page, (page - 1) * per_page)).fetchall()
    con.close()
    return {"total": total, "items": [row_to_dict(r) for r in rows]}


@app.get("/api/admin/videos/{video_id}/stats")
def admin_video_stats(video_id: int, _: None = Depends(require_admin)):
    """Per-video analytics: totals + last-14-days view counts."""
    con = db()
    v = con.execute("SELECT id, title, view_count, created_at FROM videos WHERE id=?",
                    (video_id,)).fetchone()
    if not v:
        con.close()
        raise HTTPException(404, "Video not found")
    likes = con.execute("SELECT COUNT(*) c FROM likes WHERE video_id=?", (video_id,)).fetchone()["c"]
    comments = con.execute("SELECT COUNT(*) c FROM comments WHERE video_id=?", (video_id,)).fetchone()["c"]
    in_lists = con.execute("SELECT COUNT(*) c FROM playlist_items WHERE video_id=?", (video_id,)).fetchone()["c"]
    days = con.execute(
        """SELECT date(viewed_at) AS d, COUNT(*) AS c FROM view_events
           WHERE video_id=? AND viewed_at >= datetime('now','-13 days')
           GROUP BY date(viewed_at)""", (video_id,)).fetchall()
    con.close()
    by_day = {r["d"]: r["c"] for r in days}
    series = []
    for i in range(13, -1, -1):
        d = (datetime.now(timezone.utc) - timedelta(days=i)).strftime("%Y-%m-%d")
        series.append({"date": d, "views": by_day.get(d, 0)})
    return {"video": row_to_dict(v), "likes": likes, "comments": comments,
            "in_playlists": in_lists, "views_by_day": series}


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
def admin_videos(q: Optional[str] = None, sort: str = "new",
                 _: None = Depends(require_admin)):
    con = db()
    join = ""
    params: list = []
    where = ""
    if q:
        where = " WHERE v.title LIKE ?"
        params.append(f"%{q}%")
    if sort == "popular":
        order = "v.view_count DESC, v.id DESC"
    elif sort == "trending":
        order = "COALESCE(w.recent, 0) DESC, v.view_count DESC"
        join = (" LEFT JOIN (SELECT video_id, COUNT(*) AS recent FROM view_events"
                " WHERE viewed_at > datetime('now', '-7 days') GROUP BY video_id) w"
                " ON w.video_id = v.id")
    else:
        order = "v.id DESC"
    rows = con.execute(f"{VIDEO_SELECT_COUNTS}{join}{where} ORDER BY {order} LIMIT 200",
                       params).fetchall()
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
    if "featured" in body:
        fields.append("featured=?")
        params.append(1 if body["featured"] else 0)
    if not fields:
        raise HTTPException(400, "Nothing to update")
    con = db()
    con.execute(f"UPDATE videos SET {', '.join(fields)} WHERE id=?", params + [video_id])
    if "category" in body:
        row = con.execute("SELECT category_id FROM videos WHERE id=?", (video_id,)).fetchone()
        if row and row["category_id"]:
            con.execute("INSERT OR IGNORE INTO video_categories (video_id, category_id) "
                        "VALUES (?,?)", (video_id, row["category_id"]))
    con.commit()
    con.close()
    return {"ok": True}


@app.get("/api/admin/videos/{video_id}")
def admin_video_detail(video_id: int, _: None = Depends(require_admin)):
    """Full video detail for admin preview (ignores visibility)."""
    con = db()
    r = con.execute(f"{VIDEO_SELECT_COUNTS} WHERE v.id=?", (video_id,)).fetchone()
    con.close()
    if not r:
        raise HTTPException(404, "Video not found")
    detail = _public_video_row(r)
    detail["related"] = []
    return detail


@app.delete("/api/admin/videos/{video_id}")
def admin_delete(video_id: int, _: None = Depends(require_admin)):
    """Remove the website listing only — the Telegram file is never touched."""
    con = db()
    r = con.execute("SELECT thumb_path FROM videos WHERE id=?", (video_id,)).fetchone()
    # video_categories references videos WITHOUT ON DELETE CASCADE (v16),
    # so every categorized video's delete failed on the FK — clear the
    # memberships (and any promo record) before deleting the listing.
    con.execute("DELETE FROM video_categories WHERE video_id=?", (video_id,))
    con.execute("DELETE FROM promo_posts WHERE video_id=?", (video_id,))
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
    chan = (body or {}).get("channel_id")
    try:
        return await sync_channel(limit=int(limit),
                                  channel_id=int(chan) if chan else None)
    except Exception as e:
        raise HTTPException(502, f"Sync failed: {str(e)[:300]}")


# ------------------------------------------------- v21 channel migration
# One-time tooling for switching the content source channel without
# duplicating the library: (1) backfill every listing's Telegram document
# id from its own channel, (2) rewrite refs of listings whose file also
# lives in the new channel to that channel's message. Progress is kept in
# _maint and read back via the status endpoint.
_maint = {"running": False, "kind": "", "processed": 0, "updated": 0, "error": ""}


async def _backfill_doc_ids():
    _maint.update(running=True, kind="backfill", processed=0, updated=0, error="")
    try:
        client = await tg.client()
        con = db()
        chans = [r[0] for r in con.execute(
            "SELECT DISTINCT tg_channel_id FROM videos "
            "WHERE tg_channel_id IS NOT NULL").fetchall()]
        con.close()
        for chan in chans:
            async for msg in client.iter_messages(chan, limit=None):
                attrs = _video_attrs(msg)
                if not attrs:
                    continue
                _maint["processed"] += 1
                con = db()
                cur = con.execute(
                    "UPDATE videos SET tg_doc_id=? WHERE tg_file_unique_id=? "
                    "AND (tg_doc_id IS NULL OR tg_doc_id='')",
                    (attrs["unique_id"], f"{chan}:{msg.id}"))
                _maint["updated"] += cur.rowcount
                con.commit()
                con.close()
    except Exception as e:
        _maint["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        _maint["running"] = False


async def _rewrite_refs(target: int):
    _maint.update(running=True, kind="rewrite", processed=0, updated=0, error="")
    try:
        client = await tg.client()
        async for msg in client.iter_messages(target, limit=None):
            attrs = _video_attrs(msg)
            if not attrs:
                continue
            _maint["processed"] += 1
            con = db()
            cur = con.execute(
                "UPDATE videos SET tg_channel_id=?, tg_message_id=?, tg_file_unique_id=? "
                "WHERE tg_doc_id=? AND tg_channel_id != ?",
                (target, msg.id, f"{target}:{msg.id}", attrs["unique_id"], target))
            _maint["updated"] += cur.rowcount
            con.commit()
            con.close()
    except Exception as e:
        _maint["error"] = f"{type(e).__name__}: {str(e)[:200]}"
    finally:
        _maint["running"] = False


@app.post("/api/admin/maint/backfill-doc-ids")
async def admin_backfill_doc_ids(_: None = Depends(require_admin)):
    if _maint["running"]:
        raise HTTPException(409, "maintenance already running")
    asyncio.create_task(_backfill_doc_ids())
    return {"ok": True, "started": "backfill"}


@app.post("/api/admin/maint/rewrite-refs")
async def admin_rewrite_refs(body: dict, _: None = Depends(require_admin)):
    if _maint["running"]:
        raise HTTPException(409, "maintenance already running")
    target = int((body or {}).get("channel_id") or 0)
    if not target:
        raise HTTPException(400, "channel_id required")
    asyncio.create_task(_rewrite_refs(target))
    return {"ok": True, "started": "rewrite", "target": target}


@app.get("/api/admin/maint/status")
def admin_maint_status(_: None = Depends(require_admin)):
    return dict(_maint)


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
    asyncio.create_task(_thumb_worker())
    asyncio.create_task(_promo_worker())


# ------------------------------------------------- public promo channel
# v20: posts watch-link cards (thumbnail + title + site link) for new videos
# to the PUBLIC promo channel (@StreamHubWatch) — the shareable front door
# that turns Telegram discovery + forwards into website views (and ad
# impressions). Strictly throttled so it never looks like spam: max 6 cards
# per day, at least 20 minutes apart, only videos published in the last
# 48 hours that already have a thumbnail. The channel id lives in
# data/promo.json (same pattern as channel.json) so it can change without
# a code deploy; absent file = promo posting off.
def _promo_channel_id():
    p = DATA_DIR / "promo.json"
    if p.exists():
        try:
            return json.loads(p.read_text()).get("channel_id")
        except Exception:
            return None
    return None


def _promo_base() -> str:
    return os.environ.get("PUBLIC_BASE_URL") or "https://ramwanstream.alwaysdata.net"


async def _post_promo_card(video: dict) -> bool:
    ch_id = _promo_channel_id()
    if not ch_id:
        return False
    tp = video.get("thumb_path") or ""
    if not tp:
        return False
    p = Path(tp) if os.path.isabs(tp) else (BASE_DIR.parent / tp)
    if not p.exists():
        return False
    client = await tg.client()
    ent = await client.get_entity(int(ch_id))
    title = (video.get("title") or "Watch now")[:120]
    caption = (f"🎬 {title}\n\n▶️ Watch in HD: {_promo_base()}/watch.html?v={video['id']}\n"
               "🔞 Adults only (18+)")
    await client.send_file(ent, file=str(p), caption=caption)
    con = db()
    try:
        con.execute("INSERT OR IGNORE INTO promo_posts (video_id, posted_at) "
                    "VALUES (?, datetime('now'))", (video["id"],))
        con.commit()
    finally:
        con.close()
    return True


async def _promo_tick():
    if not _promo_channel_id():
        return
    con = db()
    try:
        today = con.execute(
            "SELECT COUNT(*) c FROM promo_posts WHERE date(posted_at)=date('now')"
        ).fetchone()["c"]
        if today >= 6:
            return
        last = con.execute("SELECT MAX(posted_at) m FROM promo_posts").fetchone()["m"]
        if last:
            last_dt = datetime.fromisoformat(last).replace(tzinfo=timezone.utc)
            if (datetime.now(timezone.utc) - last_dt).total_seconds() < 1200:
                return
        row = con.execute(
            """SELECT v.id, v.title, v.thumb_path FROM videos v
               LEFT JOIN promo_posts pp ON pp.video_id = v.id
               WHERE v.visibility='published' AND pp.video_id IS NULL
                 AND v.thumb_path IS NOT NULL AND v.thumb_path != ''
                 AND v.published_at >= datetime('now','-48 hours')
               ORDER BY v.id DESC LIMIT 1""").fetchone()
    finally:
        con.close()
    if row:
        await _post_promo_card(dict(row))


async def _promo_worker():
    await asyncio.sleep(90)  # let startup/sync settle first
    while True:
        try:
            await _promo_tick()
        except Exception as e:
            print("promo worker error:", e, flush=True)
        await asyncio.sleep(600)


@app.post("/api/admin/promo-test")
async def admin_promo_test(request: Request,
                           x_admin_token: Optional[str] = Header(None)):
    require_admin(x_admin_token)
    body = await request.json()
    vid = int(body.get("video_id") or 0)
    con = db()
    try:
        row = con.execute("SELECT id, title, thumb_path FROM videos WHERE id=?",
                          (vid,)).fetchone()
    finally:
        con.close()
    if not row:
        raise HTTPException(404, "video not found")
    ok = await _post_promo_card(dict(row))
    return {"ok": ok, "video_id": vid}


# ------------------------------------------------------- promotion / SEO
# Per-video link previews: when a watch link is shared (Telegram, WhatsApp,
# X...), preview bots fetch the raw HTML without running JS — so the watch
# page is served through this route, which injects THAT video's title and
# thumbnail into the OG/Twitter tags. Visitors get the identical page.
SITE_BASE = "https://ramwanstream.alwaysdata.net"
WEB_DIR = (BASE_DIR / ".." / "web").resolve()


def _meta_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace('"', "&quot;").replace("<", "&lt;")


def _swap_meta(html: str, attr: str, value: str) -> str:
    return re.sub(r'(<meta (?:property|name)="' + re.escape(attr) + r'" content=")[^"]*(")',
                  lambda m: m.group(1) + _meta_escape(value) + m.group(2), html, count=1)


@app.get("/watch.html")
def watch_page(v: int = 0):
    from fastapi.responses import HTMLResponse
    html = (WEB_DIR / "watch.html").read_text(encoding="utf-8")
    if v:
        con = db()
        r = con.execute("SELECT title, thumb_path FROM videos WHERE id=? AND visibility='published'",
                        (v,)).fetchone()
        con.close()
        if r:
            title = (r["title"] or "Watch on StreamHub")[:120]
            desc = f"Watch {title} online in HD on StreamHub — free streaming, no download needed."
            image = f"{SITE_BASE}/api/thumb/{v}" if r["thumb_path"] else f"{SITE_BASE}/og-default.jpg"
            html = re.sub(r"<title>.*?</title>", f"<title>{_meta_escape(title)} — StreamHub</title>",
                          html, count=1)
            for attr, val in (("og:title", title), ("twitter:title", title),
                              ("og:description", desc), ("twitter:description", desc),
                              ("description", desc), ("og:image", image),
                              ("twitter:image", image),
                              ("og:url", f"{SITE_BASE}/watch.html?v={v}")):
                html = _swap_meta(html, attr, val)
    return HTMLResponse(html)


@app.get("/sitemap.xml")
def sitemap():
    from fastapi.responses import Response
    con = db()
    rows = con.execute(
        "SELECT id, COALESCE(published_at, created_at) d FROM videos "
        "WHERE visibility='published' ORDER BY id DESC LIMIT 5000").fetchall()
    con.close()
    parts = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
             f"<url><loc>{SITE_BASE}/</loc><changefreq>hourly</changefreq><priority>1.0</priority></url>",
             f"<url><loc>{SITE_BASE}/library.html</loc><changefreq>daily</changefreq><priority>0.5</priority></url>"]
    for r in rows:
        lastmod = f"<lastmod>{str(r['d'])[:10]}</lastmod>" if r["d"] else ""
        parts.append(f"<url><loc>{SITE_BASE}/watch.html?v={r['id']}</loc>{lastmod}"
                     f"<changefreq>weekly</changefreq><priority>0.7</priority></url>")
    parts.append("</urlset>")
    return Response("\n".join(parts), media_type="application/xml")


@app.get("/robots.txt")
def robots():
    from fastapi.responses import PlainTextResponse
    return PlainTextResponse(
        "User-agent: *\nAllow: /\n\n"
        f"Sitemap: {SITE_BASE}/sitemap.xml\n")


# serve the static frontend (../web) at / — mounted LAST so /api/* routes win
if WEB_DIR.exists():
    class CachedStatic(StaticFiles):
        async def get_response(self, path: str, scope):
            resp = await super().get_response(path, scope)
            # CSS/JS/images: cache 1 day (filenames don't change between deploys,
            # HTML pages stay fresh). Makes repeat visits feel instant.
            if resp.status_code == 200 and not path.endswith(".html"):
                resp.headers["Cache-Control"] = "public, max-age=86400"
            return resp
    app.mount("/", CachedStatic(directory=str(WEB_DIR), html=True), name="web")
