# TG Stream — private Telegram channel → public video website

**LIVE:** https://tg-stream-bebcf.containers.snapdeploy.app · repo: https://github.com/ramwanstream/tg-stream

YouTube-like public site. Videos live in a **private** Telegram channel;
visitors watch them on the website. No Telegram credentials ever reach the browser.

## How it works

```
Private Telegram channel (your videos)
        │  MTProto (Telethon) — reads video chunks on demand
        ▼
Backend (FastAPI, this repo) ── /api/stream/{id} with HTTP Range (seek works)
        │  serves the static frontend too
        ▼
Public website (mobile-first, no framework)
```

- **Any file size** up to Telegram's own cap (2 GB free / 4 GB Premium per file).
  The Bot API 20 MB download limit is bypassed via MTProto; chunks are proxied,
  never fully buffered in server RAM.
- **Auto-publish:** every video in the channel appears on the site automatically
  (background sync every 5 min + manual "Sync now" in admin). Unpublish/delete
  per video from admin — deleting a listing never touches the Telegram file.
- **Accounts:** signup/login (JWT), likes, playlists, watch history + resume.
- **Admin:** token-gated page — Telegram login via phone code (no terminal),
  channel linking, publish/unpublish, edit title/desc/category, sync, status.

## What's tested (2026-10-19, local)

- HTTP Range: full 200, `bytes=0-99` / open-ended / suffix → 206 with correct
  `Content-Range`, byte-identical to the source file, chunk reassembly exact,
  invalid range → 416.
- Real headless Chromium (Playwright, 390×844 mobile viewport): homepage
  renders the JS-driven video card → watch page loads → `video.play()` plays
  (currentTime advancing, readyState 4) → seek to 20s lands correctly.
  Tested with both MP4/H.264 and WebM/VP9 through the same `/api/stream`
  proxy. Zero console errors.
- Auth (signup/duplicate-400/wrong-password-401), like toggle, playlists
  (create/add/remove/delete, ownership enforced), watch history + resume,
  admin token gate (403 on wrong token), search, categories, publish/unpublish,
  graceful errors when Telegram isn't linked yet.
- **Not yet live-tested:** the actual Telegram MTProto fetch (needs your
  api_id/api_hash + login code — see REMAINING_STEPS.md). The code path is
  complete and was verified against Telethon 1.45's documented chunk semantics
  (this caught and fixed a real `limit`-in-chunks bug before launch); only the
  credentials are missing for the live run.

## Run locally

```bash
cd backend && ./start.sh        # first run prints the ADMIN_TOKEN
# open http://localhost:8000  →  admin at http://localhost:8000/admin.html
```

## Deploy (free)

SnapDeploy (no credit card needed): **New Container → Deploy from GitHub** →
connect `ramwanstream/tg-stream` → the `Dockerfile` at repo root is detected →
port 8000 → set env vars `TELEGRAM_APP_ID` / `TELEGRAM_APP_HASH` (from
my.telegram.org) and `ADMIN_TOKEN` → deploy. One container runs everything
(API + site). Public URL: `https://tg-stream-bebcf.containers.snapdeploy.app`

(Render was tried first but now requires a credit card on file even for the
free tier — skipped per the no-card rule. `render.yaml` is kept for reference;
it uses the same `TELEGRAM_APP_*` env names.)

Free-tier honesty: the free container sleeps after 15 min idle (first visitor
waits ~1 min on the wake page); no persistent disk, so SQLite + Telegram
session reset on redeploy/restart — re-login via the admin page and press
"Sync channel now" (videos come back automatically since the channel is the
source of truth). 100 awake-hours/month on the free plan. For production:
paid instance (no sleep) + Postgres.

## Layout

```
backend/  app.py  — entire backend (one file, sectioned, beginner-friendly)
          schema.sql, requirements.txt, start.sh
web/      index.html (home/search/categories) · watch.html (player)
          library.html (auth/playlists/history/liked) · admin.html
          css/style.css · js/{config,app}.js
render.yaml — Render one-click deploy (reference; Render now needs a card)
Dockerfile + .dockerignore — container build (SnapDeploy: repo root, port 8000)
```

## Security notes

- Secrets (api_id/hash, session, admin token, JWT secret) live only in env vars
  or `data/` (chmod 600, gitignored) — never in frontend code or the repo.
- Admin API is token-checked server-side, not just hidden in UI.
- Users can only touch their own playlists/history/likes (ownership checked).
- No arbitrary file-ID fetching: the server only serves videos registered from
  *your* linked channel.
