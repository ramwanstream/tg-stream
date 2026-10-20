-- TG Stream database schema (SQLite for PoC; same tables map 1:1 to Postgres later)
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS categories (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  slug TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS videos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  category_id INTEGER REFERENCES categories(id),
  -- Telegram reference (stable identity of the source file)
  tg_channel_id INTEGER,
  tg_message_id INTEGER,
  tg_file_unique_id TEXT UNIQUE,
  -- Local test mode only (never used in production)
  local_path TEXT,
  file_size INTEGER NOT NULL DEFAULT 0,
  duration INTEGER NOT NULL DEFAULT 0,
  mime TEXT NOT NULL DEFAULT 'video/mp4',
  thumb_path TEXT,
  visibility TEXT NOT NULL DEFAULT 'published' CHECK (visibility IN ('published','unpublished')),
  status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready','unavailable','error')),
  last_error TEXT,
  view_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  published_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_videos_visibility ON videos(visibility, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_videos_category ON videos(category_id);
CREATE INDEX IF NOT EXISTS idx_videos_search ON videos(title, description);

CREATE TABLE IF NOT EXISTS likes (
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (user_id, video_id)
);

CREATE TABLE IF NOT EXISTS playlists (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS playlist_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  playlist_id INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
  video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  position INTEGER NOT NULL DEFAULT 0,
  added_at TEXT NOT NULL DEFAULT (datetime('now')),
  UNIQUE (playlist_id, video_id)
);

CREATE TABLE IF NOT EXISTS watch_history (
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  position INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (user_id, video_id)
);

INSERT OR IGNORE INTO categories (name, slug) VALUES ('Uncategorized', 'uncategorized');

-- v2: per-view event log (powers the admin views chart + trending)
CREATE TABLE IF NOT EXISTS view_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  viewed_at TEXT NOT NULL DEFAULT (datetime('now')),
  ip_hash TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_view_events_video_time ON view_events(video_id, viewed_at);
CREATE INDEX IF NOT EXISTS idx_view_events_time ON view_events(viewed_at);

-- v2: comments
CREATE TABLE IF NOT EXISTS comments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  video_id INTEGER NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  body TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_comments_video ON comments(video_id, created_at DESC);

-- v6: user uploads (YouTube-style). Uploaded files go to the Telegram channel
-- via the userbot; the existing sync then imports them as videos.
-- Moderation: uploads start 'unpublished'; admin approves -> 'published'.
-- uploaded_by links a video to the user who uploaded it (NULL = channel direct).
CREATE TABLE IF NOT EXISTS pending_uploads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  category_id INTEGER REFERENCES categories(id),
  temp_path TEXT NOT NULL,
  file_size INTEGER NOT NULL DEFAULT 0,
  mime TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'uploading' CHECK (status IN ('uploading','sending','sent','failed','done')),
  tg_message_id INTEGER,
  error TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_pending_uploads_user ON pending_uploads(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pending_uploads_status ON pending_uploads(status);

-- v7: site settings (admin-editable) + admin activity log
CREATE TABLE IF NOT EXISTS settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL DEFAULT ''
);
INSERT OR IGNORE INTO settings (key, value) VALUES
  ('site_name', 'StreamHub'),
  ('uploads_enabled', '1'),
  ('moderation_enabled', '1');

CREATE TABLE IF NOT EXISTS activity_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  actor TEXT NOT NULL DEFAULT 'admin',
  action TEXT NOT NULL,
  target TEXT NOT NULL DEFAULT '',
  detail TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_activity_time ON activity_log(created_at DESC);

-- v8: reports (users can flag videos/comments for the admin)
CREATE TABLE IF NOT EXISTS reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  reporter_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
  video_id INTEGER REFERENCES videos(id) ON DELETE CASCADE,
  comment_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
  reason TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open','dismissed')),
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status, created_at DESC);

-- v9: announcement banner + maintenance mode defaults
INSERT OR IGNORE INTO settings (key, value) VALUES
  ('announcement', ''),
  ('maintenance_mode', '0');
