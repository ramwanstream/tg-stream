# StreamHub — Telegram video streaming site (single container: API + frontend)
FROM python:3.12-slim

WORKDIR /app

# deps first (better layer caching)
COPY backend/requirements.txt ./backend/requirements.txt
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY backend/app.py backend/schema.sql ./backend/
COPY web/ ./web/

ENV PORT=8000
EXPOSE 8000

# app.py expects: DATA_DIR default = backend/../data, frontend = backend/../web
CMD ["sh", "-c", "cd /app/backend && exec uvicorn app:app --host 0.0.0.0 --port ${PORT}"]
