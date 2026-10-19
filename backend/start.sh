#!/bin/bash
# Local dev start: ./start.sh   (admin token printed on first run)
cd "$(dirname "$0")"
[ -d venv ] || python3 -m venv venv
./venv/bin/pip -q install -r requirements.txt
exec ./venv/bin/uvicorn app:app --host 0.0.0.0 --port 8000
