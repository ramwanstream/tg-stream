import asyncio, json, os, re, sys
from pathlib import Path
from urllib.parse import urlparse

BASE = Path(__file__).parent
cfg = json.loads((BASE / "data" / "tg_config.json").read_text())
API_ID, API_HASH = int(cfg["api_id"]), cfg["api_hash"]

def tg_proxy():
    url = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY") or ""
    if not url:
        return None
    u = urlparse(url)
    return ("http", u.hostname, u.port or 3128, u.username, u.password, True)

CANDIDATES = ["StreamHubWatchBot", "RamwanStreamBot", "StreamHubVideosBot",
              "WatchStreamHubBot", "StreamHubRamwanBot", "RamwanStreamHubBot"]

async def main():
    from telethon import TelegramClient
    from telethon.sessions import SQLiteSession
    client = TelegramClient(SQLiteSession(str(BASE / "data" / "tg_session.session")),
                            API_ID, API_HASH, proxy=tg_proxy())
    await client.start()
    me = await client.get_me()
    print("account:", me.first_name, me.id)
    token, username = None, None
    async with client.conversation("BotFather", timeout=60) as conv:
        await conv.send_message("/newbot")
        r = await conv.get_response()
        await conv.send_message("StreamHub")
        r = await conv.get_response()
        print("botfather after name:", r.text[:80].replace("\n", " "))
        for cand in CANDIDATES:
            await conv.send_message(cand)
            r = await conv.get_response()
            m = re.search(r"\d{6,}:[A-Za-z0-9_-]{30,}", r.text)
            if m:
                token, username = m.group(0), cand
                break
            print("username rejected:", cand, "->", r.text[:100].replace("\n", " "))
    await client.disconnect()
    if not token:
        print("FAILED: no username accepted")
        sys.exit(1)
    sec = BASE / ".secrets" / "streamhub-bot-token"
    sec.write_text(token)
    os.chmod(sec, 0o600)
    print("BOT CREATED: @" + username + " (token saved to .secrets)")

    # Set the menu button so the bot opens the site as a Telegram Mini App
    import requests
    px = os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    proxies = {"https": px, "http": px} if px else None
    api = f"https://api.telegram.org/bot{token}"
    r = requests.post(api + "/setChatMenuButton", proxies=proxies, timeout=30, json={
        "menu_button": {"type": "web_app", "text": "Watch Videos",
                        "web_app": {"url": "https://ramwanstream.alwaysdata.net/"}}})
    print("setChatMenuButton:", r.json())
    r = requests.post(api + "/setMyDescription", proxies=proxies, timeout=30, json={
        "description": "Watch videos on StreamHub — tap 'Watch Videos' to open the app."})
    print("setMyDescription:", r.json())
    r = requests.get(api + "/getMe", proxies=proxies, timeout=30)
    print("getMe:", r.json().get("result", {}).get("username"))

asyncio.run(main())
