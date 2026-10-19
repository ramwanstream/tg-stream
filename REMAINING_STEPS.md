# Baaki steps — sirf aap kar sakte ho (koi approval card nahi, bas ye checklist)

Sab kuch ban aur test ho chuka hai. Neeche wale **4 steps** me aapka haath
chahiye — inme Telegram/Render ke login aur aapke phone par aane wale code
shaamil hain, isliye main inhe aapke bina nahi kar sakta.

## Step 1 — Telegram API credentials (ek baar, ~5 min)

1. Phone browser me **my.telegram.org** kholo → apna **secondary** Telegram
   account ka number daalo (country code ke saath, jaise +91…).
2. Code aapke Telegram app me aayega → site par daalo → login.
3. **API development tools** → **Create application** (app title kuch bhi,
   platform "Other") → **Create**.
4. Wahan dikhenge **api_id** (number) aur **api_hash** (lamba text) → inhe
   copy karke apne paas note kar lo. Ye secret hain — kisi ko mat bhejo.

## Step 2 — Code ko GitHub par daalo (~10 min, phone se)

1. **github.com** par login → **New repository** → naam `tg-stream`, Public,
   **Create repository**.
2. Repo me **Add file → Upload files** → is project ke ye files upload karo
   (folder structure same rakho):
   - `render.yaml`
   - `backend/` ke andar: `app.py`, `requirements.txt`, `schema.sql`, `start.sh`, `.gitignore`
   - `web/` ke andar: `index.html`, `watch.html`, `library.html`, `admin.html`,
     `css/style.css`, `js/config.js`, `js/app.js`
   - `data/.gitignore`
   - `README.md`
3. Neeche **Commit changes** dabao.

> `venv/`, `data/` ka baaki samaan aur `__pycache__` upload mat karna —
> `.gitignore` already inhe rokta hai.

## Step 3 — Render par free deploy (~10 min)

1. **render.com** par signup/login (GitHub se login sabse aasaan).
2. **New → Blueprint** → apna `tg-stream` repo select karo → **Connect**.
3. Blueprint `render.yaml` padh lega. **Environment** me do values daalo:
   - `TG_API_ID` = Step 1 wala api_id
   - `TG_API_HASH` = Step 1 wali api_hash
   (`ADMIN_TOKEN` apne aap ban jayega.)
4. **Apply** → deploy shuru. 3–5 min me aapko site ka URL milega,
   jaise `https://tg-stream.onrender.com`.

## Step 4 — Website ko Telegram se jodo (site ke admin page se, ~5 min)

1. Apni site kholo → `/admin.html` (jaise `https://tg-stream.onrender.com/admin.html`).
2. **Admin token** daalo — ye Render dashboard me **Environment** tab me
   `ADMIN_TOKEN` ki value hai (Render ne generate ki thi).
3. **Step 2** me apne **secondary** Telegram account ka phone number daalo →
   **Send code** → code Telegram app me aayega → daalo → **Verify**.
   (Agar 2FA password hai to wo bhi daalo.)
4. **Step 3** me apna **private channel** banao (secondary account ke Telegram
   app me, 30 second ka kaam) → uska username `@kuchbhi` ya invite link
   yahan paste karo → **Link**.
5. **🔄 Sync channel now** dabao → channel ke videos website par dikhne lagenge.
6. Test: site kholo → video play karo → seek (aage-peeche) karo.

## Yaad rakhne wali baatein

- **Channel me jo daaloge, wo website par PUBLIC ho jayega.** Channel
  dedicated rakho — galti se personal video mat daalna.
- Free Render server 15 min khaali raha to "so" jata hai — pehla visitor
  ~1 min wait karega, phir sab normal. Roz chalne par ye problem kam hoti hai.
- Redeploy ke baad Telegram login dobara karna padega (free me session save
  nahi rehta) — admin page ka Step 2 dohraya jata hai, 1 min ka kaam.
- Sirf apni ya permission wali videos daalo (copyright).
- Kuch gadbad ho to mujhe exact error message bhejo — main dekh lunga.
