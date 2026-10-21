/* Shared helpers: API client, auth state, video cards, formatting. */
const $ = (s, el = document) => el.querySelector(s);
const $$ = (s, el = document) => [...el.querySelectorAll(s)];

async function api(path, opts = {}) {
  const headers = { "Content-Type": "application/json", ...(opts.headers || {}) };
  const token = localStorage.getItem("token");
  if (token) headers["Authorization"] = "Bearer " + token;
  const res = await fetch(API + path, { ...opts, headers });
  let data = null;
  try { data = await res.json(); } catch (e) { /* non-JSON */ }
  if (!res.ok) {
    const msg = (data && (data.detail || data.message)) || `Request failed (${res.status})`;
    throw new Error(msg);
  }
  return data;
}

function fmtDur(sec) {
  sec = Math.max(0, Math.round(sec || 0));
  const m = Math.floor(sec / 60), s = sec % 60;
  const h = Math.floor(m / 60);
  return h ? `${h}:${String(m % 60).padStart(2, "0")}:${String(s).padStart(2, "0")}`
           : `${m}:${String(s).padStart(2, "0")}`;
}
function fmtSize(b) {
  if (!b) return "";
  if (b > 1e9) return (b / 1e9).toFixed(1) + " GB";
  if (b > 1e6) return Math.round(b / 1e6) + " MB";
  return Math.round(b / 1e3) + " KB";
}
function fmtViews(n) {
  n = n || 0;
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M views";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K views";
  return n + (n === 1 ? " view" : " views");
}
function timeAgo(iso) {
  if (!iso) return "";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < 3600) return Math.max(1, Math.round(s / 60)) + "m ago";
  if (s < 86400) return Math.round(s / 3600) + "h ago";
  if (s < 86400 * 30) return Math.round(s / 86400) + "d ago";
  return new Date(iso).toLocaleDateString();
}
function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, c =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

function showToast(msg) {
  let t = $("#toast");
  if (!t) {
    t = document.createElement("div");
    t.id = "toast"; t.className = "toast";
    document.body.appendChild(t);
  }
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(t._h);
  t._h = setTimeout(() => t.classList.remove("show"), 2400);
}


const ICON = {
  play: '<svg viewBox="0 0 24 24"><path d="M8 5.5v13l11-6.5z"/></svg>',
  sun: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><circle cx="12" cy="12" r="4.2" fill="currentColor" stroke="none"/><path d="M12 2.5v2.4M12 19.1v2.4M2.5 12h2.4M19.1 12h2.4M4.9 4.9l1.7 1.7M17.4 17.4l1.7 1.7M19.1 4.9l-1.7 1.7M6.6 17.4l-1.7 1.7"/></svg>',
  moon: '<svg viewBox="0 0 24 24"><path d="M20.2 14.6A8.6 8.6 0 1 1 9.4 3.8a7 7 0 0 0 10.8 10.8z"/></svg>',
  home: '<svg viewBox="0 0 24 24"><path d="M12 3 2.5 11h2.7v9h5.1v-6h3.4v6h5.1v-9h2.7z"/></svg>',
  lib: '<svg viewBox="0 0 24 24"><path d="M4 3.5A1.5 1.5 0 0 1 5.5 2H11v18H5.5A1.5 1.5 0 0 1 4 18.5zM13 2h5.5A1.5 1.5 0 0 1 20 3.5v15a1.5 1.5 0 0 1-1.5 1.5H13z" opacity=".95"/><path d="M13 2l7 0-1.8 18H13z" transform="rotate(8 16 11)"/></svg>',
  plus: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg>',
  user: '<svg viewBox="0 0 24 24"><path d="M12 11.2a4.6 4.6 0 1 0-4.6-4.6A4.6 4.6 0 0 0 12 11.2zm0 2.1c-3.9 0-8.5 2-8.5 5.4V21h17v-2.3c0-3.4-4.6-5.4-8.5-5.4z"/></svg>',
};

function videoCard(v) {
  const a = document.createElement("a");
  a.className = "card";
  a.href = "watch.html?v=" + v.id;
  const catInit = esc((v.category || "S")[0].toUpperCase());
  a.innerHTML = `
    <div class="thumb">
      <div class="thumb-ph">▶</div>
      ${v.thumb_url ? `<img src="${esc(v.thumb_url)}" alt="" loading="lazy" decoding="async" onerror="this.remove()">` : ""}
      ${v.duration ? `<span class="badge-dur">${fmtDur(v.duration)}</span>` : ""}
      ${v.resume_position && v.duration && v.resume_position > 5 && v.resume_position < v.duration - 10
        ? `<span class="cwbar"><i style="width:${Math.round(v.resume_position / v.duration * 100)}%"></i></span>` : ""}
    </div>
    <div class="card-row">
      <span class="card-av">${catInit}</span>
      <div class="card-txt">
        <h3>${esc(v.title)}</h3>
        <div class="meta">${v.category ? esc(v.category) + " · " : ""}${fmtViews(v.view_count)} · ${timeAgo(v.published_at)}</div>
      </div>
    </div>`;
  return a;
}

function renderHeader(active) {
  document.title = SITE_NAME + (active && active !== "home" ? " — " + active : "");
  const logged = !!localStorage.getItem("token");
  const name = localStorage.getItem("name") || "?";
  $("header.top").innerHTML = `
    <div class="top-inner">
      <a class="logo" href="index.html"><span class="logo-mark">${ICON.play}</span>${esc(SITE_NAME)}</a>
      <form class="search" id="searchForm">
        <input id="searchInput" type="search" placeholder="Search videos…" autocomplete="off">
        <div class="suggest" id="suggest" style="display:none"></div>
      </form>
      <button class="theme-btn" id="themeBtn" title="Theme">${document.documentElement.dataset.theme === "light" ? ICON.moon : ICON.sun}</button>
      <a class="navlink ${active === "library" ? "active" : ""}" href="library.html">Library</a>
      ${UPLOADS_ON ? `<a class="navlink upload-link" href="upload.html">＋ Upload</a>` : ""}
      ${logged
        ? `<button class="avatar-btn" id="avatarBtn" title="${esc(name)}">${esc(name[0].toUpperCase())}</button>`
        : `<a class="navlink" href="library.html">Sign in</a>`}
    </div>`;
  const sf = $("#searchForm");
  const params = new URLSearchParams(location.search);
  if (params.get("q")) $("#searchInput").value = params.get("q");
  sf.addEventListener("submit", e => {
    e.preventDefault();
    const q = $("#searchInput").value.trim();
    location.href = "index.html" + (q ? "?q=" + encodeURIComponent(q) : "");
  });
  $("#themeBtn").onclick = () => {
    const next = document.documentElement.dataset.theme === "light" ? "dark" : "light";
    document.documentElement.dataset.theme = next;
    localStorage.setItem("theme", next);
    $("#themeBtn").innerHTML = next === "light" ? ICON.moon : ICON.sun;
  };
  // search autocomplete
  const sInput = $("#searchInput"), sBox = $("#suggest");
  let sDeb;
  sInput.addEventListener("input", () => {
    clearTimeout(sDeb);
    const q = sInput.value.trim();
    if (q.length < 2) { sBox.style.display = "none"; return; }
    sDeb = setTimeout(async () => {
      try {
        const d = await api("/api/videos?q=" + encodeURIComponent(q) + "&per_page=6");
        if (!d.items.length) { sBox.style.display = "none"; return; }
        sBox.innerHTML = d.items.map(v => `
          <a class="sug" href="watch.html?v=${v.id}">
            ${v.thumb_url ? `<img src="${esc(v.thumb_url)}" alt="">` : `<span class="sugph">▶</span>`}
            <span class="sugt"><b>${esc(v.title)}</b><small>${fmtViews(v.view_count)}</small></span>
          </a>`).join("");
        sBox.style.display = "";
      } catch (e) { sBox.style.display = "none"; }
    }, 300);
  });
  document.addEventListener("click", e => { if (!sf.contains(e.target)) sBox.style.display = "none"; });
  const ab = $("#avatarBtn");
  if (ab) ab.addEventListener("click", () => {
    if (confirm("Sign out of " + SITE_NAME + "?")) {
      localStorage.removeItem("token");
      localStorage.removeItem("name");
      location.reload();
    }
  });
  showAnnouncement();
}

function showAnnouncement() {
  const msg = (localStorage.getItem("announcement") || "").trim();
  if (!msg || sessionStorage.getItem("ann_dismissed") === msg) return;
  const bar = document.createElement("div");
  bar.className = "announce";
  bar.innerHTML = `<span>${esc(msg)}</span><button aria-label="Dismiss">✕</button>`;
  bar.querySelector("button").onclick = () => { sessionStorage.setItem("ann_dismissed", msg); bar.remove(); };
  document.body.insertBefore(bar, document.body.firstChild);
}

function renderFooter() {
  const f = document.createElement("footer");
  f.innerHTML = `
    <div class="foot-grid">
      <div><div class="logo" style="font-size:18px"><span class="logo-mark" style="width:22px;height:22px">${ICON.play}</span>${esc(SITE_NAME)}</div>
      <p style="margin:8px 0 0">Watch videos in HD, free forever.<br>New videos added every day.</p></div>
      <div><b>Browse</b><br><a href="index.html">Home</a><br><a href="index.html">Trending</a><br><a href="library.html">Library</a></div>
      <div><b>Account</b><br><a href="library.html">Sign in</a><br><a href="upload.html">Upload video</a><br><a href="admin.html">Admin</a></div>
    </div>
    <div style="margin-top:16px">© ${new Date().getFullYear()} ${esc(SITE_NAME)} · Made with ❤️</div>`;
  document.body.appendChild(f);
  // mobile bottom navigation (YouTube-app style)
  if (!document.querySelector(".botnav") && !location.pathname.endsWith("admin.html")) {
    const path = location.pathname;
    const item = (href, icon, label, key) =>
      `<a href="${href}" class="${path.endsWith(key) ? "on" : ""}"><span class="bni">${icon}</span>${label}</a>`;
    const nav = document.createElement("nav");
    nav.className = "botnav";
    nav.innerHTML =
      item("index.html", ICON.home, "Home", "index.html") +
      item("library.html", ICON.lib, "Library", "library.html") +
      (UPLOADS_ON ? item("upload.html", ICON.plus, "Upload", "upload.html") : "") +
      item("library.html", ICON.user, "You", "library.html");
    document.body.appendChild(nav);
  }
  // back-to-top
  const btt = document.createElement("button");
  btt.className = "btt"; btt.textContent = "↑"; btt.title = "Back to top";
  btt.style.display = "none";
  btt.onclick = () => window.scrollTo({ top: 0, behavior: "smooth" });
  addEventListener("scroll", () => { btt.style.display = scrollY > 600 ? "" : "none"; }, { passive: true });
  document.body.appendChild(btt);
}

function showEmpty(el, icon, title, sub) {
  el.innerHTML = `<div class="empty"><div class="big">${icon}</div><h3>${esc(title)}</h3><p>${esc(sub || "")}</p></div>`;
}
