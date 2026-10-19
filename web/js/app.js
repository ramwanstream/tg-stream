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

function videoCard(v) {
  const a = document.createElement("a");
  a.className = "card";
  a.href = "watch.html?v=" + v.id;
  a.innerHTML = `
    <div class="thumb">
      ${v.thumb_url ? `<img src="${esc(v.thumb_url)}" alt="" loading="lazy">` : ""}
      ${v.duration ? `<span class="badge-dur">${fmtDur(v.duration)}</span>` : ""}
    </div>
    <h3>${esc(v.title)}</h3>
    <div class="meta">${fmtViews(v.view_count)} · ${timeAgo(v.published_at)}</div>`;
  return a;
}

function renderHeader(active) {
  document.title = SITE_NAME + (active && active !== "home" ? " — " + active : "");
  const logged = !!localStorage.getItem("token");
  const name = localStorage.getItem("name") || "?";
  $("header.top").innerHTML = `
    <div class="top-inner">
      <a class="logo" href="index.html">${esc(SITE_NAME.replace(/(Hub)$/, ""))}<span>Hub</span></a>
      <form class="search" id="searchForm">
        <input id="searchInput" type="search" placeholder="Search videos…" autocomplete="off">
      </form>
      <a class="navlink ${active === "library" ? "active" : ""}" href="library.html">Library</a>
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
  const ab = $("#avatarBtn");
  if (ab) ab.addEventListener("click", () => {
    if (confirm("Sign out of " + SITE_NAME + "?")) {
      localStorage.removeItem("token");
      localStorage.removeItem("name");
      location.reload();
    }
  });
}

function renderFooter() {
  const f = document.createElement("footer");
  f.innerHTML = `© ${new Date().getFullYear()} ${esc(SITE_NAME)} · Videos stream from a private library · <a href="admin.html" style="color:var(--muted)">Admin</a>`;
  document.body.appendChild(f);
}

function showEmpty(el, icon, title, sub) {
  el.innerHTML = `<div class="empty"><div class="big">${icon}</div><h3>${esc(title)}</h3><p>${esc(sub || "")}</p></div>`;
}
