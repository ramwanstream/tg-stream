/* Site settings — the admin can rename the site in Admin → Settings;
   the public name is fetched from /api/settings and cached for instant paint. */
const SITE_NAME = localStorage.getItem("site_name") || "StreamHub";
const UPLOADS_ON = localStorage.getItem("uploads_enabled") !== "0";
const API = ""; // same origin as the backend (empty = this site). Change only if API is hosted separately.

fetch("/api/settings").then(r => r.json()).then(s => {
  if (s.site_name) localStorage.setItem("site_name", s.site_name);
  localStorage.setItem("uploads_enabled", s.uploads_enabled ? "1" : "0");
  localStorage.setItem("announcement", s.announcement || "");
}).catch(() => {});
