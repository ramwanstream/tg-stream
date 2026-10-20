/* StreamHub ad placements (Adsterra).
 *
 * HOW TO USE: after Adsterra approves the site, create one ad unit per
 * placement in the Adsterra dashboard, copy each unit's <script> snippet,
 * and paste it as the value for the matching key below (inside backticks).
 * Leave a key empty ("") to hide that slot. Changes go live on next deploy.
 *
 * Suggested Adsterra formats per slot:
 *   home_top    → 728x90 Leaderboard (or 320x50 on mobile — Adsterra auto-picks)
 *   home_feed   → Native banner
 *   watch_top   → 728x90 Leaderboard / 336x280
 *   watch_mid   → Native banner
 *   library_top → 728x90 Leaderboard
 */
const ADS = {
  home_top: ``,
  home_feed: ``,
  watch_top: ``,
  watch_mid: ``,
  library_top: ``,
};

/* Injects the configured snippet into every <div class="adslot" data-ad="key">. */
function renderAds(root = document) {
  root.querySelectorAll(".adslot").forEach(slot => {
    const key = slot.dataset.ad;
    const code = ADS[key];
    if (!code || !code.trim()) { slot.style.display = "none"; return; }
    slot.style.display = "";
    // document.write-free injection: scripts in the snippet must execute,
    // so insert via a container and re-create script tags.
    const tmp = document.createElement("div");
    tmp.innerHTML = code;
    slot.innerHTML = "";
    slot.appendChild(tmp);
    tmp.querySelectorAll("script").forEach(old => {
      const s = document.createElement("script");
      [...old.attributes].forEach(a => s.setAttribute(a.name, a.value));
      s.text = old.text;
      old.replaceWith(s);
    });
  });
}
