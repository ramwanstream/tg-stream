/* StreamHub ad placements — ExoClick (site ID 1117024, verified 2026-10-25).
 *
 * Slot codes render into <div class="adslot" data-ad="key"> elements.
 * Banner slots hold {wide, narrow} pairs: 728x90 on >=768px screens,
 * 300x50 below that (picked at render time, so only one ad is ever loaded).
 * Sitewide formats (Popunder + In-Page Push) are in GLOBAL_ADS and are
 * injected once per page — except inside Telegram, where the Monetag
 * Mini App ads (js/tma-ads.js) already cover that surface.
 * Leave a key empty ("") to hide that slot.
 */
const BANNER_WIDE = `<script async type="application/javascript" src="https://a.magsrv.com/ad-provider.js"></script>
<ins class="eas6a97888e2" data-zoneid="6065132"></ins>
<script>(AdProvider = window.AdProvider || []).push({"serve": {}});</script>`;
const BANNER_NARROW = `<script async type="application/javascript" src="https://a.magsrv.com/ad-provider.js"></script>
<ins class="eas6a97888e10" data-zoneid="6065134"></ins>
<script>(AdProvider = window.AdProvider || []).push({"serve": {}});</script>`;
const NATIVE_CODE = `<script async type="application/javascript" src="https://a.magsrv.com/ad-provider.js"></script>
<ins class="eas6a97888e20" data-zoneid="6065136"></ins>
<script>(AdProvider = window.AdProvider || []).push({"serve": {}});</script>`;

const ADS = {
  home_top: { wide: BANNER_WIDE, narrow: BANNER_NARROW },
  home_feed: NATIVE_CODE,
  watch_top: { wide: BANNER_WIDE, narrow: BANNER_NARROW },
  watch_mid: NATIVE_CODE,
  library_top: { wide: BANNER_WIDE, narrow: BANNER_NARROW },
};

const POPUNDER_CODE = `<script type="application/javascript">
    var ad_idzone = "6065128",
    ad_popup_fallback = false,
    ad_popup_force = false,
    ad_chrome_enabled = true,
    ad_new_tab = false,
    ad_frequency_period = 60,
    ad_frequency_count = 1,
    ad_trigger_method = 3,
    ad_trigger_delay = 0,
    ad_capping_enabled = true;
</script>
<script type="application/javascript" src="https://a.pemsrv.com/popunder1000.js"></script>`;

const INPAGE_PUSH_CODE = `<script async type="application/javascript" src="https://a.magsrv.com/ad-provider.js"></script>
<ins class="eas6a97888e42" data-zoneid="6065130"></ins>
<script>(AdProvider = window.AdProvider || []).push({"serve": {}});</script>`;

function injectAdHTML(container, code) {
  const tmp = document.createElement("div");
  tmp.innerHTML = code;
  container.appendChild(tmp);
  // Scripts inserted via innerHTML do not execute — re-create them in place.
  tmp.querySelectorAll("script").forEach(old => {
    const s = document.createElement("script");
    [...old.attributes].forEach(a => s.setAttribute(a.name, a.value));
    s.text = old.text;
    old.replaceWith(s);
  });
}

function renderAds(root = document) {
  root.querySelectorAll(".adslot").forEach(slot => {
    const key = slot.dataset.ad;
    let code = ADS[key];
    if (code && typeof code === "object") {
      code = window.innerWidth >= 768 ? code.wide : code.narrow;
    }
    if (!code || !code.trim()) { slot.style.display = "none"; return; }
    slot.style.display = "";
    slot.innerHTML = "";
    injectAdHTML(slot, code);
  });
}

function inTelegramApp() {
  return !!(window.Telegram && Telegram.WebApp && Telegram.WebApp.initData);
}

function renderGlobalAds() {
  if (window.__globalAdsDone) return;
  window.__globalAdsDone = true;
  if (inTelegramApp()) return; // Monetag TMA ads cover the Telegram surface
  // Popunder is scripts-only — safe in a hidden holder. The In-Page Push
  // <ins> must sit in the body flow (its ad renders as a fixed overlay).
  const holder = document.createElement("div");
  holder.style.display = "none";
  document.body.appendChild(holder);
  injectAdHTML(holder, POPUNDER_CODE);
  injectAdHTML(document.body, INPAGE_PUSH_CODE);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", renderGlobalAds);
} else {
  renderGlobalAds();
}
