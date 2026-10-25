// Monetag Telegram Mini App ads (zone 12119804).
// Runs ONLY inside Telegram (WebApp context) — normal browser visits are untouched.
(function () {
  function inTelegram() {
    return !!(window.Telegram && Telegram.WebApp && Telegram.WebApp.initData);
  }
  function start() {
    if (!inTelegram()) return;
    if (typeof window.show_12119804 !== "function") return;
    try {
      window.show_12119804({
        type: "inApp",
        inAppSettings: {
          frequency: 2,   // max 2 ads per capping window
          capping: 0.1,   // 0.1 hours = 6 minutes
          interval: 30,   // 30s between ads
          timeout: 5,     // first ad after 5s
          everyPage: false
        }
      });
    } catch (e) { /* ads are best-effort */ }
  }
  // The SDK tag sits in <head>; poll briefly until its show_ function is ready.
  var tries = 0;
  var timer = setInterval(function () {
    tries++;
    if (typeof window.show_12119804 === "function") {
      clearInterval(timer);
      start();
    } else if (tries >= 24) {
      clearInterval(timer);
    }
  }, 500);
})();
