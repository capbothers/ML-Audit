/**
 * Privacy-Safe Site Health Tracker
 *
 * Captures JS errors, Core Web Vitals, slow resources, and long tasks.
 * No cookies. No PII. Session ID is random and per-tab only.
 *
 * Embed in Shopify theme.liquid:
 *   <script src="https://YOUR_BACKEND/static/site-health.js" defer></script>
 */
(function () {
  "use strict";

  // ── Config ──────────────────────────────────────────────────────────
  var BATCH_SIZE = 10;
  var BATCH_TIMEOUT = 5000; // ms
  var SLOW_RESOURCE_MS = 2000;
  var LONG_TASK_MS = 50;

  // Resolve API endpoint: same origin as the script tag, or current page origin
  var scriptEl = document.currentScript;
  var apiBase = "";
  if (scriptEl && scriptEl.src) {
    try {
      var u = new URL(scriptEl.src);
      apiBase = u.origin;
    } catch (_) {}
  }
  var ENDPOINT = apiBase + "/site-health/track";

  // ── State ───────────────────────────────────────────────────────────
  var queue = [];
  var timer = null;
  var sid = sessionId();
  var device = deviceType();

  // ── Helpers ─────────────────────────────────────────────────────────
  function sessionId() {
    try {
      var id = sessionStorage.getItem("_sh_sid");
      if (!id) {
        id =
          Math.random().toString(36).slice(2) +
          Math.random().toString(36).slice(2);
        sessionStorage.setItem("_sh_sid", id);
      }
      return id;
    } catch (_) {
      return Math.random().toString(36).slice(2);
    }
  }

  function deviceType() {
    var w = window.innerWidth || 0;
    if (w < 768) return "mobile";
    if (w < 1024) return "tablet";
    return "desktop";
  }

  function rate(metric, value) {
    var t = {
      LCP: [2500, 4000],
      CLS: [0.1, 0.25],
      INP: [200, 500],
      TTFB: [800, 1800],
    };
    var bounds = t[metric];
    if (!bounds) return "unknown";
    if (value <= bounds[0]) return "good";
    if (value <= bounds[1]) return "needs-improvement";
    return "poor";
  }

  // ── Queue & send ───────────────────────────────────────────────────
  function enqueue(data) {
    data.page_url = location.href;
    data.session_id = sid;
    data.device_type = device;
    data.client_timestamp = new Date().toISOString();
    data.viewport_width = window.innerWidth;
    data.viewport_height = window.innerHeight;
    data.user_agent = navigator.userAgent;
    queue.push(data);

    if (queue.length >= BATCH_SIZE) {
      flush();
    } else {
      if (timer) clearTimeout(timer);
      timer = setTimeout(flush, BATCH_TIMEOUT);
    }
  }

  function flush() {
    if (!queue.length) return;
    var events = queue.splice(0);
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    var body = JSON.stringify({ events: events });
    if (navigator.sendBeacon) {
      navigator.sendBeacon(ENDPOINT, new Blob([body], { type: "application/json" }));
    } else {
      try {
        fetch(ENDPOINT, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: body,
          keepalive: true,
        });
      } catch (_) {}
    }
  }

  // ── Error tracking ─────────────────────────────────────────────────
  window.addEventListener("error", function (e) {
    enqueue({
      event_type: "error",
      error_message: e.message || "Unknown error",
      error_type: (e.error && e.error.name) || "Error",
      error_stack: (e.error && e.error.stack) || null,
      error_source_file: e.filename || null,
      error_line_number: e.lineno || null,
      error_column_number: e.colno || null,
      is_unhandled_rejection: false,
    });
  });

  window.addEventListener("unhandledrejection", function (e) {
    var reason = e.reason || {};
    enqueue({
      event_type: "error",
      error_message: reason.message || String(e.reason) || "Unhandled rejection",
      error_type: reason.name || "UnhandledRejection",
      error_stack: reason.stack || null,
      is_unhandled_rejection: true,
    });
  });

  // ── Core Web Vitals ────────────────────────────────────────────────
  if (typeof PerformanceObserver !== "undefined") {
    // LCP
    try {
      new PerformanceObserver(function (list) {
        var entries = list.getEntries();
        var last = entries[entries.length - 1];
        var val = last.renderTime || last.loadTime;
        enqueue({
          event_type: "web_vital",
          metric_name: "LCP",
          metric_value: val,
          metric_rating: rate("LCP", val),
        });
      }).observe({ type: "largest-contentful-paint", buffered: true });
    } catch (_) {}

    // CLS (cumulative, reported on pagehide)
    try {
      var clsValue = 0;
      new PerformanceObserver(function (list) {
        list.getEntries().forEach(function (e) {
          if (!e.hadRecentInput) clsValue += e.value;
        });
      }).observe({ type: "layout-shift", buffered: true });
      document.addEventListener("visibilitychange", function () {
        if (document.visibilityState === "hidden" && clsValue > 0) {
          enqueue({
            event_type: "web_vital",
            metric_name: "CLS",
            metric_value: clsValue,
            metric_rating: rate("CLS", clsValue),
          });
        }
      });
    } catch (_) {}

    // INP (longest event processing delay)
    try {
      var maxINP = 0;
      new PerformanceObserver(function (list) {
        list.getEntries().forEach(function (e) {
          var dur = e.duration;
          if (dur > maxINP) maxINP = dur;
        });
      }).observe({ type: "event", buffered: true, durationThreshold: 16 });
      document.addEventListener("visibilitychange", function () {
        if (document.visibilityState === "hidden" && maxINP > 0) {
          enqueue({
            event_type: "web_vital",
            metric_name: "INP",
            metric_value: maxINP,
            metric_rating: rate("INP", maxINP),
          });
        }
      });
    } catch (_) {}

    // Slow resources (> 2 s)
    try {
      new PerformanceObserver(function (list) {
        list.getEntries().forEach(function (e) {
          if (e.duration > SLOW_RESOURCE_MS) {
            enqueue({
              event_type: "slow_resource",
              resource_url: e.name,
              resource_type: e.initiatorType,
              resource_duration: e.duration,
              resource_transfer_size: e.transferSize || 0,
            });
          }
        });
      }).observe({ type: "resource", buffered: true });
    } catch (_) {}

    // Long tasks (> 50 ms)
    try {
      new PerformanceObserver(function (list) {
        list.getEntries().forEach(function (e) {
          if (e.duration > LONG_TASK_MS) {
            enqueue({
              event_type: "long_task",
              task_duration: e.duration,
              task_attribution:
                (e.attribution && e.attribution[0] && e.attribution[0].name) ||
                "unknown",
            });
          }
        });
      }).observe({ type: "longtask", buffered: true });
    } catch (_) {}
  }

  // TTFB (from navigation timing)
  try {
    var nav = performance.getEntriesByType("navigation")[0];
    if (nav) {
      var ttfb = nav.responseStart - nav.requestStart;
      if (ttfb > 0) {
        enqueue({
          event_type: "web_vital",
          metric_name: "TTFB",
          metric_value: ttfb,
          metric_rating: rate("TTFB", ttfb),
          metric_navigation_type: nav.type,
        });
      }
    }
  } catch (_) {}

  // Flush on page hide
  window.addEventListener("pagehide", flush);
  document.addEventListener("visibilitychange", function () {
    if (document.visibilityState === "hidden") flush();
  });
})();
