(() => {
  const state = {
    healthTimer: null,
    healthEndpoint: "/api/health",
  };

  function ensureContainer(id, className) {
    let node = document.getElementById(id);
    if (!node) {
      node = document.createElement("div");
      node.id = id;
      if (className) node.className = className;
      document.body.prepend(node);
    }
    return node;
  }

  function renderBreadcrumb() {
    const node = ensureContainer("global-breadcrumb", "status");
    node.style.padding = "8px 16px";
    node.style.borderBottom = "1px solid rgba(148,163,184,0.2)";
    node.style.background = "rgba(10,15,26,0.7)";
    const path = window.location.pathname || "/";
    const page = path === "/" ? "Home" : path.replace("/", "").replace(/\?.*$/, "");
    node.textContent = `Alexandria > ${page}`;
  }

  function renderHealthDot(ok, reason) {
    const nav = document.querySelector(".nav");
    if (!nav) return;
    let wrap = document.getElementById("global-health");
    if (!wrap) {
      wrap = document.createElement("div");
      wrap.id = "global-health";
      wrap.style.marginLeft = "auto";
      wrap.style.display = "inline-flex";
      wrap.style.alignItems = "center";
      wrap.style.gap = "8px";
      wrap.style.fontSize = "12px";
      wrap.className = "status";
      nav.appendChild(wrap);
    }
    const dotColor = ok ? "var(--green-500)" : "var(--red-500)";
    wrap.innerHTML = `<span aria-hidden="true" style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${dotColor};"></span><span>${ok ? "Healthy" : "Degraded"}</span>`;
    if (reason) wrap.title = reason;
  }

  function renderStartupBanner(healthy, issues, warnings) {
    const node = ensureContainer("global-startup-health", "status");
    node.style.padding = "8px 16px";
    node.style.background = "rgba(127, 29, 29, 0.92)";
    node.style.borderBottom = "1px solid rgba(248, 113, 113, 0.55)";
    node.style.color = "#fecaca";
    node.style.display = "none";
    node.style.zIndex = "3000";
    if (healthy) {
      node.style.display = "none";
      node.textContent = "";
      return;
    }

    const issueList = Array.isArray(issues) ? issues : [];
    const warningList = Array.isArray(warnings) ? warnings : [];
    const issueText = issueList.length ? issueList.slice(0, 2).join(" | ") : "Startup health checks failed";
    const warningText = warningList.length ? ` Warnings: ${warningList.slice(0, 2).join(" | ")}` : "";
    node.textContent = `${issueText}.${warningText}`.trim();
    node.style.display = "block";
  }

  async function refreshHealth() {
    try {
      const response = await fetch(state.healthEndpoint, { cache: "no-store" });
      if (!response.ok) {
        renderHealthDot(false, `HTTP ${response.status}`);
        renderStartupBanner(false, [`Health endpoint returned HTTP ${response.status}`], []);
        return;
      }
      const payload = await response.json();
      const providers = payload.providers || {};
      const inactive = Object.values(providers).filter((row) => row && row.status !== "active").length;
      const startup = payload.startup_checks || {};
      const issues = Array.isArray(startup.issues) ? startup.issues : [];
      const warnings = Array.isArray(startup.warnings) ? startup.warnings : [];
      const startupHealthy = payload.healthy !== false;
      const healthy = startupHealthy && inactive === 0;
      const reasons = [];
      if (!startupHealthy && issues.length) reasons.push(`${issues.length} startup issue(s)`);
      if (inactive > 0) reasons.push(`${inactive} provider(s) inactive`);
      if (warnings.length) reasons.push(`${warnings.length} startup warning(s)`);
      renderHealthDot(healthy, reasons.join(" | "));
      renderStartupBanner(startupHealthy, issues, warnings);
    } catch (error) {
      renderHealthDot(false, String(error && error.message || error));
      renderStartupBanner(false, [String(error && error.message || error)], []);
    }
  }

  function installCommandPaletteHint() {
    const node = ensureContainer("global-command-hint", "status");
    node.style.position = "fixed";
    node.style.bottom = "10px";
    node.style.right = "12px";
    node.style.padding = "6px 10px";
    node.style.background = "rgba(10,15,26,0.85)";
    node.style.border = "1px solid rgba(148,163,184,0.2)";
    node.style.borderRadius = "8px";
    node.textContent = "Ctrl+K: quick command";

    function handleKeydown(event) {
      if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        const command = window.prompt("Quick command: go iterate/review/batch/catalogs/jobs/compare/history/dashboard/similarity/mockups");
        if (!command) return;
        const token = command.trim().toLowerCase();
        const map = {
          iterate: "/iterate",
          review: "/review",
          batch: "/batch",
          catalog: "/catalogs",
          catalogs: "/catalogs",
          jobs: "/jobs",
          compare: "/compare",
          speed: "/review?mode=speed",
          history: "/history",
          dashboard: "/dashboard",
          similarity: "/similarity",
          mockups: "/mockups",
          docs: "/api/docs",
        };
        if (map[token]) window.location.href = map[token];
      }
    }
    window.addEventListener("keydown", handleKeydown);
  }

  function applyNavAccessibility() {
    document.querySelectorAll(".nav a").forEach((link) => {
      if (!link.getAttribute("aria-label")) {
        link.setAttribute("aria-label", `Go to ${link.textContent.trim() || "page"}`);
      }
    });
    document.querySelectorAll("button").forEach((button) => {
      if (!button.getAttribute("aria-label")) {
        const label = button.textContent.trim();
        if (label) button.setAttribute("aria-label", label);
      }
    });
    document.querySelectorAll("img").forEach((img) => {
      const alt = img.getAttribute("alt");
      if (!alt || !alt.trim()) {
        img.setAttribute("alt", "Cover image");
      }
      if (!img.hasAttribute("loading") && !img.dataset.noLazy) {
        img.setAttribute("loading", "lazy");
      }
    });
  }

  function installConnectivityBanner() {
    const node = ensureContainer("global-connectivity", "status");
    node.style.position = "fixed";
    node.style.top = "10px";
    node.style.right = "12px";
    node.style.padding = "6px 10px";
    node.style.borderRadius = "8px";
    node.style.border = "1px solid rgba(148,163,184,0.2)";
    node.style.background = "rgba(10,15,26,0.92)";
    node.style.display = "none";
    node.style.zIndex = "4000";

    const setState = () => {
      if (navigator.onLine) {
        node.style.display = "none";
        node.textContent = "";
      } else {
        node.style.display = "block";
        node.textContent = "Backend/network appears offline";
      }
    };

    window.addEventListener("online", setState);
    window.addEventListener("offline", setState);
    setState();
  }

  function installModalA11yHelpers() {
    function isVisible(node) {
      if (!node) return false;
      const style = window.getComputedStyle(node);
      return style.display !== "none" && style.visibility !== "hidden";
    }

    function activeModal() {
      const candidates = Array.from(document.querySelectorAll("[id$='Modal']"));
      return candidates.find((node) => isVisible(node)) || null;
    }

    function focusableWithin(node) {
      if (!node) return [];
      const selectors = [
        "a[href]",
        "button:not([disabled])",
        "input:not([disabled])",
        "select:not([disabled])",
        "textarea:not([disabled])",
        "[tabindex]:not([tabindex='-1'])",
      ];
      return Array.from(node.querySelectorAll(selectors.join(","))).filter((el) => {
        const style = window.getComputedStyle(el);
        return style.display !== "none" && style.visibility !== "hidden";
      });
    }

    document.addEventListener("keydown", (event) => {
      const modal = activeModal();
      if (!modal) return;

      if (event.key === "Escape") {
        event.preventDefault();
        modal.style.display = "none";
        return;
      }

      if (event.key !== "Tab") return;
      const items = focusableWithin(modal);
      if (!items.length) return;
      const first = items[0];
      const last = items[items.length - 1];
      const active = document.activeElement;

      if (event.shiftKey && (active === first || active === modal)) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && active === last) {
        event.preventDefault();
        first.focus();
      }
    });
  }

  function start() {
    renderBreadcrumb();
    applyNavAccessibility();
    installConnectivityBanner();
    installModalA11yHelpers();
    installCommandPaletteHint();
    refreshHealth();
    if (state.healthTimer) window.clearInterval(state.healthTimer);
    state.healthTimer = window.setInterval(refreshHealth, 30000);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
