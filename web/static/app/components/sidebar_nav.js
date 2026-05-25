/**
 * Sidebar nav active-state component.
 *
 * The sidebar is application chrome that persists across boosted navigations
 * (the body's hx-target is #main). Active link/section state is therefore
 * URL-derived on the client, not re-rendered by the server on each navigation.
 *
 * Reads declarative hints from the markup:
 *   <a href="..." [data-active-match="exact"]>     → link is active when href
 *                                                    matches location.pathname
 *                                                    (prefix by default, mirrors
 *                                                    server IsActivePath)
 *   <details data-active-prefixes="/a /b">         → details auto-opens and its
 *                                                    summary is marked active
 *                                                    when any prefix matches
 *
 * Sections auto-open when the user navigates INTO them; user-driven open state
 * is preserved otherwise (deliberate departure from the prior re-render-each-nav
 * behavior, which made it impossible to keep a section expanded while browsing
 * elsewhere).
 */
import { register } from "./registry.js";

const trim = (s) => (s || "").trim();

// Mirrors helpers.go IsActivePath: target "/" is exact-match-only; otherwise
// HasPrefix. The Go side accepts naked prefix matches (e.g. /set would match
// /settings), so we mirror that for byte-for-byte parity on the initial render.
const isPrefixActive = (path, target) => {
  const a = trim(path);
  const t = trim(target);
  if (t === "/") return a === "/";
  return a.startsWith(t);
};

const isExactActive = (path, target) => trim(path) === trim(target);

const splitPrefixes = (raw) => trim(raw).split(/\s+/).filter(Boolean);

const setAriaCurrent = (el, active) => {
  if (active) {
    el.setAttribute("aria-current", "page");
  } else {
    el.removeAttribute("aria-current");
  }
};

const applyActiveState = (root, path) => {
  root.querySelectorAll("a[href]").forEach((link) => {
    const href = link.getAttribute("href") || "";
    const exact = link.dataset.activeMatch === "exact";
    const active = exact ? isExactActive(path, href) : isPrefixActive(path, href);
    setAriaCurrent(link, active);
  });

  root.querySelectorAll("details[data-active-prefixes]").forEach((details) => {
    const prefixes = splitPrefixes(details.dataset.activePrefixes);
    const active = prefixes.some((p) => isPrefixActive(path, p));

    const summary = details.querySelector(":scope > summary");
    if (summary) setAriaCurrent(summary, active);

    if (active && !details.open) details.open = true;
  });
};

const init = (el) => {
  const doc = el.ownerDocument;
  const view = doc.defaultView;
  const viewTarget =
    view &&
    typeof view.addEventListener === "function" &&
    typeof view.removeEventListener === "function"
      ? view
      : null;
  const loc = view?.location || location;
  let lastPath = loc.pathname;
  const refresh = () => {
    lastPath = loc.pathname;
    applyActiveState(el, lastPath);
  };

  // Server already rendered correct initial state, but reapply defensively
  // in case the markup was hydrated outside a fresh page load.
  refresh();

  const maybeRefresh = () => {
    if (loc.pathname !== lastPath) refresh();
  };

  doc.addEventListener("htmx:afterSettle", maybeRefresh);
  viewTarget?.addEventListener("popstate", maybeRefresh);

  return () => {
    if (typeof doc.removeEventListener === "function") {
      doc.removeEventListener("htmx:afterSettle", maybeRefresh);
    }
    if (typeof viewTarget?.removeEventListener === "function") {
      viewTarget.removeEventListener("popstate", maybeRefresh);
    }
  };
};

register("sidebar-nav", "[data-sidebar-nav]:not([data-sidebar-nav-initialized])", init);
