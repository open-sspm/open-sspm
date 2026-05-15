/**
 * Sidebar component
 *
 * Manages sidebar open/close state via aria-hidden, responds to osspm:sidebar
 * events, and handles mobile/desktop breakpoint behavior.
 */
import { register } from "./registry.js";

const init = (el) => {
  const doc = el.ownerDocument;
  const view = doc.defaultView;
  const resizeTarget =
    view &&
    typeof view.addEventListener === "function" &&
    typeof view.removeEventListener === "function"
      ? view
      : null;
  const nav = el.querySelector("nav");
  if (!nav) return;

  const breakpoint = Number.parseInt(el.dataset.breakpoint || "", 10) || 768;
  const isMobile = () =>
    (typeof resizeTarget?.innerWidth === "number"
      ? resizeTarget.innerWidth
      : window.innerWidth) < breakpoint;

  const initialMobileOpen = el.dataset.initialMobileOpen === "true";

  // Desktop open/closed precedence: client-persisted pref (data-sidebar-pref,
  // written from localStorage in layout.templ) wins, then the server-rendered
  // data-initial-open, then default to open.
  const resolveDesktopInitialOpen = () => {
    const pref = doc.documentElement.dataset.sidebarPref;
    if (pref === "closed") return false;
    if (pref === "open") return true;
    return el.dataset.initialOpen !== "false";
  };

  const syncInteractivity = () => {
    const open = el.getAttribute("aria-hidden") !== "true";
    nav.inert = !open;
  };

  const setOpen = (open) => {
    el.setAttribute("aria-hidden", String(!open));
    syncInteractivity();
  };

  // Set initial state
  if (isMobile()) {
    setOpen(initialMobileOpen);
  } else {
    setOpen(resolveDesktopInitialOpen());
  }

  // Listen for toggle/open/close events
  const handler = (event) => {
    const detail = event.detail || {};
    if (detail.id && detail.id !== el.id) return;

    const action = detail.action || "toggle";
    const isOpen = el.getAttribute("aria-hidden") !== "true";

    if (action === "open") setOpen(true);
    else if (action === "close") setOpen(false);
    else setOpen(!isOpen);
  };

  doc.addEventListener("osspm:sidebar", handler);

  // Keep inert in sync when the visible state changes across breakpoints.
  const onResize = () => {
    syncInteractivity();
  };
  if (resizeTarget && typeof resizeTarget.addEventListener === "function") {
    resizeTarget.addEventListener("resize", onResize);
  }

  return () => {
    doc.removeEventListener("osspm:sidebar", handler);
    if (resizeTarget && typeof resizeTarget.removeEventListener === "function") {
      resizeTarget.removeEventListener("resize", onResize);
    }
  };
};

register("sidebar", ".sidebar:not([data-sidebar-initialized])", init);
