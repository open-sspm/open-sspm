/**
 * Sidebar component
 *
 * Manages sidebar open/close state via aria-hidden, responds to osspm:sidebar
 * events, and handles mobile/desktop breakpoint behavior.
 */
import { register } from "./registry.js";

const init = (el) => {
  const nav = el.querySelector("nav");
  if (!nav) return;

  const breakpoint = Number.parseInt(el.dataset.breakpoint || "", 10) || 768;
  const isMobile = () => window.innerWidth < breakpoint;

  const initialOpen = el.dataset.initialOpen !== "false";
  const initialMobileOpen = el.dataset.initialMobileOpen === "true";

  const setOpen = (open) => {
    el.setAttribute("aria-hidden", String(!open));
    if (isMobile()) {
      nav.inert = !open;
    } else {
      nav.inert = false;
    }
  };

  // Set initial state
  if (isMobile()) {
    setOpen(initialMobileOpen);
  } else {
    setOpen(initialOpen);
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

  document.addEventListener("osspm:sidebar", handler);

  // Handle resize: update inert state
  window.addEventListener("resize", () => {
    const isOpen = el.getAttribute("aria-hidden") !== "true";
    if (isMobile()) {
      nav.inert = !isOpen;
    } else {
      nav.inert = false;
    }
  });
};

register("sidebar", ".sidebar:not([data-sidebar-initialized])", init);
