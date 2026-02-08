import { findFirstFocusable, focusElement, scheduleSoon } from "open-sspm-app/dom_focus.js";

let sidebarStateObserver = null;
let sidebarEscapeHandler = null;
let sidebarResizeHandler = null;
let sidebarBackdropHandler = null;

export const wireSidebarToggle = (root = document) => {
  const sidebar = document.getElementById("app-sidebar");
  const sidebarToggle = root.querySelector("#sidebar-toggle");
  if (!(sidebar instanceof HTMLElement) || !(sidebarToggle instanceof HTMLElement)) return;
  if (sidebarToggle.dataset.sidebarToggleBound === "true") {
    return;
  }

  if (sidebarStateObserver instanceof MutationObserver) {
    sidebarStateObserver.disconnect();
  }
  if (typeof sidebarEscapeHandler === "function") {
    document.removeEventListener("keydown", sidebarEscapeHandler);
  }
  if (typeof sidebarResizeHandler === "function") {
    window.removeEventListener("resize", sidebarResizeHandler);
  }
  if (sidebarBackdropHandler?.element instanceof HTMLElement && typeof sidebarBackdropHandler.handler === "function") {
    sidebarBackdropHandler.element.removeEventListener("click", sidebarBackdropHandler.handler, true);
  }

  let moveFocusToSidebarOnOpen = false;
  let returnFocusToToggleOnClose = false;
  const mobileBreakpoint = Number.parseInt(sidebar.dataset.breakpoint || "", 10) || 768;

  const isMobileViewport = () => window.innerWidth < mobileBreakpoint;
  const isSidebarOpen = () => sidebar.getAttribute("aria-hidden") !== "true";

  const syncSidebarState = () => {
    const open = isSidebarOpen();
    sidebarToggle.setAttribute("aria-expanded", String(open));
    sidebarToggle.setAttribute("aria-label", open ? "Close navigation" : "Open navigation");

    const wasOpen = sidebar.dataset.lastKnownOpen === "true";
    sidebar.dataset.lastKnownOpen = String(open);

    if (!isMobileViewport()) {
      moveFocusToSidebarOnOpen = false;
      returnFocusToToggleOnClose = false;
      return;
    }

    if (open && !wasOpen && moveFocusToSidebarOnOpen) {
      const firstFocusable = findFirstFocusable(sidebar);
      if (firstFocusable) {
        focusElement(firstFocusable);
      }
    }

    if (!open && wasOpen && returnFocusToToggleOnClose) {
      focusElement(sidebarToggle);
    }

    moveFocusToSidebarOnOpen = false;
    returnFocusToToggleOnClose = false;
  };

  sidebarStateObserver = new MutationObserver(() => {
    syncSidebarState();
  });
  sidebarStateObserver.observe(sidebar, { attributes: true, attributeFilter: ["aria-hidden"] });

  sidebarToggle.addEventListener("click", () => {
    if (isMobileViewport()) {
      const currentlyOpen = isSidebarOpen();
      moveFocusToSidebarOnOpen = !currentlyOpen;
      returnFocusToToggleOnClose = currentlyOpen;
    }

    document.dispatchEvent(new CustomEvent("basecoat:sidebar", { detail: { id: "app-sidebar" } }));
  });

  const onSidebarClick = (event) => {
    if (!isMobileViewport()) return;
    if (!(event.target instanceof Element)) return;

    const clickedOutsideNav = event.target === sidebar;
    const clickedAction = Boolean(event.target.closest("a, button"));
    const keepSidebarOpen = Boolean(event.target.closest("[data-keep-mobile-sidebar-open]"));

    if (clickedOutsideNav || (clickedAction && !keepSidebarOpen)) {
      returnFocusToToggleOnClose = true;
    }
  };
  sidebar.addEventListener("click", onSidebarClick, true);
  sidebarBackdropHandler = { element: sidebar, handler: onSidebarClick };

  sidebarEscapeHandler = (event) => {
    if (event.key !== "Escape") return;
    if (event.defaultPrevented) return;
    if (!isMobileViewport()) return;
    if (!isSidebarOpen()) return;
    if (event.target instanceof Element && event.target.closest("dialog[open]")) return;

    returnFocusToToggleOnClose = true;
    document.dispatchEvent(
      new CustomEvent("basecoat:sidebar", {
        detail: {
          id: "app-sidebar",
          action: "close",
        },
      }),
    );
  };
  document.addEventListener("keydown", sidebarEscapeHandler);

  sidebarResizeHandler = () => {
    syncSidebarState();
  };
  window.addEventListener("resize", sidebarResizeHandler);

  syncSidebarState();
  scheduleSoon(syncSidebarState);

  sidebarToggle.dataset.sidebarToggleBound = "true";
};
