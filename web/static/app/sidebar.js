import { findFirstFocusable, focusElement, scheduleSoon } from "open-sspm-app/dom_focus.js";

const SIDEBAR_ID = "app-sidebar";
const DESKTOP_SIDEBAR_STATE_KEY = "openSspm.sidebar.desktopOpen";

let sidebarStateObserver = null;
let sidebarEscapeHandler = null;
let sidebarResizeHandler = null;
let sidebarBackdropHandler = null;

const readDesktopSidebarPreference = () => {
  try {
    const stored = localStorage.getItem(DESKTOP_SIDEBAR_STATE_KEY);
    if (stored === "true") return true;
    if (stored === "false") return false;
  } catch (_) {
    return null;
  }
  return null;
};

const writeDesktopSidebarPreference = (open) => {
  try {
    localStorage.setItem(DESKTOP_SIDEBAR_STATE_KEY, String(open));
  } catch (_) {}
};

const dispatchSidebarEvent = (detail = {}) => {
  document.dispatchEvent(
    new CustomEvent("basecoat:sidebar", {
      detail: {
        id: SIDEBAR_ID,
        ...detail,
      },
    }),
  );
};

export const wireSidebarToggle = (root = document) => {
  const sidebar = document.getElementById(SIDEBAR_ID);
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
  let desktopPersistenceReady = false;
  let desktopOpenPreference = readDesktopSidebarPreference();
  const mobileBreakpoint = Number.parseInt(sidebar.dataset.breakpoint || "", 10) || 768;
  let wasMobileViewport = window.innerWidth < mobileBreakpoint;

  const isMobileViewport = () => window.innerWidth < mobileBreakpoint;
  const isSidebarOpen = () => sidebar.getAttribute("aria-hidden") !== "true";
  const isBasecoatSidebarReady = () => sidebar.dataset.sidebarInitialized === "true";

  const persistDesktopPreference = (open) => {
    if (!desktopPersistenceReady) return;
    if (isMobileViewport()) return;
    if (desktopOpenPreference === open) return;
    desktopOpenPreference = open;
    writeDesktopSidebarPreference(open);
  };

  const applyDesktopPreference = () => {
    if (isMobileViewport()) return true;
    if (desktopOpenPreference === null) return true;
    if (isSidebarOpen() === desktopOpenPreference) return true;
    if (!isBasecoatSidebarReady()) return false;

    dispatchSidebarEvent({ action: desktopOpenPreference ? "open" : "close" });
    return true;
  };

  const initializeDesktopPersistence = () => {
    if (desktopPersistenceReady) return;
    if (isMobileViewport()) return;
    if (!isBasecoatSidebarReady()) return;

    const applied = applyDesktopPreference();
    if (!applied) return;

    desktopPersistenceReady = true;
    persistDesktopPreference(isSidebarOpen());
    syncSidebarState();
  };

  const syncSidebarState = () => {
    const open = isSidebarOpen();
    sidebarToggle.setAttribute("aria-expanded", String(open));
    sidebarToggle.setAttribute("aria-label", open ? "Close navigation" : "Open navigation");

    const wasOpen = sidebar.dataset.lastKnownOpen === "true";
    sidebar.dataset.lastKnownOpen = String(open);

    if (!isMobileViewport()) {
      persistDesktopPreference(open);
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

    dispatchSidebarEvent();
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
    dispatchSidebarEvent({ action: "close" });
  };
  document.addEventListener("keydown", sidebarEscapeHandler);

  sidebarResizeHandler = () => {
    const isMobile = isMobileViewport();
    if (wasMobileViewport && !isMobile) {
      if (desktopPersistenceReady) {
        applyDesktopPreference();
      } else {
        initializeDesktopPersistence();
      }
    }
    wasMobileViewport = isMobile;
    syncSidebarState();
  };
  window.addEventListener("resize", sidebarResizeHandler);

  if (sidebar.dataset.sidebarInitialized !== "true") {
    sidebar.addEventListener("basecoat:initialized", initializeDesktopPersistence, { once: true });
  }

  initializeDesktopPersistence();
  scheduleSoon(initializeDesktopPersistence);
  syncSidebarState();
  scheduleSoon(syncSidebarState);

  sidebarToggle.dataset.sidebarToggleBound = "true";
};
