import { captureFocusDescriptor, focusMainContent, restoreFocusAfterSwap, scheduleSoon } from "open-sspm-app/dom_focus.js";
import { initFragment, scheduleVisibleLazyHx, triggerVisibleLazyHx } from "open-sspm-app/fragment.js";
import { showFlashToast } from "open-sspm-app/toast.js";

const htmxRequestState = new WeakMap();
const busyElementCounts = new WeakMap();
const activeRequests = new Set();

const syncGlobalBusyIndicators = () => {
  const isBusy = activeRequests.size > 0;
  document.documentElement.dataset.htmxBusy = isBusy ? "true" : "false";
  document.querySelectorAll("[data-htmx-busy-indicator]").forEach((indicator) => {
    if (!(indicator instanceof HTMLElement)) return;
    indicator.hidden = !isBusy;
    indicator.setAttribute("aria-hidden", isBusy ? "false" : "true");
  });
};

const collectBusyElements = (detail) => {
  const elements = new Set();

  const addBusyElement = (element) => {
    if (!(element instanceof HTMLElement)) return;
    elements.add(element);

    const card = element.closest(".card");
    if (card instanceof HTMLElement) {
      elements.add(card);
    }

    const region = element.closest("[data-busy-region]");
    if (region instanceof HTMLElement) {
      elements.add(region);
    }
  };

  addBusyElement(detail?.target);
  addBusyElement(detail?.elt);

  if (detail?.requestConfig?.boosted) {
    const main = document.querySelector("[data-main-content], main");
    if (main instanceof HTMLElement) {
      elements.add(main);
    }
  }

  return Array.from(elements);
};

const setBusyState = (element, isBusy) => {
  if (!(element instanceof HTMLElement)) return;
  element.setAttribute("aria-busy", isBusy ? "true" : "false");
};

const incrementBusy = (element) => {
  const currentCount = busyElementCounts.get(element) || 0;
  busyElementCounts.set(element, currentCount + 1);
  setBusyState(element, true);
};

const decrementBusy = (element) => {
  const currentCount = busyElementCounts.get(element) || 0;
  if (currentCount <= 1) {
    busyElementCounts.delete(element);
    setBusyState(element, false);
    return;
  }

  busyElementCounts.set(element, currentCount - 1);
};

const isTextEntryElement = (element) => {
  if (!(element instanceof HTMLElement)) return false;
  if (element.isContentEditable) return true;
  return ["INPUT", "TEXTAREA", "SELECT"].includes(element.tagName);
};

const focusCommandSearchInput = () => {
  const searchInput = document.getElementById("command-search-input");
  if (!(searchInput instanceof HTMLElement)) return false;
  searchInput.focus();
  return document.activeElement === searchInput;
};

const finalizeRequestBusyState = (xhr) => {
  if (!(xhr instanceof XMLHttpRequest)) return null;

  activeRequests.delete(xhr);
  syncGlobalBusyIndicators();

  const state = htmxRequestState.get(xhr);
  if (!state || state.busyFinalized) {
    return state || null;
  }

  state.busyElements.forEach((element) => {
    decrementBusy(element);
  });
  state.busyFinalized = true;
  return state;
};

export const bindGlobalListenersOnce = (options = {}) => {
  const { initGlobal = null } = options;
  if (document.documentElement.dataset.openSspmAppListenersBound === "true") return;
  document.documentElement.dataset.openSspmAppListenersBound = "true";
  syncGlobalBusyIndicators();

  document.addEventListener("htmx:beforeRequest", (event) => {
    if (event.defaultPrevented) return;

    const detail = event.detail;
    if (!(detail?.xhr instanceof XMLHttpRequest)) return;

    activeRequests.add(detail.xhr);
    syncGlobalBusyIndicators();

    const busyElements = collectBusyElements(detail);
    busyElements.forEach((element) => {
      incrementBusy(element);
    });

    const target = detail.target instanceof HTMLElement ? detail.target : null;
    const active = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const state = {
      busyElements,
      focusStrategy: null,
      focusDescriptor: null,
      busyFinalized: false,
    };

    const targetsPageRoot = target === document.body || target === document.documentElement;
    if (targetsPageRoot || detail.requestConfig?.boosted) {
      state.focusStrategy = "main";
    } else if (target && active && target.contains(active)) {
      state.focusStrategy = "restore";
      state.focusDescriptor = captureFocusDescriptor(active);
    }

    htmxRequestState.set(detail.xhr, state);
  });

  document.addEventListener("htmx:afterRequest", (event) => {
    const detail = event.detail;
    const state = finalizeRequestBusyState(detail?.xhr);
    if (!state) return;

    // Keep focus metadata until afterSwap reads it.
    const noSwapExpected = detail.failed === true || detail.xhr.status === 0 || detail.xhr.status === 204;
    if (noSwapExpected || !state.focusStrategy) {
      htmxRequestState.delete(detail.xhr);
    }
  });

  document.addEventListener("htmx:afterSwap", (event) => {
    if (!(event.target instanceof HTMLElement)) return;

    const xhr = event.detail?.xhr instanceof XMLHttpRequest ? event.detail.xhr : null;
    const target = event.target;
    const requestState = xhr ? htmxRequestState.get(xhr) : null;

    try {
      initFragment(target);

      if (target === document.body || target === document.documentElement) {
        if (typeof initGlobal === "function") {
          initGlobal();
        }
        initFragment(document);
      }

      if (target.querySelector("#flash-toast")) {
        showFlashToast();
      }

      if (requestState?.focusStrategy === "main") {
        scheduleSoon(focusMainContent);
      } else if (requestState?.focusStrategy === "restore") {
        restoreFocusAfterSwap(target, requestState.focusDescriptor);
      }

      triggerVisibleLazyHx(document);
    } finally {
      if (xhr) {
        // Fallback cleanup for lifecycles that skip afterRequest (e.g. cancelled/edge cases).
        finalizeRequestBusyState(xhr);
        htmxRequestState.delete(xhr);
      }
    }
  });

  const failSafeFinalize = (event) => {
    const xhr = event.detail?.xhr;
    if (!(xhr instanceof XMLHttpRequest)) return;
    finalizeRequestBusyState(xhr);
    htmxRequestState.delete(xhr);
  };
  document.addEventListener("htmx:sendAbort", failSafeFinalize);
  document.addEventListener("htmx:sendError", failSafeFinalize);
  document.addEventListener("htmx:timeout", failSafeFinalize);
  document.addEventListener("htmx:responseError", failSafeFinalize);

  document.addEventListener("htmx:load", (event) => {
    if (event.target instanceof HTMLElement) {
      initFragment(event.target);
    }
  });

  document.addEventListener("click", (event) => {
    if (!(event.target instanceof Element)) return;
    if (!event.target.closest('[role="tab"]')) return;
    scheduleVisibleLazyHx(document);
  });

  document.addEventListener("keydown", (event) => {
    const activeElement = document.activeElement;
    if (!(activeElement instanceof HTMLElement)) return;

    const isSlashShortcut = event.key === "/" && !event.metaKey && !event.ctrlKey && !event.altKey;
    const isCommandShortcut =
      event.key.toLowerCase() === "k" &&
      (event.metaKey || event.ctrlKey) &&
      !event.altKey &&
      !event.shiftKey;
    if (!isSlashShortcut && !isCommandShortcut) return;
    if (isSlashShortcut && isTextEntryElement(activeElement)) return;
    if (isCommandShortcut && isTextEntryElement(activeElement) && activeElement.id !== "command-search-input") return;
    if (!focusCommandSearchInput()) return;

    event.preventDefault();
  });

  document.addEventListener("keydown", (event) => {
    if (!(event.target instanceof Element)) return;
    if (!event.target.closest('[role="tab"]')) return;
    if (!["ArrowRight", "ArrowLeft", "Home", "End", "Enter", " "].includes(event.key)) return;
    scheduleVisibleLazyHx(document);
  });
};
