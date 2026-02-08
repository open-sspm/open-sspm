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

export const bindGlobalListenersOnce = (options = {}) => {
  const { initGlobal = null } = options;
  if (document.documentElement.dataset.openSspmAppListenersBound === "true") return;
  document.documentElement.dataset.openSspmAppListenersBound = "true";
  syncGlobalBusyIndicators();

  document.addEventListener("htmx:beforeRequest", (event) => {
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
    if (!(detail?.xhr instanceof XMLHttpRequest)) return;

    activeRequests.delete(detail.xhr);
    syncGlobalBusyIndicators();

    const state = htmxRequestState.get(detail.xhr);
    if (!state) return;

    state.busyElements.forEach((element) => {
      decrementBusy(element);
    });

    // Keep focus metadata until afterSwap reads it.
    const noSwapExpected = detail.failed === true || detail.xhr.status === 0 || detail.xhr.status === 204;
    if (noSwapExpected || !state.focusStrategy) {
      htmxRequestState.delete(detail.xhr);
    }
  });

  document.addEventListener("htmx:afterSwap", (event) => {
    if (!(event.target instanceof HTMLElement)) return;

    initFragment(event.target);

    const xhr = event.detail?.xhr instanceof XMLHttpRequest ? event.detail.xhr : null;
    const requestState = xhr ? htmxRequestState.get(xhr) : null;

    if (event.target === document.body || event.target === document.documentElement) {
      if (typeof initGlobal === "function") {
        initGlobal();
      }
      initFragment(document);
    }

    if (event.target.querySelector("#flash-toast")) {
      showFlashToast();
    }

    if (requestState?.focusStrategy === "main") {
      scheduleSoon(focusMainContent);
    } else if (requestState?.focusStrategy === "restore") {
      restoreFocusAfterSwap(event.target, requestState.focusDescriptor);
    }

    triggerVisibleLazyHx(document);

    if (xhr) {
      htmxRequestState.delete(xhr);
    }
  });

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
