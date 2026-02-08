import { captureFocusDescriptor, focusMainContent, restoreFocusAfterSwap, scheduleSoon } from "open-sspm-app/dom_focus.js";
import { initFragment, scheduleVisibleLazyHx, triggerVisibleLazyHx } from "open-sspm-app/fragment.js";
import { showFlashToast } from "open-sspm-app/toast.js";

const htmxRequestState = new WeakMap();
const busyElementCounts = new WeakMap();

const collectBusyElements = (detail) => {
  const elements = new Set();

  const addBusyElement = (element) => {
    if (!(element instanceof HTMLElement)) return;
    elements.add(element);

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

export const bindGlobalListenersOnce = (options = {}) => {
  const { initGlobal = null } = options;
  if (document.documentElement.dataset.openSspmAppListenersBound === "true") return;
  document.documentElement.dataset.openSspmAppListenersBound = "true";

  document.addEventListener("htmx:beforeRequest", (event) => {
    const detail = event.detail;
    if (!(detail?.xhr instanceof XMLHttpRequest)) return;

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
    if (event.key !== "/") return;
    if (["INPUT", "TEXTAREA"].includes(activeElement.tagName)) return;
    if (activeElement.isContentEditable) return;

    event.preventDefault();
    const searchInput = document.getElementById("command-search-input");
    if (searchInput instanceof HTMLElement) {
      searchInput.focus();
    }
  });

  document.addEventListener("keydown", (event) => {
    if (!(event.target instanceof Element)) return;
    if (!event.target.closest('[role="tab"]')) return;
    if (!["ArrowRight", "ArrowLeft", "Home", "End", "Enter", " "].includes(event.key)) return;
    scheduleVisibleLazyHx(document);
  });
};
