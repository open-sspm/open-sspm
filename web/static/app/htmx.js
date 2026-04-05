import { captureFocusDescriptor, focusMainContent, restoreFocusAfterSwap, scheduleSoon } from "open-sspm-app/dom_focus.js";
import {
  clearLazyHxPending,
  hasLazyHxError,
  initFragment,
  isLazyHxLoaded,
  isLazyHxPending,
  isManagedLazyHx,
  markLazyHxLoaded,
  markLazyHxPending,
  scheduleVisibleLazyHx,
  triggerVisibleLazyHx,
} from "open-sspm-app/fragment.js";
import { showFlashToast } from "open-sspm-app/toast.js";

const htmxRequestState = new WeakMap();
const busyElementCounts = new WeakMap();
const activeRequests = new Set();

const isRequestHandle = (value) => value !== null && (typeof value === "object" || typeof value === "function");

const resolveEnterOnlyQueryInput = (detail) => {
  const element = detail?.elt instanceof Element ? detail.elt : null;
  const form = element?.closest("form[data-enter-only-query]");
  if (!(form instanceof HTMLFormElement)) return null;

  const queryParam = (form.dataset.enterOnlyQuery || "").trim();
  if (!queryParam) return null;

  const input = form.elements.namedItem(queryParam);
  if (input instanceof HTMLInputElement || input instanceof HTMLTextAreaElement) {
    return input;
  }

  if (typeof RadioNodeList !== "undefined" && input instanceof RadioNodeList) {
    const first = input.item(0);
    if (first instanceof HTMLInputElement || first instanceof HTMLTextAreaElement) {
      return first;
    }
  }

  return null;
};

const resolveManagedLazyElement = (detail) => {
  if (isManagedLazyHx(detail?.elt)) return detail.elt;
  if (isManagedLazyHx(detail?.target)) return detail.target;
  return null;
};

const shouldSkipLazyRequest = (element) => {
  if (!isManagedLazyHx(element)) return false;
  if (isLazyHxLoaded(element) || isLazyHxPending(element)) return true;

  if (element.dataset.hxLazyOpenOnly !== "true") return false;
  const details = element.closest("details");
  return details instanceof HTMLDetailsElement && !details.open;
};

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

const clearStaleBusyState = (root = document.body || document.documentElement) => {
  if (!(root instanceof HTMLElement)) return;

  const maybeClear = (element) => {
    if (!(element instanceof HTMLElement)) return;
    if (element.getAttribute("aria-busy") !== "true") return;
    if ((busyElementCounts.get(element) || 0) > 0) return;
    setBusyState(element, false);
  };

  maybeClear(root);
  root.querySelectorAll('[aria-busy="true"]').forEach(maybeClear);
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
  if (!isRequestHandle(xhr)) return null;

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
  clearStaleBusyState();
  return state;
};

const clearPendingLazyState = (state) => {
  if (state?.lazyElement) {
    clearLazyHxPending(state.lazyElement);
  }
};

const syncEnterOnlyQueryParameter = (detail) => {
  if (detail?.triggeringEvent?.type !== "change") return;

  const queryInput = resolveEnterOnlyQueryInput(detail);
  if (!queryInput) return;

  const queryParam = (queryInput.getAttribute("name") || "").trim();
  if (!queryParam) return;

  if (queryInput.defaultValue === "") {
    delete detail.parameters?.[queryParam];
    return;
  }

  detail.parameters[queryParam] = queryInput.defaultValue;
};

const prepareLazyRequest = (event) => {
  const lazyElement = resolveManagedLazyElement(event.detail);
  if (!lazyElement) return null;

  if (shouldSkipLazyRequest(lazyElement)) {
    event.preventDefault();
    return null;
  }

  markLazyHxPending(lazyElement);
  return lazyElement;
};

const createRequestState = (detail, lazyElement) => {
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
    lazyElement,
  };

  const targetsPageRoot = target === document.body || target === document.documentElement;
  if (targetsPageRoot || detail.requestConfig?.boosted) {
    state.focusStrategy = "main";
  } else if (target && active && target.contains(active)) {
    state.focusStrategy = "restore";
    state.focusDescriptor = captureFocusDescriptor(active);
  }

  return state;
};

const finalizeManagedLazySwap = (target) => {
  if (!isManagedLazyHx(target)) return false;
  if (hasLazyHxError(target)) {
    clearLazyHxPending(target);
    return true;
  }

  markLazyHxLoaded(target);
  return false;
};

const restoreSwapFocus = (target, requestState) => {
  if (requestState?.focusStrategy === "main") {
    scheduleSoon(focusMainContent);
    return;
  }

  if (requestState?.focusStrategy === "restore") {
    restoreFocusAfterSwap(target, requestState.focusDescriptor);
  }
};

const initializeSwapTarget = (target, initGlobal) => {
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
};

const handleConfigRequest = (event) => {
  syncEnterOnlyQueryParameter(event.detail);
};

const handleBeforeRequest = (event) => {
  if (event.defaultPrevented) return;

  const detail = event.detail;
  if (!isRequestHandle(detail?.xhr)) return;

  const lazyElement = prepareLazyRequest(event);
  if (event.defaultPrevented) return;

  activeRequests.add(detail.xhr);
  syncGlobalBusyIndicators();
  htmxRequestState.set(detail.xhr, createRequestState(detail, lazyElement));
};

const handleAfterRequest = (event) => {
  const detail = event.detail;
  const state = finalizeRequestBusyState(detail?.xhr);
  if (!state) return;

  // Keep focus metadata until afterSwap reads it.
  const noSwapExpected = detail.failed === true || detail.xhr.status === 0 || detail.xhr.status === 204;
  if (noSwapExpected) {
    clearPendingLazyState(state);
  }
  if (noSwapExpected || !state.focusStrategy) {
    htmxRequestState.delete(detail.xhr);
  }
};

const handleAfterSwap = (event, initGlobal) => {
  const xhr = isRequestHandle(event.detail?.xhr) ? event.detail.xhr : null;
  const target = event.target;
  const requestState = xhr ? htmxRequestState.get(xhr) : null;

  try {
    if (!(target instanceof HTMLElement)) return;

    initializeSwapTarget(target, initGlobal);

    const managedLazySwapErrored = finalizeManagedLazySwap(target);
    restoreSwapFocus(target, requestState);

    // Avoid immediately re-triggering a lazy panel that just swapped an error fragment.
    if (!managedLazySwapErrored) {
      triggerVisibleLazyHx(document);
    }
  } finally {
    clearStaleBusyState(target instanceof HTMLElement ? target : undefined);
    if (xhr) {
      // Fallback cleanup for lifecycles that skip afterRequest (e.g. cancelled/edge cases).
      finalizeRequestBusyState(xhr);
      htmxRequestState.delete(xhr);
    }
  }
};

const handleFailedRequest = (event) => {
  const xhr = event.detail?.xhr;
  if (!isRequestHandle(xhr)) return;

  const state = finalizeRequestBusyState(xhr);
  clearPendingLazyState(state);
  htmxRequestState.delete(xhr);
};

const handleHtmxLoad = (event) => {
  if (!(event.target instanceof HTMLElement)) return;
  clearStaleBusyState(event.target);
  initFragment(event.target);
};

const handleShortcutKeydown = (event) => {
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
};

const handleTabClick = (event) => {
  if (!(event.target instanceof Element)) return;
  if (!event.target.closest('[role="tab"]')) return;
  scheduleVisibleLazyHx(document);
};

const handleTabKeydown = (event) => {
  if (!(event.target instanceof Element)) return;
  if (!event.target.closest('[role="tab"]')) return;
  if (!["ArrowRight", "ArrowLeft", "Home", "End", "Enter", " "].includes(event.key)) return;
  scheduleVisibleLazyHx(document);
};

export const bindGlobalListenersOnce = (options = {}) => {
  const { initGlobal = null } = options;
  if (document.documentElement.dataset.openSspmAppListenersBound === "true") return;
  document.documentElement.dataset.openSspmAppListenersBound = "true";
  clearStaleBusyState();
  syncGlobalBusyIndicators();

  document.addEventListener("htmx:configRequest", handleConfigRequest);
  document.addEventListener("htmx:beforeRequest", handleBeforeRequest);
  document.addEventListener("htmx:afterRequest", handleAfterRequest);
  document.addEventListener("htmx:afterSwap", (event) => {
    handleAfterSwap(event, initGlobal);
  });
  document.addEventListener("htmx:sendAbort", handleFailedRequest);
  document.addEventListener("htmx:sendError", handleFailedRequest);
  document.addEventListener("htmx:timeout", handleFailedRequest);
  document.addEventListener("htmx:responseError", handleFailedRequest);
  document.addEventListener("htmx:onLoadError", handleFailedRequest);
  document.addEventListener("htmx:swapError", handleFailedRequest);
  document.addEventListener("htmx:load", handleHtmxLoad);
  document.addEventListener("click", handleTabClick);
  document.addEventListener("keydown", handleShortcutKeydown);
  document.addEventListener("keydown", handleTabKeydown);
};
