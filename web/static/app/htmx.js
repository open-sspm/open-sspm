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
} from "open-sspm-app/fragment.js";
import { showFlashToast } from "open-sspm-app/components/toast.js";

const htmxRequestState = new WeakMap();
const busyElementCounts = new WeakMap();
const activeRequests = new Set();
const reportedFailures = new WeakSet();

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

const readCSRFToken = () => {
  const meta = document.querySelector('meta[name="csrf-token"]');
  if (!(meta instanceof HTMLMetaElement)) return "";
  return (meta.content || "").trim();
};

const applyCSRFHeader = (detail) => {
  const token = readCSRFToken();
  if (!token || !detail?.headers) return;
  detail.headers["X-CSRF-Token"] = token;
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
    if (detail.parameters) delete detail.parameters[queryParam];
    return;
  }

  if (detail.parameters) {
    detail.parameters[queryParam] = queryInput.defaultValue;
  }
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
    failureReported: false,
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

const finalizeManagedLazySwap = (target, requestState) => {
  const lazyElement = isManagedLazyHx(target) ? target : requestState?.lazyElement;
  if (!isManagedLazyHx(lazyElement)) return false;
  if (hasLazyHxError(target)) {
    clearLazyHxPending(lazyElement);
    return true;
  }

  markLazyHxLoaded(lazyElement);
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
  applyCSRFHeader(event.detail);
  syncEnterOnlyQueryParameter(event.detail);
};

const handleBeforeSwap = (event) => {
  const detail = event.detail;
  const status = Number(detail?.xhr?.status || 0);
  const hasFragment = typeof detail?.xhr?.responseText === "string" && detail.xhr.responseText.trim() !== "";
  const contentType =
    typeof detail?.xhr?.getResponseHeader === "function"
      ? detail.xhr.getResponseHeader("Content-Type") || ""
      : detail?.xhr?.contentType || "";
  const isHTML = contentType.toLowerCase().includes("text/html");
  if ((status === 400 || status === 401 || status === 409 || status === 422) && hasFragment && isHTML) {
    detail.shouldSwap = true;
    detail.isError = false;
    return;
  }

  const state = isRequestHandle(detail?.xhr) ? htmxRequestState.get(detail.xhr) : null;
  const isLazyErrorFragment =
    status >= 400 &&
    status <= 599 &&
    hasFragment &&
    isHTML &&
    Boolean(state?.lazyElement) &&
    detail.xhr.responseText.includes("data-hx-lazy-error");
  if (isLazyErrorFragment) {
    detail.shouldSwap = true;
    detail.isError = false;
  }
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
  // HTMX emits sendError/sendAbort/timeout immediately after afterRequest for
  // status-0 requests. Keep the state until that more specific event reports
  // the correct outcome.
  if (detail.xhr.status === 0) return;
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
    finalizeManagedLazySwap(target, requestState);
    restoreSwapFocus(target, requestState);
  } finally {
    clearStaleBusyState(target instanceof HTMLElement ? target : undefined);
    if (xhr) {
      // Fallback cleanup for lifecycles that skip afterRequest (e.g. cancelled/edge cases).
      finalizeRequestBusyState(xhr);
      htmxRequestState.delete(xhr);
    }
  }
};

// Statuses we explicitly swap as content (see handleBeforeSwap). For those, the
// response body already carries the user-facing error and the generic "Request
// failed" toast would be a duplicate.
const swapAllowedErrorStatuses = new Set([400, 401, 409, 422]);

const hxTriggerHasEvent = (triggerHeader, eventName) => {
  const header = (triggerHeader || "").trim();
  if (!header) return false;
  if (header.startsWith("{")) {
    try {
      return Object.prototype.hasOwnProperty.call(JSON.parse(header), eventName);
    } catch (_) {
      return false;
    }
  }
  return header.split(",").some((name) => name.trim() === eventName);
};

const failureKindForEvent = (eventName) => {
  switch (eventName) {
    case "htmx:sendAbort":
      return "abort";
    case "htmx:sendError":
      return "network";
    case "htmx:timeout":
      return "timeout";
    case "htmx:onLoadError":
    case "htmx:swapError":
      return "display";
    default:
      return "response";
  }
};

const failureMessage = (kind, status) => {
  switch (kind) {
    case "network":
      return {
        title: "Connection problem",
        description: "Open SSPM couldn't be reached. Check your connection and try again.",
      };
    case "timeout":
      return {
        title: "Request timed out",
        description: "This is taking longer than expected. Try again.",
      };
    case "display":
      return {
        title: "Couldn't display the response",
        description: "The response couldn't be displayed. Refresh the page and try again.",
      };
    default:
      return {
        title: "Request failed",
        description: status >= 500 ? "The server could not complete that request." : "Refresh the page and try again.",
      };
  }
};

const activateLazyFailure = (lazyElement, message) => {
  if (!(lazyElement instanceof HTMLElement)) return false;

  if (hasLazyHxError(lazyElement)) {
    const description = lazyElement.querySelector("[data-hx-lazy-error-description]");
    if (description instanceof HTMLElement) description.textContent = message.description;
    return true;
  }

  const template = lazyElement.querySelector("template[data-hx-lazy-error-template]");
  if (!(template instanceof HTMLTemplateElement)) return false;

  const fragment = template.content.cloneNode(true);
  const replacement = fragment.querySelector("[data-hx-lazy-error]");
  if (!(replacement instanceof HTMLElement)) return false;

  const description = replacement.querySelector("[data-hx-lazy-error-description]");
  if (description instanceof HTMLElement) description.textContent = message.description;
  lazyElement.replaceWith(replacement);
  if (typeof window.htmx?.process === "function") {
    window.htmx.process(replacement);
  }
  return true;
};

const dispatchFailureToast = (message) => {
  document.dispatchEvent(
    new CustomEvent("osspm:toast", {
      detail: {
        config: {
          category: "error",
          title: message.title,
          description: message.description,
        },
      },
    }),
  );
};

const handleFailedRequest = (event) => {
  const xhr = event.detail?.xhr;
  if (!isRequestHandle(xhr)) return;

  const state = finalizeRequestBusyState(xhr);
  clearPendingLazyState(state);

  const kind = failureKindForEvent(event.type);
  if (kind === "abort") {
    htmxRequestState.delete(xhr);
    return;
  }
  if (reportedFailures.has(xhr) || state?.failureReported) {
    htmxRequestState.delete(xhr);
    return;
  }

  reportedFailures.add(xhr);
  if (state) state.failureReported = true;
  htmxRequestState.delete(xhr);

  const status = Number(xhr.status || 0);
  if (swapAllowedErrorStatuses.has(status)) return;
  const triggerHeader = typeof xhr.getResponseHeader === "function" ? xhr.getResponseHeader("HX-Trigger") || "" : "";
  if (hxTriggerHasEvent(triggerHeader, "osspm:toast")) return;

  const message = failureMessage(kind, status);
  if (activateLazyFailure(state?.lazyElement, message)) return;
  dispatchFailureToast(message);
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

const handleDetailsToggle = (event) => {
  const details = event.target;
  if (!(details instanceof HTMLDetailsElement) || !details.open) return;
  scheduleVisibleLazyHx(details);
};

export const bindGlobalListenersOnce = (options = {}) => {
  const { initGlobal = null } = options;
  if (document.documentElement.dataset.openSspmAppListenersBound === "true") return;
  document.documentElement.dataset.openSspmAppListenersBound = "true";
  clearStaleBusyState();
  syncGlobalBusyIndicators();

  document.addEventListener("htmx:configRequest", handleConfigRequest);
  document.addEventListener("htmx:beforeRequest", handleBeforeRequest);
  document.addEventListener("htmx:beforeSwap", handleBeforeSwap);
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
  document.addEventListener("keydown", handleShortcutKeydown);
  document.addEventListener("toggle", handleDetailsToggle, true);
};
