import {
  openServerDialogs,
  wireDialogCloseButtons,
  wireDialogCloseNavigation,
  wireDialogOpenTriggers,
  wireDialogRemoveOnClose,
} from "open-sspm-app/dialogs.js";
import { wireCopyButtons } from "open-sspm-app/copy.js";
import { scheduleSoon } from "open-sspm-app/dom_focus.js";

const INTERACTIVE_ROW_SELECTOR = [
  "a[href]",
  "button",
  "input",
  "select",
  "textarea",
  "label",
  "summary",
  "[role='button']",
  "[role='link']",
  "[data-row-link-ignore]",
  "[contenteditable='true']",
].join(",");

const HX_LAZY_STATE_PENDING = "pending";
const HX_LAZY_STATE_LOADED = "loaded";

const getLazyHxState = (element) => {
  if (!(element instanceof HTMLElement)) return "";
  return (element.dataset.hxLazyState || "").trim();
};

export const isManagedLazyHx = (element) => element instanceof HTMLElement && element.hasAttribute("data-hx-lazy-load");

export const isLazyHxPending = (element) => isManagedLazyHx(element) && getLazyHxState(element) === HX_LAZY_STATE_PENDING;

export const isLazyHxLoaded = (element) => isManagedLazyHx(element) && getLazyHxState(element) === HX_LAZY_STATE_LOADED;

export const markLazyHxPending = (element) => {
  if (!isManagedLazyHx(element)) return;
  element.dataset.hxLazyState = HX_LAZY_STATE_PENDING;
};

export const markLazyHxLoaded = (element) => {
  if (!isManagedLazyHx(element)) return;
  element.dataset.hxLazyState = HX_LAZY_STATE_LOADED;
};

export const clearLazyHxPending = (element) => {
  if (!isManagedLazyHx(element)) return;
  if (!isLazyHxPending(element)) return;
  delete element.dataset.hxLazyState;
};

export const hasLazyHxError = (element) => {
  if (!(element instanceof HTMLElement)) return false;
  if (element.hasAttribute("data-hx-lazy-error")) return true;
  return element.querySelector("[data-hx-lazy-error]") instanceof HTMLElement;
};

export const wireAutosubmit = (root = document) => {
  root.querySelectorAll("[data-autosubmit]").forEach((element) => {
    if (!(element instanceof HTMLElement)) return;
    if (element.dataset.autosubmitBound === "true") return;
    element.addEventListener("change", () => {
      const form = element.closest("form");
      if (form instanceof HTMLFormElement) {
        if (typeof form.requestSubmit === "function") {
          form.requestSubmit();
        } else {
          form.submit();
        }
      }
    });
    element.dataset.autosubmitBound = "true";
  });
};

const syncDiscoveryReplacementFields = (select) => {
  if (!(select instanceof HTMLSelectElement)) return;
  const form = select.closest("form");
  if (!(form instanceof HTMLFormElement)) return;

  const replacementFields = form.querySelector("[data-discovery-replacement-fields]");
  if (!(replacementFields instanceof HTMLElement)) return;

  replacementFields.hidden = (select.value || "").trim() !== "replace";
};

export const wireDiscoveryGovernanceDisposition = (root = document) => {
  root.querySelectorAll("select[data-discovery-review-disposition]").forEach((element) => {
    if (!(element instanceof HTMLSelectElement)) return;
    if (element.dataset.discoveryReviewDispositionBound === "true") {
      syncDiscoveryReplacementFields(element);
      return;
    }

    element.addEventListener("change", () => {
      syncDiscoveryReplacementFields(element);
    });
    element.dataset.discoveryReviewDispositionBound = "true";
    syncDiscoveryReplacementFields(element);
  });
};

export const triggerVisibleLazyHx = (root = document) => {
  const htmxApi = window.htmx;
  if (!htmxApi || typeof htmxApi.trigger !== "function") return;

  root.querySelectorAll("[data-hx-lazy-load][data-hx-lazy-panel]").forEach((element) => {
    if (!(element instanceof HTMLElement)) return;
    if (isLazyHxPending(element) || isLazyHxLoaded(element)) return;

    if (element.dataset.hxLazyOpenOnly === "true") {
      const details = element.closest("details");
      if (details instanceof HTMLDetailsElement && !details.open) return;
    }

    const panelID = (element.dataset.hxLazyPanel || "").trim();
    if (!panelID) return;

    const panel = document.getElementById(panelID);
    if (!(panel instanceof HTMLElement) || panel.hidden) return;

    htmxApi.trigger(element, "oss-panel-visible");
  });
};

export const scheduleVisibleLazyHx = (root = document) => {
  scheduleSoon(() => {
    triggerVisibleLazyHx(root);
  });
};

const rowHref = (row) => (row.dataset.rowHref || "").trim();

const primaryRowAnchor = (row) => row.querySelector("a[href]");

const rowHasSelection = () => {
  if (typeof window.getSelection !== "function") return false;
  const selection = window.getSelection();
  if (!selection) return false;
  return selection.type === "Range" && selection.toString().trim() !== "";
};

const isInteractiveRowTarget = (target, row) => {
  if (!(target instanceof Element)) return false;
  const interactive = target.closest(INTERACTIVE_ROW_SELECTOR);
  if (!(interactive instanceof Element)) return false;
  if (!row.contains(interactive)) return false;
  return interactive !== row;
};

const rowNavigationHref = (row) => {
  const anchor = primaryRowAnchor(row);
  if (anchor instanceof HTMLAnchorElement) {
    return anchor.getAttribute("href") || anchor.href;
  }
  return rowHref(row);
};

const navigateToRowHref = (row, openInNewTab = false) => {
  const primaryAnchor = primaryRowAnchor(row);
  const href = rowNavigationHref(row);
  if (!href) return;

  if (openInNewTab) {
    window.open(href, "_blank", "noopener");
    return;
  }

  if (primaryAnchor instanceof HTMLAnchorElement) {
    primaryAnchor.click();
    return;
  }

  window.location.assign(href);
};

export const wireRowLinks = (root = document) => {
  root.querySelectorAll("tr[data-row-href]").forEach((row) => {
    if (!(row instanceof HTMLTableRowElement)) return;
    if (row.dataset.rowLinkBound === "true") return;
    if (!rowHref(row)) return;

    if (!row.hasAttribute("tabindex")) {
      row.setAttribute("tabindex", "0");
    }
    if (!row.hasAttribute("role")) {
      row.setAttribute("role", "link");
    }

    row.addEventListener("click", (event) => {
      if (event.button !== 0) return;
      if (event.defaultPrevented) return;
      if (rowHasSelection()) return;
      if (isInteractiveRowTarget(event.target, row)) return;
      navigateToRowHref(row, event.metaKey || event.ctrlKey || event.shiftKey);
    });

    row.addEventListener("auxclick", (event) => {
      if (event.button !== 1) return;
      if (event.defaultPrevented) return;
      if (rowHasSelection()) return;
      if (isInteractiveRowTarget(event.target, row)) return;
      navigateToRowHref(row, true);
    });

    row.addEventListener("keydown", (event) => {
      if (event.defaultPrevented) return;
      if (event.key !== "Enter" && event.key !== " ") return;
      if (isInteractiveRowTarget(event.target, row)) return;
      event.preventDefault();
      navigateToRowHref(row);
    });

    row.dataset.rowLinkBound = "true";
  });
};

export const initFragment = (root = document) => {
  openServerDialogs(root);
  wireDialogOpenTriggers(root);
  wireDialogCloseNavigation(root);
  wireDialogCloseButtons(root);
  wireDialogRemoveOnClose(root);
  wireCopyButtons(root);
  wireAutosubmit(root);
  wireDiscoveryGovernanceDisposition(root);
  wireRowLinks(root);
};
