import { scheduleSoon } from "open-sspm-app/dom_focus.js";
import { wireDialogCloseButtons } from "open-sspm-app/dialogs.js";

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

export const triggerVisibleLazyHx = (root = document) => {
  const htmxApi = window.htmx;
  if (!htmxApi || typeof htmxApi.trigger !== "function") return;

  root.querySelectorAll("[data-hx-lazy-panel]").forEach((element) => {
    if (!(element instanceof HTMLElement)) return;
    if (element.dataset.hxLazyLoaded === "true") return;

    const panelID = (element.dataset.hxLazyPanel || "").trim();
    if (!panelID) return;

    const panel = document.getElementById(panelID);
    if (!(panel instanceof HTMLElement) || panel.hidden) return;

    htmxApi.trigger(element, "oss-panel-visible");
    element.dataset.hxLazyLoaded = "true";
  });
};

export const scheduleVisibleLazyHx = (root = document) => {
  scheduleSoon(() => {
    triggerVisibleLazyHx(root);
  });
};

const rowHref = (row) => (row.dataset.rowHref || "").trim();

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

const navigateToRowHref = (row, openInNewTab = false) => {
  const href = rowHref(row);
  if (!href) return;
  if (openInNewTab) {
    window.open(href, "_blank", "noopener");
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
  wireDialogCloseButtons(root);
  wireAutosubmit(root);
  wireRowLinks(root);
  triggerVisibleLazyHx(root);
};
