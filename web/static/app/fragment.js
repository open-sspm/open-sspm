import { scheduleSoon } from "open-sspm-app/dom_focus.js";
import { wireDialogCloseButtons } from "open-sspm-app/dialogs.js";

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

export const initFragment = (root = document) => {
  wireDialogCloseButtons(root);
  wireAutosubmit(root);
  triggerVisibleLazyHx(root);
};
