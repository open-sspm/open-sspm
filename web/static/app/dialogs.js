import { focusElement, scheduleSoon } from "open-sspm-app/dom_focus.js";

const dialogReturnFocus = new WeakMap();

const rememberDialogFocus = (dialog) => {
  if (!(dialog instanceof HTMLElement)) return;
  const active = document.activeElement;
  if (!(active instanceof HTMLElement)) return;
  if (dialog.contains(active)) return;
  dialogReturnFocus.set(dialog, active);
};

const restoreDialogFocus = (dialog) => {
  if (!(dialog instanceof HTMLElement)) return;
  const returnFocusTarget = dialogReturnFocus.get(dialog);
  dialogReturnFocus.delete(dialog);

  if (!(returnFocusTarget instanceof HTMLElement)) return;
  if (!returnFocusTarget.isConnected) return;

  scheduleSoon(() => {
    focusElement(returnFocusTarget);
  });
};

const isDialogOpen = (dialog) => {
  if (!(dialog instanceof HTMLElement)) return false;
  if (dialog.hasAttribute("open")) return true;
  return dialog.open === true;
};

const openDialog = (dialog) => {
  if (!(dialog instanceof HTMLElement)) return;
  if (isDialogOpen(dialog)) return;

  rememberDialogFocus(dialog);

  const showModal = dialog.showModal;
  if (typeof showModal === "function") {
    try {
      showModal.call(dialog);
      return;
    } catch {
      // Fall back to setting the open attribute.
    }
  }

  dialog.setAttribute("open", "");
};

const closeDialog = (dialog) => {
  if (!(dialog instanceof HTMLElement)) return;
  if (!isDialogOpen(dialog)) return;

  const shouldRestoreFocus = !dialog.hasAttribute("data-close-href");
  const close = dialog.close;
  if (typeof close === "function") {
    try {
      close.call(dialog);
      if (shouldRestoreFocus) {
        restoreDialogFocus(dialog);
      }
      return;
    } catch {
      // Fall back to removing the open attribute.
    }
  }

  dialog.removeAttribute("open");
  dialog.dispatchEvent(new Event("close"));
  if (shouldRestoreFocus) {
    restoreDialogFocus(dialog);
  }
};

const removeOlderDialogWithSameID = (dialog) => {
  if (!(dialog instanceof HTMLElement)) return;
  const id = (dialog.id || "").trim();
  if (!id || typeof CSS === "undefined" || typeof CSS.escape !== "function") return;

  document.querySelectorAll(`dialog#${CSS.escape(id)}[data-remove-on-close]`).forEach((existing) => {
    if (existing !== dialog) existing.remove();
  });
};

export const openServerDialogs = (root = document) => {
  root.querySelectorAll("dialog[data-open]").forEach((dialog) => {
    removeOlderDialogWithSameID(dialog);
    openDialog(dialog);
    dialog.removeAttribute("data-open");
  });
};

const resolveDialogTarget = (trigger) => {
  if (!(trigger instanceof HTMLElement)) return null;

  const selector = (trigger.getAttribute("data-dialog-open") || "").trim();
  if (!selector) return null;

  try {
    return document.querySelector(selector);
  } catch {
    return null;
  }
};

export const wireDialogOpenTriggers = (root = document) => {
  root.querySelectorAll("[data-dialog-open]").forEach((element) => {
    if (!(element instanceof HTMLElement)) return;
    if (element.dataset.dialogOpenBound === "true") return;

    element.addEventListener("click", (event) => {
      const dialog = resolveDialogTarget(element);
      if (!(dialog instanceof HTMLElement)) return;

      event.preventDefault();
      openDialog(dialog);
    });

    element.dataset.dialogOpenBound = "true";
  });
};

export const wireDialogCloseButtons = (root = document) => {
  root.querySelectorAll("[data-dialog-close]").forEach((element) => {
    if (!(element instanceof HTMLElement)) return;
    if (element.dataset.dialogCloseBound === "true") return;

    element.addEventListener("click", () => {
      const dialog = element.closest("dialog");
      closeDialog(dialog);
    });

    element.dataset.dialogCloseBound = "true";
  });
};

export const wireDialogCloseNavigation = (root = document) => {
  root.querySelectorAll("dialog[data-close-href]").forEach((dialog) => {
    if (!(dialog instanceof HTMLElement)) return;
    if (dialog.dataset.closeNavBound === "true") return;

    const closeHref = (dialog.getAttribute("data-close-href") || "").trim();
    if (!closeHref) return;

    const navigateToCloseHref = () => {
      let targetUrl;
      try {
        targetUrl = new URL(closeHref, window.location.href);
      } catch {
        return;
      }

      if (targetUrl.origin !== window.location.origin) return;

      const current = window.location.pathname + window.location.search + window.location.hash;
      const target = targetUrl.pathname + targetUrl.search + targetUrl.hash;
      if (current === target) return;
      window.location.href = targetUrl.href;
    };

    dialog.addEventListener("cancel", (event) => {
      event.preventDefault();
      navigateToCloseHref();
    });

    dialog.addEventListener("close", () => {
      navigateToCloseHref();
    });

    dialog.dataset.closeNavBound = "true";
  });
};

export const wireDialogRemoveOnClose = (root = document) => {
  root.querySelectorAll("dialog[data-remove-on-close]").forEach((dialog) => {
    if (!(dialog instanceof HTMLElement)) return;
    if (dialog.dataset.removeOnCloseBound === "true") return;

    dialog.addEventListener("close", () => {
      scheduleSoon(() => {
        if (!isDialogOpen(dialog)) dialog.remove();
      });
    });

    dialog.dataset.removeOnCloseBound = "true";
  });
};
