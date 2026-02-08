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

export const openDialog = (dialog) => {
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

export const closeDialog = (dialog) => {
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

export const openServerDialogs = () => {
  document.querySelectorAll("dialog[data-open]").forEach((dialog) => {
    openDialog(dialog);
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

export const wireDialogCloseNavigation = () => {
  document.querySelectorAll("dialog[data-close-href]").forEach((dialog) => {
    if (!(dialog instanceof HTMLElement)) return;
    if (dialog.dataset.closeNavBound === "true") return;

    const closeHref = dialog.getAttribute("data-close-href");
    if (!closeHref) return;

    const navigateToCloseHref = () => {
      const current = window.location.pathname + window.location.search + window.location.hash;
      if (current === closeHref) return;
      window.location.href = closeHref;
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
