/**
 * Command Palette
 *
 * Opens a command dialog via ⌘K / Ctrl+K or trigger button click.
 */

const DIALOG_ID = "command-palette";
const TRIGGER_ATTR = "data-command-palette-trigger";
const DOCUMENT_BOUND_ATTR = "commandPaletteListenersBound";
const DIALOG_BOUND_ATTR = "commandPaletteBound";

const getDialog = () => document.getElementById(DIALOG_ID);

const getInput = (dialog) => {
  if (!dialog) return null;
  return dialog.querySelector(".command > header input");
};

const open = () => {
  const dialog = getDialog();
  if (!dialog || dialog.open) return;

  try {
    dialog.showModal();
  } catch {
    dialog.setAttribute("open", "");
  }

  const input = getInput(dialog);
  if (input) {
    input.value = "";
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.focus();
  }
};

const close = () => {
  const dialog = getDialog();
  if (!dialog || !dialog.open) return;

  try {
    dialog.close();
  } catch {
    dialog.removeAttribute("open");
  }
};

const handleShortcutKeydown = (e) => {
  if ((e.metaKey || e.ctrlKey) && e.key === "k") {
    e.preventDefault();
    e.stopPropagation();
    const dialog = getDialog();
    if (dialog && dialog.open) {
      close();
    } else {
      open();
    }
  }
};

const handleTriggerClick = (e) => {
  if (!(e.target instanceof Element)) return;

  const trigger = e.target.closest(`[${TRIGGER_ATTR}]`);
  if (trigger) {
    e.preventDefault();
    open();
  }
};

const handleDialogClick = (e) => {
  const dialog = e.currentTarget;
  if (!(dialog instanceof HTMLElement)) return;

  if (e.target === dialog) {
    close();
    return;
  }

  if (!(e.target instanceof Element)) return;

  const link = e.target.closest("a[role='menuitem'], [role='menuitem'] a");
  if (link) {
    close();
  }
};

export const wireCommandPalette = () => {
  if (document.documentElement.dataset[DOCUMENT_BOUND_ATTR] !== "true") {
    document.addEventListener("keydown", handleShortcutKeydown);
    document.addEventListener("click", handleTriggerClick);
    document.documentElement.dataset[DOCUMENT_BOUND_ATTR] = "true";
  }

  const dialog = getDialog();
  if (!(dialog instanceof HTMLElement)) return;
  if (dialog.dataset[DIALOG_BOUND_ATTR] === "true") return;

  dialog.addEventListener("click", handleDialogClick);
  dialog.dataset[DIALOG_BOUND_ATTR] = "true";
};
