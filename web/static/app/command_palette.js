/**
 * Command Palette
 *
 * Opens a command dialog via ⌘K / Ctrl+K or trigger button click.
 */

const DIALOG_ID = "command-palette";
const TRIGGER_ATTR = "data-command-palette-trigger";

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

export const wireCommandPalette = () => {
  document.addEventListener("keydown", (e) => {
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
  });

  document.addEventListener("click", (e) => {
    const trigger = e.target.closest(`[${TRIGGER_ATTR}]`);
    if (trigger) {
      e.preventDefault();
      open();
    }
  });

  const dialog = getDialog();
  if (!dialog) return;

  dialog.addEventListener("click", (e) => {
    if (e.target === dialog) {
      close();
      return;
    }

    const link = e.target.closest("a[role='menuitem'], [role='menuitem'] a");
    if (link) {
      close();
    }
  });
};
