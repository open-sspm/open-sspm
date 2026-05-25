import { bindGlobalListenersOnce } from "open-sspm-app/htmx.js";
import { bindBoostScopeOnce } from "open-sspm-app/boost.js";
import { openServerDialogs, wireDialogCloseNavigation } from "open-sspm-app/dialogs.js";
import { initFragment } from "open-sspm-app/fragment.js";
import { wireSidebarToggle } from "open-sspm-app/sidebar.js";
import { showFlashToast } from "open-sspm-app/toast.js";
import { wireCommandPalette } from "open-sspm-app/command_palette.js";
import { bindConfirmListener } from "open-sspm-app/confirm.js";

const initGlobal = () => {
  showFlashToast();
  wireSidebarToggle();
  wireCommandPalette();
  openServerDialogs();
  wireDialogCloseNavigation();
  bindConfirmListener();
};

const initPage = () => {
  initGlobal();
  initFragment(document);
};

export const bootOpenSspmApp = () => {
  if (document.documentElement.dataset.openSspmAppBootstrapped === "true") return;
  document.documentElement.dataset.openSspmAppBootstrapped = "true";

  bindBoostScopeOnce();
  bindGlobalListenersOnce({ initGlobal });

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initPage, { once: true });
  } else {
    initPage();
  }
};
