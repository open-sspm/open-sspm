import { bindGlobalListenersOnce } from "open-sspm-app/htmx.js";
import { openServerDialogs, wireDialogCloseNavigation } from "open-sspm-app/dialogs.js";
import { triggerVisibleLazyHx, initFragment } from "open-sspm-app/fragment.js";
import { wireSidebarToggle } from "open-sspm-app/sidebar.js";
import { showFlashToast } from "open-sspm-app/toast.js";

export const initGlobal = () => {
  showFlashToast();
  wireSidebarToggle();
  openServerDialogs();
  wireDialogCloseNavigation();
  triggerVisibleLazyHx(document);
};

const initPage = () => {
  initGlobal();
  initFragment(document);
};

export const bootOpenSspmApp = () => {
  if (document.documentElement.dataset.openSspmAppBootstrapped === "true") return;
  document.documentElement.dataset.openSspmAppBootstrapped = "true";

  bindGlobalListenersOnce({ initGlobal });

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initPage, { once: true });
  } else {
    initPage();
  }
};
