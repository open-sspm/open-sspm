(() => {
  const showFlashToast = () => {
    const toastEl = document.getElementById("flash-toast");
    if (!(toastEl instanceof HTMLElement)) return;

    const category = (toastEl.dataset.category || "info").trim() || "info";
    const title = (toastEl.dataset.title || "").trim();
    const description = (toastEl.dataset.description || "").trim();
    if (!title && !description) {
      toastEl.remove();
      return;
    }

    document.dispatchEvent(
      new CustomEvent("basecoat:toast", {
        detail: {
          config: {
            category,
            title,
            description,
          },
        },
      }),
    );

    toastEl.remove();
  };

  const wireSidebarToggle = (root = document) => {
    const sidebarToggle = root.querySelector("#sidebar-toggle");
    if (!(sidebarToggle instanceof HTMLElement)) return;
    if (sidebarToggle.dataset.sidebarToggleBound === "true") return;

    sidebarToggle.addEventListener("click", () => {
      document.dispatchEvent(new CustomEvent("basecoat:sidebar", { detail: { id: "app-sidebar" } }));
    });
    sidebarToggle.dataset.sidebarToggleBound = "true";
  };

  const openServerDialogs = () => {
    document.querySelectorAll("dialog[data-open]").forEach((dialog) => {
      if (!(dialog instanceof HTMLDialogElement)) return;
      if (dialog.open) return;
      try {
        dialog.showModal();
      } catch {
        dialog.setAttribute("open", "");
      }
    });
  };

  const wireDialogCloseButtons = (root = document) => {
    root.querySelectorAll("[data-dialog-close]").forEach((element) => {
      if (!(element instanceof HTMLElement)) return;
      if (element.dataset.dialogCloseBound === "true") return;

      element.addEventListener("click", () => {
        const dialog = element.closest("dialog");
        if (!(dialog instanceof HTMLDialogElement)) return;
        if (!dialog.open) return;
        try {
          dialog.close();
        } catch {
          dialog.removeAttribute("open");
          dialog.dispatchEvent(new Event("close"));
        }
      });

      element.dataset.dialogCloseBound = "true";
    });
  };

  const wireDialogCloseNavigation = () => {
    document.querySelectorAll("dialog[data-close-href]").forEach((dialog) => {
      if (!(dialog instanceof HTMLDialogElement)) return;
      if (dialog.dataset.closeNavBound === "true") return;

      const closeHref = dialog.getAttribute("data-close-href");
      if (!closeHref) return;

      const navigateToCloseHref = () => {
        const current = window.location.pathname + window.location.search;
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

  const wireAutosubmit = (root = document) => {
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

  const init = () => {
    showFlashToast();
    wireSidebarToggle();
    openServerDialogs();
    wireDialogCloseButtons();
    wireDialogCloseNavigation();
    wireAutosubmit();
  };

  const bindGlobalListenersOnce = () => {
    if (document.documentElement.dataset.openSspmAppListenersBound === "true") return;
    document.documentElement.dataset.openSspmAppListenersBound = "true";

    document.addEventListener("htmx:afterSwap", () => {
      // Re-run init after hx-boost or other large swaps
      // We use afterSwap to ensure the DOM is ready
      init();
    });

    document.addEventListener("htmx:load", (event) => {
      // This handles partial updates if needed, but init() covers global state.
      // wireAutosubmit is safe to run multiple times.
      if (event.target instanceof HTMLElement) {
        wireAutosubmit(event.target);
        wireDialogCloseButtons(event.target);
      }
    });

    document.addEventListener("keydown", (e) => {
      if (e.key === "/" && document.activeElement.tagName !== "INPUT" && document.activeElement.tagName !== "TEXTAREA") {
        e.preventDefault();
        const searchInput = document.getElementById("command-search-input");
        if (searchInput) {
          searchInput.focus();
        }
      }
    });
  };

  bindGlobalListenersOnce();

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init, { once: true });
  } else {
    init();
  }
})();
