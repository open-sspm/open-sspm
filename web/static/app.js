(() => {
  const isDialogOpen = (dialog) => {
    if (!(dialog instanceof HTMLElement)) return false;
    if (dialog.hasAttribute("open")) return true;
    return dialog.open === true;
  };

  const openDialog = (dialog) => {
    if (!(dialog instanceof HTMLElement)) return;
    if (isDialogOpen(dialog)) return;

    const showModal = dialog.showModal;
    if (typeof showModal === "function") {
      try {
        showModal.call(dialog);
        return;
      } catch {
        // fall back to setting the open attribute
      }
    }

    dialog.setAttribute("open", "");
  };

  const closeDialog = (dialog) => {
    if (!(dialog instanceof HTMLElement)) return;
    if (!isDialogOpen(dialog)) return;

    const close = dialog.close;
    if (typeof close === "function") {
      try {
        close.call(dialog);
        return;
      } catch {
        // fall back to removing the open attribute
      }
    }

    dialog.removeAttribute("open");
    dialog.dispatchEvent(new Event("close"));
  };

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
      openDialog(dialog);
    });
  };

  const wireDialogCloseButtons = (root = document) => {
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

  const wireDialogCloseNavigation = () => {
    document.querySelectorAll("dialog[data-close-href]").forEach((dialog) => {
      if (!(dialog instanceof HTMLElement)) return;
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

  const triggerVisibleLazyHx = (root = document) => {
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

  const initFragment = (root = document) => {
    wireDialogCloseButtons(root);
    wireAutosubmit(root);
    triggerVisibleLazyHx(root);
  };

  const initGlobal = () => {
    showFlashToast();
    wireSidebarToggle();
    openServerDialogs();
    wireDialogCloseNavigation();
    triggerVisibleLazyHx(document);
  };

  const bindGlobalListenersOnce = () => {
    if (document.documentElement.dataset.openSspmAppListenersBound === "true") return;
    document.documentElement.dataset.openSspmAppListenersBound = "true";

    document.addEventListener("htmx:afterSwap", (event) => {
      if (!(event.target instanceof HTMLElement)) return;

      initFragment(event.target);

      if (event.target === document.body || event.target === document.documentElement) {
        initGlobal();
        initFragment(document);
        return;
      }

      if (event.target.querySelector("#flash-toast")) {
        showFlashToast();
      }

      triggerVisibleLazyHx(document);
    });

    document.addEventListener("htmx:load", (event) => {
      if (event.target instanceof HTMLElement) {
        initFragment(event.target);
      }
    });

    document.addEventListener("click", (event) => {
      if (!(event.target instanceof Element)) return;
      if (!event.target.closest('[role="tab"]')) return;
      triggerVisibleLazyHx(document);
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

    document.addEventListener("keydown", (event) => {
      if (!(event.target instanceof Element)) return;
      if (!event.target.closest('[role="tab"]')) return;
      if (!["ArrowRight", "ArrowLeft", "Home", "End", "Enter", " "].includes(event.key)) return;
      triggerVisibleLazyHx(document);
    });
  };

  bindGlobalListenersOnce();

  const initPage = () => {
    initGlobal();
    initFragment(document);
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initPage, { once: true });
  } else {
    initPage();
  }
})();
