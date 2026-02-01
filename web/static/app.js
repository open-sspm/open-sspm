(() => {
  const sidebarToggle = document.getElementById("sidebar-toggle");

  if (sidebarToggle) {
    sidebarToggle.addEventListener("click", () => {
      document.dispatchEvent(new CustomEvent("basecoat:sidebar", { detail: { id: "app-sidebar" } }));
    });
  }

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
    openServerDialogs();
    wireDialogCloseNavigation();
    wireAutosubmit();
  };

  document.addEventListener("DOMContentLoaded", () => {
    init();
    
    document.addEventListener("htmx:afterSwap", (event) => {
       // Re-run init after hx-boost or other large swaps
       // We use afterSwap to ensure the DOM is ready
       init();
    });

    document.addEventListener("htmx:load", (event) => {
      // This handles partial updates if needed, but init() covers global state.
      // wireAutosubmit is safe to run multiple times.
      if (event.target instanceof HTMLElement) {
        wireAutosubmit(event.target);
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
  });
})();
