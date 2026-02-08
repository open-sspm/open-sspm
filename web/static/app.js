(() => {
  const FOCUSABLE_SELECTOR = [
    "a[href]",
    "button:not([disabled])",
    "input:not([disabled])",
    "select:not([disabled])",
    "textarea:not([disabled])",
    "[tabindex]:not([tabindex='-1'])",
  ].join(",");

  const htmxRequestState = new WeakMap();
  const busyElementCounts = new WeakMap();
  const dialogReturnFocus = new WeakMap();
  let sidebarStateObserver = null;
  let sidebarEscapeHandler = null;
  let sidebarResizeHandler = null;
  let sidebarBackdropHandler = null;

  const cssEscape = (value) => {
    if (typeof value !== "string") return "";
    if (typeof CSS !== "undefined" && typeof CSS.escape === "function") {
      return CSS.escape(value);
    }
    return value.replace(/([!"#$%&'()*+,./:;<=>?@[\\\]^`{|}~])/g, "\\$1");
  };

  const isElementVisible = (element) => {
    if (!(element instanceof HTMLElement)) return false;
    if (!element.isConnected) return false;
    if (element.hasAttribute("hidden")) return false;
    if (element.getAttribute("aria-hidden") === "true") return false;
    const style = window.getComputedStyle(element);
    if (style.display === "none" || style.visibility === "hidden") return false;
    return true;
  };

  const isFocusable = (element, options = {}) => {
    const { allowProgrammatic = false } = options;
    if (!(element instanceof HTMLElement)) return false;
    if (!isElementVisible(element)) return false;
    if (element.matches("[disabled], [inert]")) return false;
    if (element.closest("[inert]")) return false;
    if (element.closest("[aria-hidden='true']")) return false;

    if (typeof element.tabIndex === "number" && element.tabIndex >= 0) {
      return true;
    }

    if (element.matches("a[href], button, input, select, textarea")) {
      return true;
    }

    if (
      allowProgrammatic &&
      element.hasAttribute("tabindex") &&
      Number.parseInt(element.getAttribute("tabindex") || "", 10) === -1
    ) {
      return true;
    }

    return false;
  };

  const focusElement = (element, options = {}) => {
    if (!isFocusable(element, options)) return false;
    try {
      element.focus({ preventScroll: true });
    } catch {
      element.focus();
    }
    return document.activeElement === element;
  };

  const findFirstFocusable = (root) => {
    if (!(root instanceof HTMLElement) && root !== document) return null;
    const scope = root === document ? document : root;
    const focusable = scope.querySelectorAll(FOCUSABLE_SELECTOR);
    for (const element of focusable) {
      if (isFocusable(element)) {
        return element;
      }
    }
    return null;
  };

  const focusMainContent = () => {
    const main = document.querySelector("[data-main-content], main");
    if (!(main instanceof HTMLElement)) return;

    if (!main.hasAttribute("tabindex")) {
      main.setAttribute("tabindex", "-1");
    }

    focusElement(main, { allowProgrammatic: true });
  };

  const captureFocusDescriptor = (element) => {
    if (!(element instanceof HTMLElement)) return null;
    return {
      id: (element.id || "").trim(),
      tagName: element.tagName.toLowerCase(),
      name: (element.getAttribute("name") || "").trim(),
      type: (element.getAttribute("type") || "").trim(),
      role: (element.getAttribute("role") || "").trim(),
      ariaLabel: (element.getAttribute("aria-label") || "").trim(),
      dataFocusKey: (element.getAttribute("data-focus-key") || "").trim(),
    };
  };

  const findMatchByDescriptor = (root, descriptor) => {
    if (!descriptor) return null;
    const scope = root instanceof HTMLElement ? root : document;

    if (descriptor.id) {
      const byId = document.getElementById(descriptor.id);
      if (byId && isFocusable(byId)) {
        return byId;
      }
    }

    const selectors = [];

    if (descriptor.dataFocusKey) {
      selectors.push(`[data-focus-key=\"${cssEscape(descriptor.dataFocusKey)}\"]`);
    }

    if (descriptor.tagName && descriptor.name && descriptor.type) {
      selectors.push(`${descriptor.tagName}[name=\"${cssEscape(descriptor.name)}\"][type=\"${cssEscape(descriptor.type)}\"]`);
    }

    if (descriptor.tagName && descriptor.name) {
      selectors.push(`${descriptor.tagName}[name=\"${cssEscape(descriptor.name)}\"]`);
    }

    if (descriptor.name) {
      selectors.push(`[name=\"${cssEscape(descriptor.name)}\"]`);
    }

    if (descriptor.role && descriptor.ariaLabel) {
      selectors.push(`[role=\"${cssEscape(descriptor.role)}\"][aria-label=\"${cssEscape(descriptor.ariaLabel)}\"]`);
    }

    if (descriptor.role) {
      selectors.push(`[role=\"${cssEscape(descriptor.role)}\"]`);
    }

    if (descriptor.ariaLabel) {
      selectors.push(`[aria-label=\"${cssEscape(descriptor.ariaLabel)}\"]`);
    }

    for (const selector of selectors) {
      const candidate = scope.querySelector(selector);
      if (candidate && isFocusable(candidate)) {
        return candidate;
      }
    }

    return null;
  };

  const scheduleSoon = (callback) => {
    if (typeof window.requestAnimationFrame === "function") {
      window.requestAnimationFrame(() => {
        callback();
      });
      return;
    }

    if (typeof queueMicrotask === "function") {
      queueMicrotask(callback);
      return;
    }

    setTimeout(callback, 0);
  };

  const restoreFocusAfterSwap = (swapTarget, descriptor) => {
    scheduleSoon(() => {
      const active = document.activeElement;
      if (active instanceof HTMLElement && active !== document.body && active.isConnected) {
        return;
      }

      const target = findMatchByDescriptor(swapTarget, descriptor) || findFirstFocusable(swapTarget);
      if (!target) return;
      focusElement(target);
    });
  };

  const collectBusyElements = (detail) => {
    const elements = new Set();

    const addBusyElement = (element) => {
      if (!(element instanceof HTMLElement)) return;
      elements.add(element);

      const region = element.closest("[data-busy-region]");
      if (region instanceof HTMLElement) {
        elements.add(region);
      }
    };

    addBusyElement(detail?.target);
    addBusyElement(detail?.elt);

    if (detail?.requestConfig?.boosted) {
      const main = document.querySelector("[data-main-content], main");
      if (main instanceof HTMLElement) {
        elements.add(main);
      }
    }

    return Array.from(elements);
  };

  const setBusyState = (element, isBusy) => {
    if (!(element instanceof HTMLElement)) return;
    element.setAttribute("aria-busy", isBusy ? "true" : "false");
  };

  const incrementBusy = (element) => {
    const currentCount = busyElementCounts.get(element) || 0;
    busyElementCounts.set(element, currentCount + 1);
    setBusyState(element, true);
  };

  const decrementBusy = (element) => {
    const currentCount = busyElementCounts.get(element) || 0;
    if (currentCount <= 1) {
      busyElementCounts.delete(element);
      setBusyState(element, false);
      return;
    }

    busyElementCounts.set(element, currentCount - 1);
  };

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
    const sidebar = document.getElementById("app-sidebar");
    const sidebarToggle = root.querySelector("#sidebar-toggle");
    if (!(sidebar instanceof HTMLElement) || !(sidebarToggle instanceof HTMLElement)) return;
    if (sidebarToggle.dataset.sidebarToggleBound === "true") {
      return;
    }

    if (sidebarStateObserver instanceof MutationObserver) {
      sidebarStateObserver.disconnect();
    }
    if (typeof sidebarEscapeHandler === "function") {
      document.removeEventListener("keydown", sidebarEscapeHandler);
    }
    if (typeof sidebarResizeHandler === "function") {
      window.removeEventListener("resize", sidebarResizeHandler);
    }
    if (sidebarBackdropHandler?.element instanceof HTMLElement && typeof sidebarBackdropHandler.handler === "function") {
      sidebarBackdropHandler.element.removeEventListener("click", sidebarBackdropHandler.handler, true);
    }

    let moveFocusToSidebarOnOpen = false;
    let returnFocusToToggleOnClose = false;
    const mobileBreakpoint = Number.parseInt(sidebar.dataset.breakpoint || "", 10) || 768;

    const isMobileViewport = () => window.innerWidth < mobileBreakpoint;
    const isSidebarOpen = () => sidebar.getAttribute("aria-hidden") !== "true";

    const syncSidebarState = () => {
      const open = isSidebarOpen();
      sidebarToggle.setAttribute("aria-expanded", String(open));
      sidebarToggle.setAttribute("aria-label", open ? "Close navigation" : "Open navigation");

      const wasOpen = sidebar.dataset.lastKnownOpen === "true";
      sidebar.dataset.lastKnownOpen = String(open);

      if (!isMobileViewport()) {
        moveFocusToSidebarOnOpen = false;
        returnFocusToToggleOnClose = false;
        return;
      }

      if (open && !wasOpen && moveFocusToSidebarOnOpen) {
        const firstFocusable = findFirstFocusable(sidebar);
        if (firstFocusable) {
          focusElement(firstFocusable);
        }
      }

      if (!open && wasOpen && returnFocusToToggleOnClose) {
        focusElement(sidebarToggle);
      }

      moveFocusToSidebarOnOpen = false;
      returnFocusToToggleOnClose = false;
    };

    sidebarStateObserver = new MutationObserver(() => {
      syncSidebarState();
    });
    sidebarStateObserver.observe(sidebar, { attributes: true, attributeFilter: ["aria-hidden"] });

    sidebarToggle.addEventListener("click", () => {
      if (isMobileViewport()) {
        const currentlyOpen = isSidebarOpen();
        moveFocusToSidebarOnOpen = !currentlyOpen;
        returnFocusToToggleOnClose = currentlyOpen;
      }

      document.dispatchEvent(new CustomEvent("basecoat:sidebar", { detail: { id: "app-sidebar" } }));
    });

    const onSidebarClick = (event) => {
      if (!isMobileViewport()) return;
      if (!(event.target instanceof Element)) return;

      const clickedOutsideNav = event.target === sidebar;
      const clickedAction = Boolean(event.target.closest("a, button"));
      const keepSidebarOpen = Boolean(event.target.closest("[data-keep-mobile-sidebar-open]"));

      if (clickedOutsideNav || (clickedAction && !keepSidebarOpen)) {
        returnFocusToToggleOnClose = true;
      }
    };
    sidebar.addEventListener("click", onSidebarClick, true);
    sidebarBackdropHandler = { element: sidebar, handler: onSidebarClick };

    sidebarEscapeHandler = (event) => {
      if (event.key !== "Escape") return;
      if (event.defaultPrevented) return;
      if (!isMobileViewport()) return;
      if (!isSidebarOpen()) return;
      if (event.target instanceof Element && event.target.closest("dialog[open]")) return;

      returnFocusToToggleOnClose = true;
      document.dispatchEvent(
        new CustomEvent("basecoat:sidebar", {
          detail: {
            id: "app-sidebar",
            action: "close",
          },
        }),
      );
    };
    document.addEventListener("keydown", sidebarEscapeHandler);

    sidebarResizeHandler = () => {
      syncSidebarState();
    };
    window.addEventListener("resize", sidebarResizeHandler);

    syncSidebarState();
    scheduleSoon(syncSidebarState);

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

  const scheduleVisibleLazyHx = (root = document) => {
    scheduleSoon(() => {
      triggerVisibleLazyHx(root);
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

    document.addEventListener("htmx:beforeRequest", (event) => {
      const detail = event.detail;
      if (!(detail?.xhr instanceof XMLHttpRequest)) return;

      const busyElements = collectBusyElements(detail);
      busyElements.forEach((element) => {
        incrementBusy(element);
      });

      const target = detail.target instanceof HTMLElement ? detail.target : null;
      const active = document.activeElement instanceof HTMLElement ? document.activeElement : null;
      const state = {
        busyElements,
        focusStrategy: null,
        focusDescriptor: null,
      };

      const targetsPageRoot = target === document.body || target === document.documentElement;
      if (targetsPageRoot || detail.requestConfig?.boosted) {
        state.focusStrategy = "main";
      } else if (target && active && target.contains(active)) {
        state.focusStrategy = "restore";
        state.focusDescriptor = captureFocusDescriptor(active);
      }

      htmxRequestState.set(detail.xhr, state);
    });

    document.addEventListener("htmx:afterRequest", (event) => {
      const detail = event.detail;
      if (!(detail?.xhr instanceof XMLHttpRequest)) return;

      const state = htmxRequestState.get(detail.xhr);
      if (!state) return;

      state.busyElements.forEach((element) => {
        decrementBusy(element);
      });

      // Keep focus metadata until afterSwap reads it.
      const noSwapExpected = detail.failed === true || detail.xhr.status === 0 || detail.xhr.status === 204;
      if (noSwapExpected || !state.focusStrategy) {
        htmxRequestState.delete(detail.xhr);
      }
    });

    document.addEventListener("htmx:afterSwap", (event) => {
      if (!(event.target instanceof HTMLElement)) return;

      initFragment(event.target);

      const xhr = event.detail?.xhr instanceof XMLHttpRequest ? event.detail.xhr : null;
      const requestState = xhr ? htmxRequestState.get(xhr) : null;

      if (event.target === document.body || event.target === document.documentElement) {
        initGlobal();
        initFragment(document);
      }

      if (event.target.querySelector("#flash-toast")) {
        showFlashToast();
      }

      if (requestState?.focusStrategy === "main") {
        scheduleSoon(focusMainContent);
      } else if (requestState?.focusStrategy === "restore") {
        restoreFocusAfterSwap(event.target, requestState.focusDescriptor);
      }

      triggerVisibleLazyHx(document);

      if (xhr) {
        htmxRequestState.delete(xhr);
      }
    });

    document.addEventListener("htmx:load", (event) => {
      if (event.target instanceof HTMLElement) {
        initFragment(event.target);
      }
    });

    document.addEventListener("click", (event) => {
      if (!(event.target instanceof Element)) return;
      if (!event.target.closest('[role="tab"]')) return;
      scheduleVisibleLazyHx(document);
    });

    document.addEventListener("keydown", (event) => {
      const activeElement = document.activeElement;
      if (!(activeElement instanceof HTMLElement)) return;
      if (event.key !== "/") return;
      if (["INPUT", "TEXTAREA"].includes(activeElement.tagName)) return;
      if (activeElement.isContentEditable) return;

      event.preventDefault();
      const searchInput = document.getElementById("command-search-input");
      if (searchInput instanceof HTMLElement) {
        searchInput.focus();
      }
    });

    document.addEventListener("keydown", (event) => {
      if (!(event.target instanceof Element)) return;
      if (!event.target.closest('[role="tab"]')) return;
      if (!["ArrowRight", "ArrowLeft", "Home", "End", "Enter", " "].includes(event.key)) return;
      scheduleVisibleLazyHx(document);
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
