const FOCUSABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
].join(",");

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

export const focusElement = (element, options = {}) => {
  if (!isFocusable(element, options)) return false;
  try {
    element.focus({ preventScroll: true });
  } catch {
    element.focus();
  }
  return document.activeElement === element;
};

export const findFirstFocusable = (root) => {
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

export const focusMainContent = () => {
  const main = document.querySelector("[data-main-content], main");
  if (!(main instanceof HTMLElement)) return;

  if (!main.hasAttribute("tabindex")) {
    main.setAttribute("tabindex", "-1");
  }

  focusElement(main, { allowProgrammatic: true });
};

export const captureFocusDescriptor = (element) => {
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

export const scheduleSoon = (callback) => {
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

export const restoreFocusAfterSwap = (swapTarget, descriptor) => {
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
