/**
 * Open-SSPM Component Registry
 *
 * Auto-initializes UI components when they appear in the DOM.
 * Replaces the Basecoat JS runtime with our own lightweight system.
 */

const components = new Map();
const componentState = new WeakMap();
let observer = null;

const initializedAttr = (name) => `data-${name}-initialized`;

const isInitialized = (el, name) =>
  el.getAttribute(initializedAttr(name)) === "true";

const getComponentState = (el) => {
  let state = componentState.get(el);
  if (!state) {
    state = { initialized: new Set(), cleanups: new Map() };
    componentState.set(el, state);
  }
  return state;
};

const markInitialized = (el, name) => {
  getComponentState(el).initialized.add(name);
  el.setAttribute(initializedAttr(name), "true");
};

const dispatchInitialized = (el, name) => {
  el.dispatchEvent(
    new CustomEvent("osspm:initialized", {
      bubbles: true,
      detail: { component: name },
    }),
  );
};

const isElementNode = (node) => node?.nodeType === 1;

const runCleanup = (el) => {
  const state = componentState.get(el);
  if (!state) return;

  for (const [name, cleanup] of state.cleanups) {
    try {
      cleanup();
    } catch (err) {
      console.error(`[osspm] Failed to cleanup "${name}":`, err);
    }
  }

  state.cleanups.clear();
  state.initialized.forEach((name) => {
    el.removeAttribute(initializedAttr(name));
  });
  state.initialized.clear();
};

const cleanupTree = (root, { force = false } = {}) => {
  if (!isElementNode(root)) return;
  if (!force && root.isConnected) return;

  runCleanup(root);
  root.querySelectorAll("*").forEach((el) => {
    if (force || !el.isConnected) {
      runCleanup(el);
    }
  });
};

const initializeElement = (name, { init }, el) => {
  if (isInitialized(el, name)) return;
  try {
    const cleanup = init(el);
    if (typeof cleanup === "function") {
      getComponentState(el).cleanups.set(name, cleanup);
    }
    markInitialized(el, name);
    dispatchInitialized(el, name);
  } catch (err) {
    console.error(`[osspm] Failed to init "${name}":`, err);
  }
};

const initComponent = (name, { selector, init }, root) => {
  root.querySelectorAll(selector).forEach((el) => {
    initializeElement(name, { init }, el);
  });
};

const initAll = (root = document.body) => {
  for (const [name, def] of components) {
    initComponent(name, def, root);
  }
};

const startObserver = () => {
  if (observer) return;
  observer = new MutationObserver((mutations) => {
    for (const mutation of mutations) {
      for (const node of mutation.removedNodes) {
        cleanupTree(node);
      }

      for (const node of mutation.addedNodes) {
        if (node instanceof HTMLElement) {
          initAll(node);
          // Also check the node itself
          for (const [name, def] of components) {
            if (node.matches(def.selector)) {
              initializeElement(name, def, node);
            }
          }
        }
      }
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });
};

export const register = (name, selector, init) => {
  components.set(name, { selector, init });
};

export const start = () => {
  initAll();
  startObserver();
};

export const stop = () => {
  if (observer) {
    observer.disconnect();
    observer = null;
  }
  cleanupTree(document.body, { force: true });
};
