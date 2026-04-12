/**
 * Open-SSPM Component Registry
 *
 * Auto-initializes UI components when they appear in the DOM.
 * Replaces the Basecoat JS runtime with our own lightweight system.
 */

const components = new Map();
let observer = null;

const initComponent = (name, { selector, init }, root) => {
  root.querySelectorAll(selector).forEach((el) => {
    try {
      init(el);
      el.dataset[`${name}Initialized`] = "true";
      el.dispatchEvent(
        new CustomEvent("osspm:initialized", {
          bubbles: true,
          detail: { component: name },
        }),
      );
    } catch (err) {
      console.error(`[osspm] Failed to init "${name}":`, err);
    }
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
      for (const node of mutation.addedNodes) {
        if (node instanceof HTMLElement) {
          initAll(node);
          // Also check the node itself
          for (const [name, def] of components) {
            if (node.matches(def.selector)) {
              try {
                def.init(node);
                node.dataset[`${name}Initialized`] = "true";
                node.dispatchEvent(
                  new CustomEvent("osspm:initialized", {
                    bubbles: true,
                    detail: { component: name },
                  }),
                );
              } catch (err) {
                console.error(`[osspm] Failed to init "${name}":`, err);
              }
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
};
