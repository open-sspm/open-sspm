import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { bindViewTransitionPolicyOnce } from "open-sspm-app/view_transitions.js";

const reducedMotion = (matches) => {
  Object.defineProperty(window, "matchMedia", {
    configurable: true,
    value: () => ({ matches }),
  });
};

const fireBeforeTransition = (detail) => {
  const event = new CustomEvent("htmx:beforeTransition", {
    bubbles: true,
    cancelable: true,
    detail,
  });
  document.dispatchEvent(event);
  return event;
};

describe("view transition policy", () => {
  const originalMatchMedia = Object.getOwnPropertyDescriptor(window, "matchMedia");

  beforeAll(() => {
    delete document.documentElement.dataset.openSspmViewTransitionPolicyBound;
    bindViewTransitionPolicyOnce();
  });

  beforeEach(() => {
    document.body.innerHTML = '<main id="main"></main><section id="panel"></section>';
    reducedMotion(false);
  });

  afterAll(() => {
    if (originalMatchMedia) {
      Object.defineProperty(window, "matchMedia", originalMatchMedia);
    } else {
      delete window.matchMedia;
    }
  });

  it("allows a boosted navigation that swaps the app main region", () => {
    const event = fireBeforeTransition({
      boosted: true,
      target: document.getElementById("main"),
    });

    expect(event.defaultPrevented).toBe(false);
  });

  it("blocks boosted fragment updates", () => {
    const event = fireBeforeTransition({
      boosted: true,
      target: document.getElementById("panel"),
    });

    expect(event.defaultPrevented).toBe(true);
  });

  it("blocks app navigation when reduced motion is preferred", () => {
    reducedMotion(true);

    const event = fireBeforeTransition({
      boosted: true,
      target: document.getElementById("main"),
    });

    expect(event.defaultPrevented).toBe(true);
  });

  it("registers the policy on the supplied document", () => {
    const root = document.implementation.createHTMLDocument();
    bindViewTransitionPolicyOnce(root);

    const event = new CustomEvent("htmx:beforeTransition", {
      cancelable: true,
      detail: { boosted: false },
    });
    root.dispatchEvent(event);

    expect(root.documentElement.dataset.openSspmViewTransitionPolicyBound).toBe("true");
    expect(event.defaultPrevented).toBe(true);
  });
});
