import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

import { bindGlobalListenersOnce } from "open-sspm-app/htmx.js";

const waitForAsyncWork = async () => {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 25));
};

describe("htmx integration wiring", () => {
  beforeAll(() => {
    document.documentElement.dataset.openSspmAppListenersBound = "false";
    bindGlobalListenersOnce({ initGlobal: vi.fn() });
  });

  beforeEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
  });

  it("increments and decrements busy state around request lifecycle", () => {
    document.body.innerHTML = `
      <main id="main" data-main-content data-busy-region>
        <div id="target"></div>
      </main>
    `;

    const target = document.getElementById("target");
    const region = document.getElementById("main");
    const xhr = new XMLHttpRequest();

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr,
          target,
          elt: target,
          requestConfig: {},
        },
      }),
    );

    expect(target.getAttribute("aria-busy")).toBe("true");
    expect(region.getAttribute("aria-busy")).toBe("true");

    document.dispatchEvent(
      new CustomEvent("htmx:afterRequest", {
        detail: {
          xhr,
          failed: true,
        },
      }),
    );

    expect(target.getAttribute("aria-busy")).toBe("false");
    expect(region.getAttribute("aria-busy")).toBe("false");
  });

  it("applies main-content focus strategy after page-level swap", async () => {
    document.body.innerHTML = `
      <main id="main" data-main-content data-busy-region></main>
      <button id="trigger" type="button">Trigger</button>
    `;

    const main = document.getElementById("main");
    const trigger = document.getElementById("trigger");
    const xhr = new XMLHttpRequest();

    trigger.focus();

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr,
          target: document.body,
          elt: document.body,
          requestConfig: {},
        },
      }),
    );

    document.body.dispatchEvent(
      new CustomEvent("htmx:afterSwap", {
        bubbles: true,
        detail: { xhr },
      }),
    );

    await waitForAsyncWork();

    expect(main.getAttribute("tabindex")).toBe("-1");
    expect(document.activeElement).toBe(main);
  });

  it("restores focus to matching element after fragment swap", async () => {
    document.body.innerHTML = `
      <main data-main-content data-busy-region>
        <section id="swap-target">
          <input id="old" name="query" type="text" />
        </section>
      </main>
    `;

    const target = document.getElementById("swap-target");
    const oldInput = document.getElementById("old");
    const xhr = new XMLHttpRequest();

    oldInput.focus();

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr,
          target,
          elt: target,
          requestConfig: {},
        },
      }),
    );

    target.innerHTML = `<input id="new" name="query" type="text" />`;

    target.dispatchEvent(
      new CustomEvent("htmx:afterSwap", {
        bubbles: true,
        detail: { xhr },
      }),
    );

    await waitForAsyncWork();

    expect(document.activeElement).toBe(document.getElementById("new"));
  });
});
