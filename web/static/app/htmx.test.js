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
      <div id="busy-indicator" data-htmx-busy-indicator hidden aria-hidden="true"></div>
    `;

    const target = document.getElementById("target");
    const region = document.getElementById("main");
    const indicator = document.getElementById("busy-indicator");
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
    expect(document.documentElement.dataset.htmxBusy).toBe("true");
    expect(indicator.hidden).toBe(false);
    expect(indicator.getAttribute("aria-hidden")).toBe("false");

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
    expect(document.documentElement.dataset.htmxBusy).toBe("false");
    expect(indicator.hidden).toBe(true);
    expect(indicator.getAttribute("aria-hidden")).toBe("true");
  });

  it("cleans up busy state when swap completes before/without afterRequest", () => {
    document.body.innerHTML = `
      <main id="main" data-main-content data-busy-region>
        <div id="target"></div>
      </main>
      <div id="busy-indicator" data-htmx-busy-indicator hidden aria-hidden="true"></div>
    `;

    const target = document.getElementById("target");
    const region = document.getElementById("main");
    const indicator = document.getElementById("busy-indicator");
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

    expect(region.getAttribute("aria-busy")).toBe("true");
    expect(document.documentElement.dataset.htmxBusy).toBe("true");

    target.dispatchEvent(
      new CustomEvent("htmx:afterSwap", {
        bubbles: true,
        detail: { xhr },
      }),
    );

    expect(target.getAttribute("aria-busy")).toBe("false");
    expect(region.getAttribute("aria-busy")).toBe("false");
    expect(document.documentElement.dataset.htmxBusy).toBe("false");
    expect(indicator.hidden).toBe(true);
  });

  it("cleans up busy state when afterSwap targets a non-HTMLElement", () => {
    document.body.innerHTML = `
      <main id="main" data-main-content data-busy-region>
        <div id="target"></div>
      </main>
      <div id="busy-indicator" data-htmx-busy-indicator hidden aria-hidden="true"></div>
    `;

    const target = document.getElementById("target");
    const region = document.getElementById("main");
    const indicator = document.getElementById("busy-indicator");
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

    expect(region.getAttribute("aria-busy")).toBe("true");
    expect(document.documentElement.dataset.htmxBusy).toBe("true");

    const svgTarget = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    document.body.appendChild(svgTarget);
    svgTarget.dispatchEvent(
      new CustomEvent("htmx:afterSwap", {
        bubbles: true,
        detail: { xhr },
      }),
    );

    expect(target.getAttribute("aria-busy")).toBe("false");
    expect(region.getAttribute("aria-busy")).toBe("false");
    expect(document.documentElement.dataset.htmxBusy).toBe("false");
    expect(indicator.hidden).toBe(true);
  });

  it("ignores cancelled beforeRequest events", () => {
    document.body.innerHTML = `
      <main id="main" data-main-content data-busy-region>
        <div id="target"></div>
      </main>
      <div id="busy-indicator" data-htmx-busy-indicator hidden aria-hidden="true"></div>
    `;

    const target = document.getElementById("target");
    const region = document.getElementById("main");
    const indicator = document.getElementById("busy-indicator");
    const xhr = new XMLHttpRequest();
    const cancelled = new CustomEvent("htmx:beforeRequest", {
      cancelable: true,
      detail: {
        xhr,
        target,
        elt: target,
        requestConfig: {},
      },
    });
    cancelled.preventDefault();

    document.dispatchEvent(cancelled);

    expect(target.getAttribute("aria-busy")).toBeNull();
    expect(region.getAttribute("aria-busy")).toBeNull();
    expect(document.documentElement.dataset.htmxBusy).toBe("false");
    expect(indicator.hidden).toBe(true);
    expect(indicator.getAttribute("aria-hidden")).toBe("true");
  });

  it("cleans up busy state on abort/error lifecycle events", () => {
    document.body.innerHTML = `
      <main id="main" data-main-content data-busy-region>
        <div id="target"></div>
      </main>
      <div id="busy-indicator" data-htmx-busy-indicator hidden aria-hidden="true"></div>
    `;

    const target = document.getElementById("target");
    const region = document.getElementById("main");
    const indicator = document.getElementById("busy-indicator");

    const assertBusyCleared = () => {
      expect(target.getAttribute("aria-busy")).toBe("false");
      expect(region.getAttribute("aria-busy")).toBe("false");
      expect(document.documentElement.dataset.htmxBusy).toBe("false");
      expect(indicator.hidden).toBe(true);
      expect(indicator.getAttribute("aria-hidden")).toBe("true");
    };

    ["htmx:sendAbort", "htmx:sendError", "htmx:timeout", "htmx:responseError", "htmx:onLoadError", "htmx:swapError"].forEach((eventName) => {
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

      expect(document.documentElement.dataset.htmxBusy).toBe("true");

      document.dispatchEvent(
        new CustomEvent(eventName, {
          detail: { xhr },
        }),
      );

      assertBusyCleared();
    });
  });

  it("marks containing cards as busy for in-card HTMX requests", () => {
    document.body.innerHTML = `
      <article id="card" class="card">
        <form id="form"></form>
      </article>
    `;

    const card = document.getElementById("card");
    const form = document.getElementById("form");
    const xhr = new XMLHttpRequest();

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr,
          target: form,
          elt: form,
          requestConfig: {},
        },
      }),
    );

    expect(card.getAttribute("aria-busy")).toBe("true");

    document.dispatchEvent(
      new CustomEvent("htmx:afterRequest", {
        detail: {
          xhr,
          failed: false,
        },
      }),
    );

    expect(card.getAttribute("aria-busy")).toBe("false");
  });

  it("focuses command search when pressing Ctrl+K or Cmd+K", () => {
    document.body.innerHTML = `
      <button id="trigger" type="button">Trigger</button>
      <input id="command-search-input" type="text" />
    `;

    const trigger = document.getElementById("trigger");
    const searchInput = document.getElementById("command-search-input");
    trigger.focus();

    const ctrlKEvent = new KeyboardEvent("keydown", {
      key: "k",
      ctrlKey: true,
      cancelable: true,
    });
    document.dispatchEvent(ctrlKEvent);

    expect(ctrlKEvent.defaultPrevented).toBe(true);
    expect(document.activeElement).toBe(searchInput);

    trigger.focus();

    const cmdKEvent = new KeyboardEvent("keydown", {
      key: "k",
      metaKey: true,
      cancelable: true,
    });
    document.dispatchEvent(cmdKEvent);

    expect(cmdKEvent.defaultPrevented).toBe(true);
    expect(document.activeElement).toBe(searchInput);
  });

  it("does not intercept Ctrl+K while focused in text entry fields", () => {
    document.body.innerHTML = `
      <input id="editor" type="text" />
      <input id="command-search-input" type="text" />
    `;

    const editor = document.getElementById("editor");
    editor.focus();

    const ctrlKEvent = new KeyboardEvent("keydown", {
      key: "k",
      ctrlKey: true,
      cancelable: true,
    });
    document.dispatchEvent(ctrlKEvent);

    expect(ctrlKEvent.defaultPrevented).toBe(false);
    expect(document.activeElement).toBe(editor);
  });

  it("does not intercept slash while focused in command search input", () => {
    document.body.innerHTML = `<input id="command-search-input" type="text" />`;

    const searchInput = document.getElementById("command-search-input");
    searchInput.focus();

    const slashEvent = new KeyboardEvent("keydown", {
      key: "/",
      cancelable: true,
    });
    document.dispatchEvent(slashEvent);

    expect(slashEvent.defaultPrevented).toBe(false);
    expect(document.activeElement).toBe(searchInput);
  });

  it("does not intercept Ctrl+K when command search is unavailable", () => {
    document.body.innerHTML = `<button id="trigger" type="button">Trigger</button>`;

    const trigger = document.getElementById("trigger");
    trigger.focus();

    const ctrlKEvent = new KeyboardEvent("keydown", {
      key: "k",
      ctrlKey: true,
      cancelable: true,
    });
    document.dispatchEvent(ctrlKEvent);

    expect(ctrlKEvent.defaultPrevented).toBe(false);
    expect(document.activeElement).toBe(trigger);
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
