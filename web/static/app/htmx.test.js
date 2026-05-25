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

  it("preserves the committed query on change-triggered enter-only forms", () => {
    document.body.innerHTML = `
      <form id="filters" data-enter-only-query="q">
        <input id="query" type="search" name="q" value="applied" />
        <select name="status">
          <option value="">All</option>
          <option value="active">Active</option>
        </select>
      </form>
    `;

    const form = document.getElementById("filters");
    const query = document.getElementById("query");
    query.value = "draft";

    const detail = {
      elt: form,
      parameters: {
        q: query.value,
        status: "active",
      },
      triggeringEvent: new Event("change"),
    };

    document.dispatchEvent(new CustomEvent("htmx:configRequest", { detail }));

    expect(detail.parameters.q).toBe("applied");
    expect(detail.parameters.status).toBe("active");
  });

  it("adds the CSRF meta token to every HTMX request", () => {
    document.head.innerHTML = `<meta name="csrf-token" content="csrf-123" />`;

    const detail = {
      headers: {},
      parameters: {},
      triggeringEvent: new Event("submit"),
    };

    document.dispatchEvent(new CustomEvent("htmx:configRequest", { detail }));

    expect(detail.headers["X-CSRF-Token"]).toBe("csrf-123");
  });

  it("allows validation fragments to swap on 422 and 409 responses", () => {
    const detail = {
      shouldSwap: false,
      isError: true,
      xhr: {
        status: 422,
        responseText: `<div role="alert">Fix this field</div>`,
        getResponseHeader: (name) => (name === "Content-Type" ? "text/html; charset=utf-8" : ""),
      },
    };

    document.dispatchEvent(new CustomEvent("htmx:beforeSwap", { detail }));

    expect(detail.shouldSwap).toBe(true);
    expect(detail.isError).toBe(false);
  });

  it("does not swap plain-text error bodies into HTMX targets", () => {
    const detail = {
      shouldSwap: false,
      isError: true,
      xhr: {
        status: 400,
        responseText: "unexpected HTMX target",
        getResponseHeader: (name) => (name === "Content-Type" ? "text/plain; charset=utf-8" : ""),
      },
    };

    document.dispatchEvent(new CustomEvent("htmx:beforeSwap", { detail }));

    expect(detail.shouldSwap).toBe(false);
    expect(detail.isError).toBe(true);
  });

  it("still swaps invalid-login HTML fragments on 401 responses", () => {
    const detail = {
      shouldSwap: false,
      isError: true,
      xhr: {
        status: 401,
        responseText: `<div id="login-form-shell"><div role="alert">Invalid email or password.</div></div>`,
        getResponseHeader: (name) => (name === "Content-Type" ? "text/html; charset=utf-8" : ""),
      },
    };

    document.dispatchEvent(new CustomEvent("htmx:beforeSwap", { detail }));

    expect(detail.shouldSwap).toBe(true);
    expect(detail.isError).toBe(false);
  });

  it("removes empty committed queries from change-triggered enter-only forms", () => {
    document.body.innerHTML = `
      <form id="filters" data-enter-only-query="q">
        <input id="query" type="search" name="q" value="" />
        <select name="status">
          <option value="">All</option>
          <option value="active">Active</option>
        </select>
      </form>
    `;

    const form = document.getElementById("filters");
    const query = document.getElementById("query");
    query.value = "draft";

    const detail = {
      elt: form,
      parameters: {
        q: query.value,
        status: "active",
      },
      triggeringEvent: new Event("change"),
    };

    document.dispatchEvent(new CustomEvent("htmx:configRequest", { detail }));

    expect("q" in detail.parameters).toBe(false);
    expect(detail.parameters.status).toBe("active");
  });

  it("keeps the live query on submit-triggered enter-only forms", () => {
    document.body.innerHTML = `
      <form id="filters" data-enter-only-query="q">
        <input id="query" type="search" name="q" value="applied" />
      </form>
    `;

    const form = document.getElementById("filters");
    const query = document.getElementById("query");
    query.value = "draft";

    const detail = {
      elt: form,
      parameters: {
        q: query.value,
      },
      triggeringEvent: new Event("submit"),
    };

    document.dispatchEvent(new CustomEvent("htmx:configRequest", { detail }));

    expect(detail.parameters.q).toBe("draft");
  });

  it("does not change queries for forms without enter-only query handling", () => {
    document.body.innerHTML = `
      <form id="filters">
        <input id="query" type="search" name="q" value="applied" />
        <select name="status">
          <option value="">All</option>
          <option value="active">Active</option>
        </select>
      </form>
    `;

    const form = document.getElementById("filters");
    const query = document.getElementById("query");
    query.value = "draft";

    const detail = {
      elt: form,
      parameters: {
        q: query.value,
        status: "active",
      },
      triggeringEvent: new Event("change"),
    };

    document.dispatchEvent(new CustomEvent("htmx:configRequest", { detail }));

    expect(detail.parameters.q).toBe("draft");
    expect(detail.parameters.status).toBe("active");
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

  it("clears mirrored aria-busy from outerHTML replacement targets", () => {
    document.body.innerHTML = `
      <main id="main" data-main-content data-busy-region>
        <div id="target">
          <div data-busy-inline-indicator>Updating results...</div>
        </div>
      </main>
      <div id="busy-indicator" data-htmx-busy-indicator hidden aria-hidden="true"></div>
    `;

    const oldTarget = document.getElementById("target");
    const region = document.getElementById("main");
    const xhr = new XMLHttpRequest();

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr,
          target: oldTarget,
          elt: oldTarget,
          requestConfig: {},
        },
      }),
    );

    expect(oldTarget.getAttribute("aria-busy")).toBe("true");
    expect(region.getAttribute("aria-busy")).toBe("true");

    oldTarget.outerHTML = `
      <div id="target" aria-busy="true">
        <div data-busy-inline-indicator>Updating results...</div>
      </div>
    `;

    const newTarget = document.getElementById("target");
    newTarget.dispatchEvent(
      new CustomEvent("htmx:afterSwap", {
        bubbles: true,
        detail: { xhr },
      }),
    );

    expect(newTarget.getAttribute("aria-busy")).toBe("false");
    expect(region.getAttribute("aria-busy")).toBe("false");
    expect(document.documentElement.dataset.htmxBusy).toBe("false");
  });

  it("clears stale aria-busy from live replacements when the original target finalizes elsewhere", () => {
    document.body.innerHTML = `
      <main id="main" data-main-content data-busy-region>
        <div id="target">
          <div data-busy-inline-indicator>Updating results...</div>
        </div>
      </main>
      <div id="busy-indicator" data-htmx-busy-indicator hidden aria-hidden="true"></div>
    `;

    const oldTarget = document.getElementById("target");
    const xhr = new XMLHttpRequest();

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr,
          target: oldTarget,
          elt: oldTarget,
          requestConfig: {},
        },
      }),
    );

    oldTarget.outerHTML = `
      <div id="target" aria-busy="true">
        <div data-busy-inline-indicator>Updating results...</div>
      </div>
    `;

    const newTarget = document.getElementById("target");
    expect(newTarget.getAttribute("aria-busy")).toBe("true");

    document.dispatchEvent(
      new CustomEvent("htmx:afterRequest", {
        detail: {
          xhr,
          failed: false,
        },
      }),
    );

    expect(newTarget.getAttribute("aria-busy")).toBe("false");
    expect(document.documentElement.dataset.htmxBusy).toBe("false");
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

  it("falls back to a generic failure toast only when the exact toast trigger is absent", () => {
    const listener = vi.fn();
    document.addEventListener("osspm:toast", listener);

    document.dispatchEvent(
      new CustomEvent("htmx:responseError", {
        detail: {
          xhr: {
            status: 500,
            getResponseHeader: (name) => (name === "HX-Trigger" ? `{"osspm:toast-debug":{}}` : ""),
          },
        },
      }),
    );

    expect(listener).toHaveBeenCalledTimes(1);

    listener.mockClear();
    document.dispatchEvent(
      new CustomEvent("htmx:responseError", {
        detail: {
          xhr: {
            status: 500,
            getResponseHeader: (name) => (name === "HX-Trigger" ? `{"osspm:toast":{}}` : ""),
          },
        },
      }),
    );

    expect(listener).not.toHaveBeenCalled();
    document.removeEventListener("osspm:toast", listener);
  });

  it("deduplicates managed lazy requests while one is pending", () => {
    document.body.innerHTML = `<section id="lazy" data-hx-lazy-load data-hx-lazy-panel="lazy"></section>`;
    const lazy = document.getElementById("lazy");

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr: new XMLHttpRequest(),
          target: lazy,
          elt: lazy,
          requestConfig: {},
        },
      }),
    );

    const duplicate = new CustomEvent("htmx:beforeRequest", {
      cancelable: true,
      detail: {
        xhr: new XMLHttpRequest(),
        target: lazy,
        elt: lazy,
        requestConfig: {},
      },
    });
    document.dispatchEvent(duplicate);

    expect(duplicate.defaultPrevented).toBe(true);
  });

  it("re-arms failed managed lazy requests", async () => {
    document.body.innerHTML = `<section id="lazy" data-hx-lazy-load data-hx-lazy-panel="lazy"></section>`;
    const lazy = document.getElementById("lazy");
    const trigger = vi.fn();
    window.htmx = { trigger };
    const xhr = new XMLHttpRequest();

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr,
          target: lazy,
          elt: lazy,
          requestConfig: {},
        },
      }),
    );
    document.dispatchEvent(
      new CustomEvent("htmx:responseError", {
        detail: {
          xhr,
        },
      }),
    );

    await waitForAsyncWork();

    expect(lazy.dataset.hxLazyState).toBeUndefined();
    expect(trigger).toHaveBeenCalledWith(lazy, "oss-panel-visible");
    delete window.htmx;
  });

  it("keeps open-only lazy panels dormant until their details panel opens", async () => {
    document.body.innerHTML = `
      <details id="panel">
        <summary>More</summary>
        <section id="lazy" data-hx-lazy-load data-hx-lazy-open-only="true" data-hx-lazy-panel="lazy"></section>
      </details>
    `;
    const details = document.getElementById("panel");
    const lazy = document.getElementById("lazy");
    const trigger = vi.fn();
    window.htmx = { trigger };

    const closedRequest = new CustomEvent("htmx:beforeRequest", {
      cancelable: true,
      detail: {
        xhr: new XMLHttpRequest(),
        target: lazy,
        elt: lazy,
        requestConfig: {},
      },
    });
    document.dispatchEvent(closedRequest);

    expect(closedRequest.defaultPrevented).toBe(true);

    details.open = true;
    details.dispatchEvent(new Event("toggle", { bubbles: true }));
    await waitForAsyncWork();

    expect(trigger).toHaveBeenCalledWith(lazy, "oss-panel-visible");
    delete window.htmx;
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

  it("restores focus to the command search input after command fragment swap", async () => {
    document.body.innerHTML = `
      <main data-main-content data-busy-region>
        <section id="command-root" data-command-root>
          <div class="command header-command">
            <header>
              <input id="command-search-input" name="q" type="text" value="azure" />
            </header>
            <div role="menu">
              <a id="old-action" role="menuitem" href="/identities?q=azure">Search Identities</a>
            </div>
          </div>
        </section>
      </main>
    `;

    const target = document.getElementById("command-root");
    const oldInput = document.getElementById("command-search-input");
    const xhr = new XMLHttpRequest();

    oldInput.focus();

    document.dispatchEvent(
      new CustomEvent("htmx:beforeRequest", {
        detail: {
          xhr,
          target,
          elt: oldInput,
          requestConfig: {},
        },
      }),
    );

    target.outerHTML = `
      <section id="command-root" data-command-root>
        <div class="command header-command">
          <header>
            <input id="command-search-input" name="q" type="text" value="azure" />
          </header>
          <div role="menu">
            <a id="new-action" role="menuitem" href="/identities?q=azure">Search Identities</a>
            <a id="second-action" role="menuitem" href="/app-assets?q=azure">Search App Assets</a>
          </div>
        </div>
      </section>
    `;

    const newTarget = document.getElementById("command-root");
    newTarget.dispatchEvent(
      new CustomEvent("htmx:afterSwap", {
        bubbles: true,
        detail: { xhr },
      }),
    );

    await waitForAsyncWork();

    const newInput = document.getElementById("command-search-input");
    const firstAction = document.getElementById("new-action");

    expect(document.activeElement).toBe(newInput);
    expect(firstAction.getAttribute("role")).toBe("menuitem");
  });
});
