import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { initAskbar as initializeAskbar } from "open-sspm-app/components/askbar.js";
import { start, stop } from "open-sspm-app/components/registry.js";

const cleanups = [];
const initAskbar = (root) => {
  const cleanup = initializeAskbar(root);
  cleanups.push(cleanup);
  return cleanup;
};

const deferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
};

const suggestionHTML = (value) =>
  `<button type="button" data-field="status" data-value="${value}" data-label="${value}">${value}</button>`;

const config = {
  fieldParam: { search: "q", status: "status", owner: "owner" },
  keyLabel: { status: "status", owner: "owner" },
  fieldLabel: { status: "Status", owner: "Owner" },
  keywordTokens: {
    active: { field: "status", value: "active", label: "active", tone: "ok" },
    revoked: { field: "status", value: "revoked", label: "revoked", tone: "danger" },
  },
  fieldAliases: { status: "status", owner: "owner" },
  singletonFields: ["status", "owner"],
  freeTextFields: ["owner"],
  staticHidden: [{ name: "source_kind", value: "github" }],
};

const renderAskbar = ({ search = "", chips = "", hidden = "", groups = "", overrides = {}, htmx = true } = {}) => {
  const askbarConfig = { ...config, ...overrides };
  document.body.innerHTML = `
    <form method="get" action="/items" ${htmx ? 'hx-get="/items"' : ""}>
      <div data-osspm-askbar data-osspm-askbar-config='${JSON.stringify(askbarConfig)}'>
        <div data-osspm-askbar-bar>
          <label for="search">Filter items</label>
          <span data-osspm-askbar-chips>${chips}</span>
          <input id="search" type="search" name="q" value="${search}" data-osspm-askbar-input />
          <button type="button" data-osspm-askbar-clear hidden>Clear search</button>
          <button type="button" data-osspm-askbar-add-filter aria-expanded="false">Filter</button>
        </div>
        <div data-osspm-askbar-filter-panel hidden role="dialog">
          <button type="button" data-osspm-askbar-filter-back hidden>Back</button>
          <div data-osspm-askbar-filter-title data-default-title="Suggested filters">Suggested filters</div>
          <button type="button" data-osspm-askbar-filter-close>Close</button>
          <div data-osspm-askbar-filter-search-wrap ${groups ? "hidden" : ""}>
            <input type="search" role="combobox" aria-controls="filter-values" aria-expanded="false" data-osspm-askbar-filter-input />
          </div>
          ${groups ? `<div data-osspm-askbar-filter-groups>${groups}</div>` : ""}
          <div id="filter-values" data-osspm-askbar-suggest role="listbox" ${groups ? "hidden" : ""}></div>
        </div>
        <div data-osspm-askbar-bank>${hidden}</div>
      </div>
    </form>`;
  const root = document.querySelector("[data-osspm-askbar]");
  const form = root.closest("form");
  form.addEventListener("submit", (event) => event.preventDefault());
  return root;
};

const bankValues = (root) =>
  Object.fromEntries(
    Array.from(root.querySelectorAll("[data-osspm-askbar-bank] input"), (input) => [
      input.name,
      input.value,
    ]),
  );

describe("askbar", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    window.history.replaceState({}, "", "/items");
  });

  afterEach(() => {
    stop();
    cleanups.splice(0).forEach((cleanup) => cleanup());
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it("submits free text as a native named GET control without JavaScript", () => {
    const root = renderAskbar({ search: "alice" });
    const data = new FormData(root.closest("form"));

    expect(data.get("q")).toBe("alice");
    expect(root.querySelector('[data-osspm-askbar-input][name="q"]')).not.toBeNull();
  });

  it("never duplicates q in the enhanced hidden-input bank", () => {
    const root = renderAskbar({
      search: "alice",
      hidden: '<input type="hidden" name="q" value="legacy"><input type="hidden" name="source_name" value="primary">',
    });
    initAskbar(root);

    expect(root.querySelector('[data-osspm-askbar-bank] input[name="q"]')).toBeNull();
    expect(bankValues(root)).toEqual({ source_kind: "github", source_name: "primary" });
  });

  it("marks its form so draft text is only applied on submit", () => {
    const root = renderAskbar();

    initAskbar(root);

    expect(root.closest("form").dataset.enterOnlyQuery).toBe("q");
  });

  it("submits trimmed free text when Enter is pressed", () => {
    const root = renderAskbar();
    const form = root.closest("form");
    const input = root.querySelector("[data-osspm-askbar-input]");
    const submitted = vi.fn();
    form.addEventListener("submit", submitted);
    initAskbar(root);

    input.value = "  alice  ";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));

    expect(input.value).toBe("alice");
    expect(submitted).toHaveBeenCalledOnce();
  });

  it("opens a separate filter dialog and focuses its search field", () => {
    const root = renderAskbar();
    initAskbar(root);

    const button = root.querySelector("[data-osspm-askbar-add-filter]");
    button.click();

    expect(root.querySelector("[data-osspm-askbar-filter-panel]").hidden).toBe(false);
    expect(button.getAttribute("aria-expanded")).toBe("true");
    expect(document.activeElement).toBe(root.querySelector("[data-osspm-askbar-filter-input]"));
    expect(root.querySelectorAll("[data-osspm-askbar-suggest] button").length).toBe(2);
  });

  it("leaves Tab native and closes the filter dialog on Escape", () => {
    const root = renderAskbar();
    initAskbar(root);
    const button = root.querySelector("[data-osspm-askbar-add-filter]");
    const input = root.querySelector("[data-osspm-askbar-filter-input]");
    button.click();

    const tab = new KeyboardEvent("keydown", { key: "Tab", bubbles: true, cancelable: true });
    expect(input.dispatchEvent(tab)).toBe(true);

    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
    expect(root.querySelector("[data-osspm-askbar-filter-panel]").hidden).toBe(true);
    expect(button.getAttribute("aria-expanded")).toBe("false");
    expect(document.activeElement).toBe(button);
  });

  it("navigates grouped filters and their values with arrows, Enter, and Escape", () => {
    const root = renderAskbar({
      htmx: false,
      groups: `
        <button type="button" data-osspm-askbar-filter-group data-field="status" data-query="status:" data-label="Status">Status</button>
        <button type="button" data-osspm-askbar-filter-group data-field="owner" data-query="owner:" data-label="Owner">Owner</button>`,
    });
    initAskbar(root);
    const input = root.querySelector("[data-osspm-askbar-filter-input]");
    const groups = root.querySelector("[data-osspm-askbar-filter-groups]");
    const suggestions = root.querySelector("[data-osspm-askbar-suggest]");

    root.querySelector("[data-osspm-askbar-add-filter]").click();
    expect(groups.hidden).toBe(false);
    const statusGroup = groups.querySelector('[data-field="status"]');
    expect(groups.querySelector('[aria-selected="true"]').textContent).toContain("Status");
    expect(document.activeElement).toBe(statusGroup);

    statusGroup.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true, cancelable: true }));
    expect(groups.querySelector('[aria-selected="true"]').textContent).toContain("Owner");
    expect(document.activeElement).toBe(groups.querySelector('[data-field="owner"]'));

    statusGroup.click();
    expect(input.value).toBe("status:");
    expect(groups.hidden).toBe(true);
    expect(suggestions.hidden).toBe(false);
    expect(suggestions.querySelectorAll("[data-field][data-value]")).toHaveLength(2);
    expect(input.getAttribute("aria-expanded")).toBe("true");

    input.dispatchEvent(new KeyboardEvent("keydown", { key: "ArrowDown", bubbles: true, cancelable: true }));
    expect(suggestions.querySelector('[aria-selected="true"]').dataset.value).toBe("revoked");
    expect(input.getAttribute("aria-activedescendant")).toBe(suggestions.querySelector('[aria-selected="true"]').id);
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true }));
    expect(root.querySelector(".osspm-askbar-chip-label").textContent).toBe("revoked");

    root.querySelector("[data-osspm-askbar-add-filter]").click();
    statusGroup.click();
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
    expect(groups.hidden).toBe(false);
    expect(root.querySelector("[data-osspm-askbar-filter-panel]").hidden).toBe(false);
    expect(root.querySelector("[data-osspm-askbar-filter-search-wrap]").hidden).toBe(true);
    expect(document.activeElement).toBe(statusGroup);
    expect(input.getAttribute("aria-expanded")).toBe("false");
    expect(input.hasAttribute("aria-activedescendant")).toBe(false);
    statusGroup.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
    expect(root.querySelector("[data-osspm-askbar-filter-panel]").hidden).toBe(true);
  });

  it("does not submit the inventory form when Enter has no matching filter", () => {
    const root = renderAskbar();
    initAskbar(root);
    root.querySelector("[data-osspm-askbar-add-filter]").click();
    const input = root.querySelector("[data-osspm-askbar-filter-input]");
    input.value = "no matching filter";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));

    const enter = new KeyboardEvent("keydown", { key: "Enter", bubbles: true, cancelable: true });
    input.dispatchEvent(enter);

    expect(enter.defaultPrevented).toBe(true);
    expect(root.querySelector("[data-osspm-askbar-filter-panel]").hidden).toBe(false);
    expect(bankValues(root)).toEqual({ source_kind: "github" });
  });

  it("focuses the page search with the slash shortcut outside text controls", () => {
    const root = renderAskbar();
    initAskbar(root);
    document.body.focus();

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "/", bubbles: true, cancelable: true }));

    expect(document.activeElement).toBe(root.querySelector("[data-osspm-askbar-input]"));
  });

  it("adds a structured filter, writes its canonical param, and submits", () => {
    const root = renderAskbar({ htmx: false });
    const form = root.closest("form");
    const submitted = vi.fn();
    form.addEventListener("submit", submitted);
    initAskbar(root);
    root.querySelector("[data-osspm-askbar-add-filter]").click();

    root.querySelector('[data-osspm-askbar-suggest] [data-value="revoked"]').click();

    expect(bankValues(root)).toEqual({ source_kind: "github", status: "revoked" });
    expect(root.querySelector(".osspm-askbar-chip-label").textContent).toBe("revoked");
    expect(submitted).toHaveBeenCalledOnce();
  });

  it("emits one HTMX change trigger without also submitting the form", () => {
    const root = renderAskbar();
    const changed = vi.fn();
    const submitted = vi.fn();
    root.querySelector("[data-osspm-askbar-bank]").addEventListener("change", changed);
    root.closest("form").addEventListener("submit", submitted);
    initAskbar(root);
    root.querySelector("[data-osspm-askbar-add-filter]").click();

    root.querySelector('[data-osspm-askbar-suggest] [data-value="active"]').click();

    expect(changed).toHaveBeenCalledOnce();
    expect(submitted).not.toHaveBeenCalled();
  });

  it("removes a filter and keeps focus on native search", () => {
    const root = renderAskbar({
      chips: '<span data-osspm-askbar-chip data-chip-field="status" data-chip-value="active" data-chip-label="active" data-chip-tone="ok"></span>',
      hidden: '<input type="hidden" name="status" value="active">',
    });
    initAskbar(root);

    root.querySelector("[data-osspm-askbar-chip-remove]").click();

    expect(root.querySelector("[data-osspm-askbar-chip]")).toBeNull();
    expect(bankValues(root)).toEqual({ source_kind: "github" });
    expect(document.activeElement).toBe(root.querySelector("[data-osspm-askbar-input]"));
  });

  it("restores search and structured filters from browser history", () => {
    const root = renderAskbar();
    initAskbar(root);
    window.history.pushState({}, "", "/items?q=bob&status=active");

    window.dispatchEvent(new PopStateEvent("popstate"));

    expect(root.querySelector("[data-osspm-askbar-input]").value).toBe("bob");
    expect(root.querySelector("[data-osspm-askbar-input]").defaultValue).toBe("bob");
    expect(root.querySelector("[data-osspm-askbar-clear]").hidden).toBe(false);
    expect(root.querySelector(".osspm-askbar-chip-label").textContent).toBe("active");
    expect(bankValues(root)).toEqual({ source_kind: "github", status: "active" });
  });

  it("keeps filter controls interactive after restoring a cached history snapshot", async () => {
    const root = renderAskbar();
    start();
    const snapshot = root.closest("form").outerHTML;

    document.body.innerHTML = snapshot;
    await Promise.resolve();
    const restored = document.querySelector("[data-osspm-askbar]");
    restored.querySelector("[data-osspm-askbar-add-filter]").click();

    expect(restored.querySelector("[data-osspm-askbar-filter-panel]").hidden).toBe(false);
    expect(document.activeElement).toBe(restored.querySelector("[data-osspm-askbar-filter-input]"));
  });

  it("clears applied free text through a form submission", () => {
    const root = renderAskbar({ search: "alice" });
    const form = root.closest("form");
    const submitted = vi.fn();
    form.addEventListener("submit", submitted);
    initAskbar(root);

    const clear = root.querySelector("[data-osspm-askbar-clear]");
    expect(clear.hidden).toBe(false);
    clear.click();

    expect(root.querySelector("[data-osspm-askbar-input]").value).toBe("");
    expect(clear.hidden).toBe(true);
    expect(document.activeElement).toBe(root.querySelector("[data-osspm-askbar-input]"));
    expect(submitted).toHaveBeenCalledOnce();
  });

  it("requests filter-only server suggestions and cancels stale requests", async () => {
    vi.useFakeTimers();
    const signals = [];
    const fetchSpy = vi.fn((_url, options) => {
      signals.push(options.signal);
      return new Promise(() => {});
    });
    vi.stubGlobal("fetch", fetchSpy);
    const root = renderAskbar({ overrides: { suggestEndpoint: "/askbar/suggestions?scope=state" } });
    initAskbar(root);
    const input = root.querySelector("[data-osspm-askbar-filter-input]");

    root.querySelector("[data-osspm-askbar-add-filter]").click();
    await vi.advanceTimersByTimeAsync(80);
    input.value = "rev";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));
    await vi.advanceTimersByTimeAsync(80);

    expect(fetchSpy).toHaveBeenCalledTimes(2);
    expect(signals[0].aborted).toBe(true);
    const requested = new URL(fetchSpy.mock.calls[1][0]);
    expect(requested.searchParams.get("filters_only")).toBe("1");
    expect(requested.searchParams.get("q")).toBe("rev");
  });

  it("cancels pending suggestion requests when the chooser is closed", async () => {
    vi.useFakeTimers();
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const root = renderAskbar({ overrides: { suggestEndpoint: "/askbar/suggestions?scope=state" } });
    initAskbar(root);
    root.querySelector("[data-osspm-askbar-add-filter]").click();
    root.querySelector("[data-osspm-askbar-filter-close]").click();

    await vi.advanceTimersByTimeAsync(80);

    expect(fetchSpy).not.toHaveBeenCalled();
  });

  it("cancels pending suggestions when returning to filter categories", async () => {
    vi.useFakeTimers();
    const fetchSpy = vi.fn();
    vi.stubGlobal("fetch", fetchSpy);
    const root = renderAskbar({
      groups: '<button type="button" data-osspm-askbar-filter-group data-field="status" data-query="status:" data-label="Status">Status</button>',
      overrides: { suggestEndpoint: "/askbar/suggestions?scope=state" },
    });
    initAskbar(root);
    root.querySelector("[data-osspm-askbar-add-filter]").click();
    root.querySelector("[data-osspm-askbar-filter-group]").click();
    root.querySelector("[data-osspm-askbar-filter-back]").click();

    await vi.advanceTimersByTimeAsync(80);

    expect(fetchSpy).not.toHaveBeenCalled();
    expect(document.activeElement).toBe(root.querySelector("[data-osspm-askbar-filter-group]"));
  });

  it("invalidates an old request as soon as the filter query changes", async () => {
    vi.useFakeTimers();
    const oldRequest = deferred();
    const fetchSpy = vi.fn().mockReturnValueOnce(oldRequest.promise);
    vi.stubGlobal("fetch", fetchSpy);
    const root = renderAskbar({ overrides: { suggestEndpoint: "/askbar/suggestions?scope=state" } });
    initAskbar(root);
    root.querySelector("[data-osspm-askbar-add-filter]").click();
    await vi.advanceTimersByTimeAsync(80);
    const input = root.querySelector("[data-osspm-askbar-filter-input]");
    input.value = "rev";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));

    expect(fetchSpy.mock.calls[0][1].signal.aborted).toBe(true);
    oldRequest.resolve({ ok: true, text: async () => suggestionHTML("active") });
    await vi.advanceTimersByTimeAsync(0);

    expect(root.querySelector("[data-osspm-askbar-suggest]").textContent).toBe("");
  });

  it("does not let a slow old response body replace newer filter values", async () => {
    vi.useFakeTimers();
    const oldBody = deferred();
    const fetchSpy = vi.fn()
      .mockResolvedValueOnce({ ok: true, text: () => oldBody.promise })
      .mockResolvedValueOnce({ ok: true, text: async () => suggestionHTML("revoked") });
    vi.stubGlobal("fetch", fetchSpy);
    const root = renderAskbar({ overrides: { suggestEndpoint: "/askbar/suggestions?scope=state" } });
    initAskbar(root);
    root.querySelector("[data-osspm-askbar-add-filter]").click();
    await vi.advanceTimersByTimeAsync(80);
    const input = root.querySelector("[data-osspm-askbar-filter-input]");
    input.value = "rev";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));
    await vi.advanceTimersByTimeAsync(80);
    oldBody.resolve(suggestionHTML("active"));
    await vi.advanceTimersByTimeAsync(0);

    expect(root.querySelector("[data-osspm-askbar-suggest]").textContent).toBe("revoked");
  });

  it("ignores an old request failure after newer filter values are shown", async () => {
    vi.useFakeTimers();
    const oldRequest = deferred();
    const fetchSpy = vi.fn()
      .mockReturnValueOnce(oldRequest.promise)
      .mockResolvedValueOnce({ ok: true, text: async () => suggestionHTML("revoked") });
    vi.stubGlobal("fetch", fetchSpy);
    const root = renderAskbar({ overrides: { suggestEndpoint: "/askbar/suggestions?scope=state" } });
    initAskbar(root);
    root.querySelector("[data-osspm-askbar-add-filter]").click();
    await vi.advanceTimersByTimeAsync(80);
    const input = root.querySelector("[data-osspm-askbar-filter-input]");
    input.value = "rev";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));
    await vi.advanceTimersByTimeAsync(80);
    oldRequest.reject(new Error("Network failure"));
    await vi.advanceTimersByTimeAsync(0);

    expect(root.querySelector("[data-osspm-askbar-suggest]").textContent).toBe("revoked");
  });
});
