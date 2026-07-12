import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { initAskbar } from "open-sspm-app/components/askbar.js";

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

const renderAskbar = ({ search = "", chips = "", hidden = "", overrides = {}, htmx = true } = {}) => {
  const askbarConfig = { ...config, ...overrides };
  document.body.innerHTML = `
    <form method="get" action="/items" ${htmx ? 'hx-get="/items"' : ""}>
      <div data-osspm-askbar data-osspm-askbar-config='${JSON.stringify(askbarConfig)}'>
        <div data-osspm-askbar-bar>
          <label for="search">Filter items</label>
          <span data-osspm-askbar-chips>${chips}</span>
          <input id="search" type="search" name="q" value="${search}" data-osspm-askbar-input />
          <button type="button" data-osspm-askbar-add-filter aria-expanded="false">Filter</button>
        </div>
        <div data-osspm-askbar-filter-panel hidden role="dialog">
          <input type="search" data-osspm-askbar-filter-input />
          <div data-osspm-askbar-suggest></div>
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
    vi.restoreAllMocks();
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
    expect(root.querySelector(".osspm-askbar-chip-label").textContent).toBe("active");
    expect(bankValues(root)).toEqual({ source_kind: "github", status: "active" });
  });

  it("requests filter-only server suggestions and cancels stale requests", async () => {
    vi.useFakeTimers();
    const signals = [];
    const fetchSpy = vi.fn((_url, options) => {
      signals.push(options.signal);
      return new Promise(() => {});
    });
    Object.defineProperty(window, "fetch", { value: fetchSpy, configurable: true });
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
});
