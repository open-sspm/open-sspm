import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  initTableQueryBar,
  positionFloatingPanel,
} from "open-sspm-app/components/table_query_bar.js";

const renderBar = ({ htmx = false } = {}) => {
  document.body.innerHTML = `
    <form id="filters" ${htmx ? 'hx-get="/items"' : ""}>
      <div data-table-query-bar>
        <div>
          <label>
            <input type="search" name="q" value="applied" />
          </label>
          <button type="button" data-table-query-clear-search>Clear query</button>
          <button type="button" data-table-query-clear-filters>Clear filters</button>
          <button type="button" data-table-query-add-trigger>Add filter</button>
        </div>
        <div data-table-query-chip-list>
          <div data-table-query-chip data-field-id="status">
            <button type="button" data-table-query-chip-trigger data-field-id="status">Status: Active</button>
            <button type="button" data-table-query-chip-remove data-field-id="status">Remove</button>
          </div>
        </div>
        <div data-table-query-controls data-table-query-trigger>
          <input type="hidden" name="page" value="3" data-table-query-control data-table-query-control-name="page" />
          <input type="hidden" name="status" value="active" data-table-query-control data-table-query-control-name="status" />
        </div>
        <div data-table-query-fields>
          <div data-table-query-field data-field-id="status" data-field-label="Status" data-field-kind="single_select" data-field-active-value="active">
            <input type="hidden" value="status" data-table-query-input-name />
            <div data-table-query-option data-option-value="active" data-option-label="Active">
              <input type="hidden" value="active" data-control-name="status" data-table-query-option-control />
            </div>
            <div data-table-query-option data-option-value="inactive" data-option-label="Inactive">
              <input type="hidden" value="inactive" data-control-name="status" data-table-query-option-control />
            </div>
          </div>
          <div data-table-query-field data-field-id="source_kind" data-field-label="Source" data-field-kind="single_select" data-field-active-value="" data-field-picker-visible>
            <input type="hidden" value="source_kind" data-table-query-input-name />
            <div data-table-query-option data-option-value="github" data-option-label="GitHub">
              <input type="hidden" value="github" data-control-name="source_kind" data-table-query-option-control />
            </div>
            <div data-table-query-option data-option-value="google" data-option-label="Google Workspace">
              <input type="hidden" value="google" data-control-name="source_kind" data-table-query-option-control />
            </div>
          </div>
          <div data-table-query-field data-field-id="type" data-field-label="Type" data-field-kind="single_select" data-field-active-value="" data-field-picker-visible>
            <input type="hidden" value="type" data-table-query-input-name />
            <div data-table-query-option data-option-value="service" data-option-label="Service">
              <input type="hidden" value="service" data-control-name="type" data-table-query-option-control />
            </div>
          </div>
          <div data-table-query-field data-field-id="privileged" data-field-label="Privileged access" data-field-kind="boolean" data-field-active-value="" data-field-picker-visible>
            <input type="hidden" value="privileged" data-table-query-input-name />
            <div data-table-query-option data-option-value="yes" data-option-label="Yes">
              <input type="hidden" value="1" data-control-name="privileged" data-table-query-option-control />
            </div>
          </div>
        </div>
        <div hidden data-table-query-picker data-state="closed">
          <input type="search" data-table-query-picker-search-input />
          <div data-table-query-picker-list>
            <button type="button" data-table-query-picker-item data-field-id="source_kind">
              <span class="osspm-table-query-picker-item-label">Source</span>
              <span class="osspm-table-query-picker-item-action">Add</span>
            </button>
            <button type="button" data-table-query-picker-item data-field-id="type">
              <span class="osspm-table-query-picker-item-label">Type</span>
              <span class="osspm-table-query-picker-item-action">Add</span>
            </button>
            <button type="button" data-table-query-picker-item data-field-id="privileged">
              <span class="osspm-table-query-picker-item-label">Privileged access</span>
              <span class="osspm-table-query-picker-item-action">Add</span>
            </button>
          </div>
        </div>
        <div hidden data-table-query-editor data-state="closed">
          <h3 data-table-query-editor-title></h3>
          <button type="button" data-table-query-editor-close>Close</button>
          <select data-table-query-editor-select></select>
        </div>
      </div>
      <button type="submit">Submit</button>
    </form>
  `;

  return document.querySelector("[data-table-query-bar]");
};

describe("table query bar", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(window, "requestAnimationFrame").mockImplementation((cb) => {
      cb();
      return 1;
    });
  });

  afterEach(() => {
    vi.runOnlyPendingTimers();
    vi.useRealTimers();
    vi.restoreAllMocks();
    document.body.innerHTML = "";
  });

  it("filters add-filter items from the picker search input", () => {
    const root = renderBar();
    initTableQueryBar(root);

    root.querySelector("[data-table-query-add-trigger]").click();
    const pickerSearch = root.querySelector("[data-table-query-picker-search-input]");
    const items = root.querySelectorAll("[data-table-query-picker-item]");

    pickerSearch.value = "typ";
    pickerSearch.dispatchEvent(new Event("input", { bubbles: true }));

    expect(items[0].hidden).toBe(true);
    expect(items[1].hidden).toBe(false);

    pickerSearch.value = "add";
    pickerSearch.dispatchEvent(new Event("input", { bubbles: true }));

    expect(items[0].hidden).toBe(true);
    expect(items[1].hidden).toBe(true);
    expect(items[2].hidden).toBe(true);
  });

  it("opens the chip editor and restores focus on escape", () => {
    const root = renderBar();
    initTableQueryBar(root);

    const trigger = root.querySelector("[data-table-query-chip-trigger]");
    const editor = root.querySelector("[data-table-query-editor]");

    trigger.click();

    expect(editor.hidden).toBe(false);
    expect(root.querySelector("[data-table-query-editor-title]").textContent).toBe(
      "Status",
    );

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    vi.runAllTimers();

    expect(editor.hidden).toBe(true);
    expect(document.activeElement).toBe(trigger);
  });

  it("dispatches a change event for HTMX-backed filter updates", () => {
    const root = renderBar({ htmx: true });
    initTableQueryBar(root);

    const form = document.getElementById("filters");
    const bank = root.querySelector("[data-table-query-controls]");
    const requestSubmit = vi.spyOn(form, "requestSubmit");
    const changeSpy = vi.fn();
    bank.addEventListener("change", changeSpy);

    root.querySelector('[data-table-query-picker-item][data-field-id="source_kind"]').click();
    expect(root.querySelector("[data-table-query-draft-chip]")).not.toBeNull();
    const editorSelect = root.querySelector("[data-table-query-editor-select]");
    editorSelect.value = "github";
    editorSelect.dispatchEvent(new Event("change", { bubbles: true }));

    expect(changeSpy).toHaveBeenCalledTimes(1);
    expect(requestSubmit).not.toHaveBeenCalled();
    expect(root.querySelector("[data-table-query-draft-chip]")).toBeNull();
    expect(
      bank.querySelector('input[data-table-query-control-name="source_kind"]').value,
    ).toBe("github");
  });

  it("creates a draft chip and leaves add-filter editors unselected until the user chooses a value", () => {
    const root = renderBar();
    initTableQueryBar(root);
    const form = document.getElementById("filters");
    vi.spyOn(form, "requestSubmit").mockImplementation(() => {});

    root.querySelector("[data-table-query-add-trigger]").click();
    root
      .querySelector('[data-table-query-picker-item][data-field-id="privileged"]')
      .click();

    const draftChip = root.querySelector("[data-table-query-draft-chip]");
    expect(draftChip).not.toBeNull();
    expect(draftChip?.textContent).toContain("Privileged access");

    const editorSelect = root.querySelector("[data-table-query-editor-select]");
    expect(editorSelect.value).toBe("");
    expect(editorSelect.options[0].disabled).toBe(true);
    expect(document.activeElement).toBe(editorSelect);

    editorSelect.value = "yes";
    editorSelect.dispatchEvent(new Event("change", { bubbles: true }));

    expect(
      root.querySelector('input[data-table-query-control-name="privileged"]').value,
    ).toBe("1");
  });

  it("removes an empty draft chip on escape and restores focus to add filter", () => {
    const root = renderBar();
    initTableQueryBar(root);

    const addTrigger = root.querySelector("[data-table-query-add-trigger]");
    addTrigger.click();
    root.querySelector('[data-table-query-picker-item][data-field-id="source_kind"]').click();

    expect(root.querySelector("[data-table-query-draft-chip]")).not.toBeNull();

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" }));
    vi.runAllTimers();

    expect(root.querySelector("[data-table-query-draft-chip]")).toBeNull();
    expect(document.activeElement).toBe(addTrigger);
  });

  it("replaces the previous empty draft chip when choosing another field", () => {
    const root = renderBar();
    initTableQueryBar(root);

    root.querySelector("[data-table-query-add-trigger]").click();
    root.querySelector('[data-table-query-picker-item][data-field-id="source_kind"]').click();

    let draftChip = root.querySelector("[data-table-query-draft-chip]");
    expect(draftChip?.textContent).toContain("Source");

    root.querySelector("[data-table-query-add-trigger]").click();
    root.querySelector('[data-table-query-picker-item][data-field-id="type"]').click();

    const draftChips = root.querySelectorAll("[data-table-query-draft-chip]");
    expect(draftChips).toHaveLength(1);
    draftChip = draftChips[0];
    expect(draftChip.textContent).toContain("Type");
  });

  it("submits plain forms with the committed query value on filter changes", () => {
    const root = renderBar();
    initTableQueryBar(root);

    const form = document.getElementById("filters");
    const searchInput = form.querySelector('input[type="search"][name="q"]');
    searchInput.value = "draft";

    const requestSubmit = vi.spyOn(form, "requestSubmit").mockImplementation(() => {});

    root.querySelector("[data-table-query-chip-trigger]").click();
    const editorSelect = root.querySelector("[data-table-query-editor-select]");
    editorSelect.value = "inactive";
    editorSelect.dispatchEvent(new Event("change", { bubbles: true }));

    expect(searchInput.value).toBe("applied");
    expect(requestSubmit).toHaveBeenCalledTimes(1);
  });

  it("dispatches change-based clear-search updates for HTMX forms", () => {
    const root = renderBar({ htmx: true });
    initTableQueryBar(root);

    const form = document.getElementById("filters");
    const bank = root.querySelector("[data-table-query-controls]");
    const searchInput = form.querySelector('input[type="search"][name="q"]');
    const requestSubmit = vi.spyOn(form, "requestSubmit");
    const changeSpy = vi.fn();
    bank.addEventListener("change", changeSpy);

    root.querySelector("[data-table-query-add-trigger]").click();
    root.querySelector('[data-table-query-picker-item][data-field-id="type"]').click();

    root.querySelector("[data-table-query-clear-search]").click();

    expect(changeSpy).toHaveBeenCalledTimes(1);
    expect(requestSubmit).not.toHaveBeenCalled();
    expect(root.querySelector("[data-table-query-draft-chip]")).toBeNull();
    expect(searchInput.value).toBe("");
    expect(searchInput.defaultValue).toBe("");
    expect(bank.querySelector('input[name="status"]')?.value).toBe("active");
    expect(bank.querySelector('input[name="page"]')).toBeNull();
  });

  it("removes any empty draft chip before clearing filters", () => {
    const root = renderBar();
    initTableQueryBar(root);

    const form = document.getElementById("filters");
    const serializedSubmissions = [];
    vi.spyOn(form, "requestSubmit").mockImplementation(() => {
      serializedSubmissions.push(new URLSearchParams(new FormData(form)).toString());
    });

    root.querySelector("[data-table-query-add-trigger]").click();
    root.querySelector('[data-table-query-picker-item][data-field-id="type"]').click();

    root.querySelector("[data-table-query-clear-filters]").click();

    expect(root.querySelector("[data-table-query-draft-chip]")).toBeNull();
    expect(serializedSubmissions).toEqual(["q=applied"]);
  });

  it("submits plain clear-search requests without an empty q parameter", () => {
    const root = renderBar();
    initTableQueryBar(root);

    const form = document.getElementById("filters");
    const serializedSubmissions = [];
    vi.spyOn(form, "requestSubmit").mockImplementation(() => {
      serializedSubmissions.push(new URLSearchParams(new FormData(form)).toString());
    });

    root.querySelector("[data-table-query-clear-search]").click();

    expect(serializedSubmissions).toEqual(["status=active"]);
    expect(form.querySelector('input[type="search"]')?.getAttribute("name")).toBe("q");
    expect(root.querySelector('[data-table-query-controls] input[name="page"]')).toBeNull();
  });

  it("keeps floating panels inside the viewport", () => {
    const panel = document.createElement("div");
    const anchor = document.createElement("button");
    document.body.append(panel, anchor);

    Object.defineProperty(window, "innerWidth", { configurable: true, value: 320 });
    Object.defineProperty(window, "innerHeight", { configurable: true, value: 240 });

    panel.getBoundingClientRect = () => ({
      width: 180,
      height: 120,
      top: 0,
      left: 0,
      right: 180,
      bottom: 120,
      x: 0,
      y: 0,
      toJSON: () => ({}),
    });
    anchor.getBoundingClientRect = () => ({
      width: 40,
      height: 24,
      top: 200,
      left: 300,
      right: 340,
      bottom: 224,
      x: 300,
      y: 200,
      toJSON: () => ({}),
    });

    positionFloatingPanel(panel, anchor, { align: "end" });

    expect(parseFloat(panel.style.left)).toBeLessThanOrEqual(132);
    expect(parseFloat(panel.style.top)).toBeLessThanOrEqual(112);
  });
});
