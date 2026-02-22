import { beforeEach, describe, expect, it, vi } from "vitest";

import { wireColumnControls } from "open-sspm-app/columns.js";

const createLocalStorageMock = () => {
  const values = new Map();
  return {
    getItem(key) {
      const normalized = String(key);
      return values.has(normalized) ? values.get(normalized) : null;
    },
    setItem(key, value) {
      values.set(String(key), String(value));
    },
    removeItem(key) {
      values.delete(String(key));
    },
    clear() {
      values.clear();
    },
  };
};

const renderColumnsFixture = ({
  tableID = "table-test",
  tableAttrs = "",
  headers = ["Primary", "Type", "Status"],
  bodyRows = '<tr><td>id-1</td><td>service</td><td>active</td></tr>',
} = {}) => `
  <div id="fixture">
    <div data-columns-root hidden>
      <div class="dropdown-menu" data-columns-control data-columns-for="${tableID}">
        <button type="button" aria-expanded="false" aria-haspopup="menu">Columns</button>
        <div data-popover aria-hidden="true">
          <div role="menu">
            <div role="heading">Visible columns</div>
            <div data-columns-options></div>
            <hr role="separator" />
            <button type="button" role="menuitem" data-columns-reset>Reset to defaults</button>
          </div>
        </div>
      </div>
    </div>
    <table data-columns-id="${tableID}" ${tableAttrs}>
      <thead>
        <tr>
          ${headers.map((label) => `<th>${label}</th>`).join("")}
        </tr>
      </thead>
      <tbody>
        ${bodyRows}
      </tbody>
    </table>
  </div>
`;

const getColumnOptions = () =>
  Array.from(document.querySelectorAll("[data-columns-options] button[data-columns-index]"));

describe("columns", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
    const localStorageMock = createLocalStorageMock();
    Object.defineProperty(window, "localStorage", { value: localStorageMock, configurable: true });
    Object.defineProperty(globalThis, "localStorage", { value: localStorageMock, configurable: true });
    localStorage.clear();
  });

  it("initializes controls only for data-columns tables and unhides the control", () => {
    document.body.innerHTML = renderColumnsFixture();

    wireColumnControls(document);

    const root = document.querySelector("[data-columns-root]");
    expect(root.hidden).toBe(false);
    expect(getColumnOptions()).toHaveLength(3);
  });

  it("keeps fixed columns visible and marks their options disabled", () => {
    document.body.innerHTML = renderColumnsFixture({
      headers: ["Primary", "Actions", "Status"],
    });

    wireColumnControls(document);

    const options = getColumnOptions();
    expect(options[0].getAttribute("aria-disabled")).toBe("true");
    expect(options[0].getAttribute("aria-checked")).toBe("true");
    expect(options[1].getAttribute("aria-disabled")).toBe("true");
    expect(options[1].getAttribute("aria-checked")).toBe("true");
    expect(options[2].getAttribute("aria-disabled")).toBe("false");
  });

  it("hides and shows table columns when toggled", () => {
    document.body.innerHTML = renderColumnsFixture();

    wireColumnControls(document);

    const options = getColumnOptions();
    options[1].click();

    const headerCells = document.querySelectorAll("thead th");
    const bodyCells = document.querySelectorAll("tbody tr:first-child td");
    expect(headerCells[1].hidden).toBe(true);
    expect(bodyCells[1].hidden).toBe(true);

    options[1].click();
    expect(headerCells[1].hidden).toBe(false);
    expect(bodyCells[1].hidden).toBe(false);
  });

  it("persists selected visibility and restores it on re-init", () => {
    document.body.innerHTML = renderColumnsFixture({ tableID: "persist-table" });

    wireColumnControls(document);

    const options = getColumnOptions();
    options[1].click();

    const stored = JSON.parse(localStorage.getItem("openSspm.tableColumns.persist-table"));
    expect(stored.visible).toEqual([1, 3]);
    expect(stored.columnCount).toBe(3);

    document.body.innerHTML = renderColumnsFixture({ tableID: "persist-table" });
    wireColumnControls(document);

    const headerCells = document.querySelectorAll("thead th");
    expect(headerCells[1].hidden).toBe(true);
  });

  it("resets to defaults and clears localStorage", () => {
    document.body.innerHTML = renderColumnsFixture({ tableID: "reset-table" });

    wireColumnControls(document);

    const options = getColumnOptions();
    options[1].click();
    expect(localStorage.getItem("openSspm.tableColumns.reset-table")).not.toBeNull();

    const resetButton = document.querySelector("[data-columns-reset]");
    resetButton.click();

    expect(localStorage.getItem("openSspm.tableColumns.reset-table")).toBeNull();
    const headerCells = document.querySelectorAll("thead th");
    expect(headerCells[1].hidden).toBe(false);
  });

  it("falls back to defaults when stored payload is invalid", () => {
    localStorage.setItem("openSspm.tableColumns.default-table", "{bad json");
    document.body.innerHTML = renderColumnsFixture({
      tableID: "default-table",
      tableAttrs: 'data-columns-default-hidden="2"',
    });

    wireColumnControls(document);

    const headerCells = document.querySelectorAll("thead th");
    expect(headerCells[1].hidden).toBe(true);
  });

  it("updates empty-state colspan to match visible columns", () => {
    document.body.innerHTML = renderColumnsFixture({
      tableID: "empty-table",
      headers: ["Primary", "Type", "Status"],
      bodyRows: '<tr><td colspan="3">No rows</td></tr>',
    });

    wireColumnControls(document);

    const options = getColumnOptions();
    options[1].click();

    const emptyCell = document.querySelector("tbody tr td[colspan]");
    expect(emptyCell.getAttribute("colspan")).toBe("2");
  });

  it("re-applies persisted settings for tables swapped in a fragment root", () => {
    document.body.innerHTML = `<div id="swap-root">${renderColumnsFixture({ tableID: "swap-table" })}</div>`;
    wireColumnControls(document);

    const options = getColumnOptions();
    options[1].click();

    const swapRoot = document.getElementById("swap-root");
    swapRoot.innerHTML = renderColumnsFixture({ tableID: "swap-table" });
    wireColumnControls(swapRoot);

    const headerCells = swapRoot.querySelectorAll("thead th");
    expect(headerCells[1].hidden).toBe(true);
  });

  it("re-applies hidden columns when wiring a swapped table row", () => {
    document.body.innerHTML = renderColumnsFixture({ tableID: "row-swap-table" });
    wireColumnControls(document);

    const options = getColumnOptions();
    options[1].click();

    let bodyCells = document.querySelectorAll("tbody tr:first-child td");
    expect(bodyCells[1].hidden).toBe(true);

    const firstRow = document.querySelector("tbody tr:first-child");
    firstRow.outerHTML = "<tr><td>id-2</td><td>human</td><td>active</td></tr>";

    const swappedRow = document.querySelector("tbody tr:first-child");
    expect(swappedRow.children[1].hidden).toBe(false);

    wireColumnControls(swappedRow);

    bodyCells = document.querySelectorAll("tbody tr:first-child td");
    expect(bodyCells[1].hidden).toBe(true);
  });
});
