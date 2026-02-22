const TABLE_SELECTOR = "table[data-columns-id]";
const STORAGE_PREFIX = "openSspm.tableColumns.";
const STORAGE_VERSION = 1;
const FIXED_HEADER_LABELS = new Set(["", "actions", "action", "link", "details", "view users"]);
const TABLE_STATES = new WeakMap();

const collapseWhitespace = (value) => value.replace(/\s+/g, " ").trim();

const normalizeHeaderLabel = (value) => collapseWhitespace(value).toLowerCase();

const storageKey = (tableID) => `${STORAGE_PREFIX}${tableID}`;

const parseIndexSet = (raw, columnCount) => {
  const indexes = new Set();
  if (typeof raw !== "string" || raw.trim() === "") return indexes;

  raw
    .split(",")
    .map((value) => Number.parseInt(value.trim(), 10))
    .forEach((value) => {
      if (!Number.isInteger(value) || value < 1 || value > columnCount) return;
      indexes.add(value);
    });
  return indexes;
};

const sortNumeric = (values) => [...values].sort((a, b) => a - b);

const readStoredVisibleColumns = (tableID, columnCount) => {
  try {
    const raw = localStorage.getItem(storageKey(tableID));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || parsed.v !== STORAGE_VERSION) return null;
    if (!Number.isInteger(parsed.columnCount) || parsed.columnCount !== columnCount) return null;
    if (!Array.isArray(parsed.visible)) return null;
    return new Set(
      parsed.visible.filter((value) => Number.isInteger(value) && value >= 1 && value <= columnCount),
    );
  } catch (_) {
    return null;
  }
};

const writeStoredVisibleColumns = (tableID, columnCount, visible) => {
  try {
    localStorage.setItem(
      storageKey(tableID),
      JSON.stringify({
        v: STORAGE_VERSION,
        columnCount,
        visible: sortNumeric(visible),
      }),
    );
  } catch (_) {}
};

const clearStoredVisibleColumns = (tableID) => {
  try {
    localStorage.removeItem(storageKey(tableID));
  } catch (_) {}
};

const collectTables = (root) => {
  const tables = [];
  const pushTable = (table) => {
    if (!(table instanceof HTMLTableElement)) return;
    if (!tables.includes(table)) {
      tables.push(table);
    }
  };

  if (root instanceof HTMLTableElement && root.matches(TABLE_SELECTOR)) {
    pushTable(root);
  }
  if (root instanceof Element) {
    pushTable(root.closest(TABLE_SELECTOR));
  }
  if (root instanceof Document || root instanceof DocumentFragment || root instanceof Element) {
    root.querySelectorAll(TABLE_SELECTOR).forEach((table) => {
      pushTable(table);
    });
  }
  return tables;
};

const labelForHeader = (header, index) => {
  if (!(header instanceof HTMLTableCellElement)) return `Column ${index + 1}`;
  const override = header.dataset.columnsLabel;
  if (typeof override === "string" && collapseWhitespace(override) !== "") return collapseWhitespace(override);
  const text = collapseWhitespace(header.textContent || "");
  if (text !== "") return text;
  return `Column ${index + 1}`;
};

const resolveFixedColumns = (table, headers) => {
  const columnCount = headers.length;
  const explicitFixed = parseIndexSet(table.dataset.columnsFixed, columnCount);
  if (explicitFixed.size > 0) {
    explicitFixed.add(1);
    return explicitFixed;
  }

  const fixed = new Set([1]);
  headers.forEach((header, index) => {
    const label = normalizeHeaderLabel((header instanceof HTMLElement ? header.textContent : "") || "");
    if (FIXED_HEADER_LABELS.has(label)) {
      fixed.add(index + 1);
    }
  });
  return fixed;
};

const ensureValidVisible = (candidate, columnCount, fixedColumns) => {
  const visible = new Set();
  candidate.forEach((value) => {
    if (Number.isInteger(value) && value >= 1 && value <= columnCount) {
      visible.add(value);
    }
  });
  fixedColumns.forEach((value) => visible.add(value));
  if (visible.size === 0) {
    visible.add(1);
  }
  return visible;
};

const defaultVisibleColumns = (columnCount, defaultHidden, fixedColumns) => {
  const visible = new Set();
  for (let index = 1; index <= columnCount; index += 1) {
    if (!defaultHidden.has(index)) {
      visible.add(index);
    }
  }
  return ensureValidVisible(visible, columnCount, fixedColumns);
};

const applyColumnVisibility = (table, visibleColumns) => {
  table.querySelectorAll("tr").forEach((row) => {
    if (!(row instanceof HTMLTableRowElement)) return;
    const cells = Array.from(row.children).filter(
      (child) => child instanceof HTMLTableCellElement,
    );
    if (cells.length === 1 && cells[0].hasAttribute("colspan")) return;

    let columnCursor = 1;
    cells.forEach((cell) => {
      const parsedSpan = Number.parseInt(cell.getAttribute("colspan") || "1", 10);
      const span = Number.isInteger(parsedSpan) && parsedSpan > 0 ? parsedSpan : 1;
      let shouldShow = false;
      for (let idx = columnCursor; idx < columnCursor + span; idx += 1) {
        if (visibleColumns.has(idx)) {
          shouldShow = true;
          break;
        }
      }
      cell.hidden = !shouldShow;
      columnCursor += span;
    });
  });
};

const updateEmptyStateColspans = (table, visibleColumnCount) => {
  const colspanValue = String(Math.max(1, visibleColumnCount));
  table.querySelectorAll("tbody tr").forEach((row) => {
    if (!(row instanceof HTMLTableRowElement)) return;
    const cells = Array.from(row.children).filter(
      (child) => child instanceof HTMLTableCellElement,
    );
    if (cells.length !== 1) return;
    if (!cells[0].hasAttribute("colspan")) return;
    cells[0].setAttribute("colspan", colspanValue);
  });
};

const applyColumns = (state) => {
  applyColumnVisibility(state.table, state.visibleColumns);
  updateEmptyStateColspans(state.table, state.visibleColumns.size);
};

const persistVisibleColumns = (state) => {
  writeStoredVisibleColumns(state.tableID, state.columnCount, state.visibleColumns);
};

const syncOptionCheckboxes = (state) => {
  state.optionsHost.querySelectorAll("button[data-columns-index]").forEach((item) => {
    if (!(item instanceof HTMLButtonElement)) return;
    const index = Number.parseInt(item.dataset.columnsIndex || "", 10);
    if (!Number.isInteger(index)) return;
    const checked = state.visibleColumns.has(index);
    const fixed = state.fixedColumns.has(index);
    item.setAttribute("aria-checked", checked ? "true" : "false");
    item.setAttribute("aria-disabled", fixed ? "true" : "false");
    const indicator = item.querySelector("[data-columns-indicator]");
    if (indicator instanceof HTMLElement) {
      indicator.textContent = checked ? "✓" : "";
    }
  });
};

const buildOptions = (state) => {
  state.optionsHost.replaceChildren();
  state.headers.forEach((header, index) => {
    const columnIndex = index + 1;
    const item = document.createElement("button");
    item.type = "button";
    item.role = "menuitemcheckbox";
    item.className = "cursor-pointer";
    item.dataset.columnsIndex = String(columnIndex);
    item.addEventListener("click", (event) => {
      event.preventDefault();
      event.stopPropagation();
      if (state.fixedColumns.has(columnIndex)) {
        return;
      }
      const nextVisible = new Set(state.visibleColumns);
      if (state.visibleColumns.has(columnIndex)) {
        nextVisible.delete(columnIndex);
      } else {
        nextVisible.add(columnIndex);
      }
      state.visibleColumns = ensureValidVisible(nextVisible, state.columnCount, state.fixedColumns);
      applyColumns(state);
      persistVisibleColumns(state);
      syncOptionCheckboxes(state);
    });
    item.addEventListener("keydown", (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      event.stopPropagation();
      item.click();
    });

    const indicator = document.createElement("span");
    indicator.dataset.columnsIndicator = "true";
    indicator.className = "inline-flex w-4 justify-center text-muted-foreground";
    indicator.setAttribute("aria-hidden", "true");

    const label = document.createElement("span");
    label.className = "truncate";
    label.textContent = labelForHeader(header, index);

    item.append(indicator, label);
    state.optionsHost.append(item);
  });
};

const initColumnsForTable = (table) => {
  if (!(table instanceof HTMLTableElement)) return;
  if (table.dataset.columnsBound === "true") {
    const existingState = TABLE_STATES.get(table);
    if (existingState) {
      applyColumns(existingState);
      syncOptionCheckboxes(existingState);
      return;
    }
    delete table.dataset.columnsBound;
  }

  const tableID = collapseWhitespace(table.dataset.columnsId || "");
  if (!tableID) return;
  const headers = Array.from(table.querySelectorAll("thead tr:first-child th")).filter(
    (header) => header instanceof HTMLTableCellElement,
  );
  if (headers.length === 0) return;

  const controlRoot = table.previousElementSibling;
  if (!(controlRoot instanceof HTMLElement) || !controlRoot.matches("[data-columns-root]")) return;
  const selector = `[data-columns-control][data-columns-for="${tableID}"]`;
  const control = controlRoot.querySelector(selector);
  if (!(control instanceof HTMLElement)) return;

  const optionsHost = control.querySelector("[data-columns-options]");
  const resetButton = control.querySelector("[data-columns-reset]");
  if (!(optionsHost instanceof HTMLElement) || !(resetButton instanceof HTMLButtonElement)) return;

  const fixedColumns = resolveFixedColumns(table, headers);
  const defaultHidden = parseIndexSet(table.dataset.columnsDefaultHidden || "", headers.length);
  const defaults = defaultVisibleColumns(headers.length, defaultHidden, fixedColumns);
  const stored = readStoredVisibleColumns(tableID, headers.length);
  const visibleColumns = ensureValidVisible(stored || defaults, headers.length, fixedColumns);

  const state = {
    table,
    tableID,
    columnCount: headers.length,
    headers,
    fixedColumns,
    visibleColumns,
    optionsHost,
  };

  buildOptions(state);
  applyColumns(state);
  syncOptionCheckboxes(state);

  resetButton.addEventListener("click", () => {
    clearStoredVisibleColumns(tableID);
    state.visibleColumns = new Set(defaults);
    applyColumns(state);
    syncOptionCheckboxes(state);
  });

  const container = control.closest("[data-columns-root]");
  if (container instanceof HTMLElement) {
    container.hidden = false;
  }

  TABLE_STATES.set(table, state);
  table.dataset.columnsBound = "true";
};

export const wireColumnControls = (root = document) => {
  collectTables(root).forEach(initColumnsForTable);
};
