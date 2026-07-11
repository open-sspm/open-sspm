import { register } from "./registry.js";

// AskBar keeps ordinary text search native and progressively enhances only the
// structured filters. The URL and the GET form remain the canonical state.

const DEFAULTS = {
  fieldParam: {},
  keyLabel: {},
  fieldLabel: {},
  keywordTokens: {},
  fieldAliases: {},
  singletonFields: [],
  freeTextFields: [],
  staticHidden: [],
  suggestEndpoint: "",
};

const readConfig = (el) => {
  let raw = {};
  try {
    raw = JSON.parse(el.dataset.osspmAskbarConfig || "{}");
  } catch (_) {
    raw = {};
  }
  return Object.fromEntries(
    Object.entries(DEFAULTS).map(([key, fallback]) => [
      key,
      raw[key] === undefined ? fallback : raw[key],
    ]),
  );
};

const readInitialChips = (root) =>
  Array.from(root.querySelectorAll("[data-osspm-askbar-chip]"), (node) => ({
    field: node.dataset.chipField || "",
    value: node.dataset.chipValue || "",
    label: node.dataset.chipLabel || "",
    tone: node.dataset.chipTone || "",
  })).filter((chip) => chip.field && chip.field !== "search");

const chipClass = (tone) => {
  if (tone === "warn") return "osspm-askbar-chip osspm-askbar-chip-warn";
  if (tone === "danger") return "osspm-askbar-chip osspm-askbar-chip-danger";
  if (tone === "ok") return "osspm-askbar-chip osspm-askbar-chip-ok";
  return "osspm-askbar-chip";
};

export const initAskbar = (el) => {
  if (!(el instanceof HTMLElement)) return () => {};

  const cfg = readConfig(el);
  const doc = el.ownerDocument;
  const win = doc.defaultView || window;
  const form = el.closest("form");
  const chipHost = el.querySelector("[data-osspm-askbar-chips]");
  const searchInput = el.querySelector("[data-osspm-askbar-input]");
  const addFilter = el.querySelector("[data-osspm-askbar-add-filter]");
  const panel = el.querySelector("[data-osspm-askbar-filter-panel]");
  const filterInput = el.querySelector("[data-osspm-askbar-filter-input]");
  const suggestions = el.querySelector("[data-osspm-askbar-suggest]");
  const bank = el.querySelector("[data-osspm-askbar-bank]");

  if (!(chipHost instanceof HTMLElement)) return () => {};
  if (!(searchInput instanceof HTMLInputElement)) return () => {};
  if (!(bank instanceof HTMLElement)) return () => {};

  const singletonFields = new Set(cfg.singletonFields);
  const staticHidden = Array.isArray(cfg.staticHidden) ? cfg.staticHidden : [];
  const fieldByParam = Object.fromEntries(
    Object.entries(cfg.fieldParam).map(([field, param]) => [param, field]),
  );
  const staticKeys = new Set(
    staticHidden.map(({ name, value }) => `${name}\u0000${value}`),
  );
  const historyExtraNames = new Set([
    "expiry_state",
    "sort_by",
    "sort_dir",
    "source_name",
  ]);
  const initialBank = Array.from(bank.querySelectorAll("input[name]"), (node) => ({
    name: node.name,
    value: node.value,
  }));
  const state = { chips: readInitialChips(chipHost), extraHidden: [] };
  let requestTimer = 0;
  let requestController = null;
  let requestSequence = 0;
  const hasHtmxBehavior =
    form instanceof HTMLFormElement &&
    ["hx-get", "hx-post", "hx-put", "hx-patch", "hx-delete"].some((attribute) =>
      form.hasAttribute(attribute),
    );

  const namedFormControls = () => {
    if (!(form instanceof HTMLFormElement)) return [];
    return Array.from(
      form.querySelectorAll("input[name], select[name], textarea[name]"),
    ).filter((control) => !bank.contains(control) && control !== filterInput && !control.disabled);
  };

  const formControlNames = () =>
    new Set(namedFormControls().map((control) => control.name).filter(Boolean));

  const chipParamNames = () =>
    new Set(
      state.chips
        .map((chip) => cfg.fieldParam[chip.field])
        .filter(Boolean),
    );

  state.extraHidden = initialBank.filter(({ name, value }) => {
    if (!name || name === "q" || name === "page") return false;
    if (formControlNames().has(name)) return false;
    if (staticKeys.has(`${name}\u0000${value}`)) return false;
    return !chipParamNames().has(name);
  });

  const tokenFor = (field, value) =>
    Object.values(cfg.keywordTokens).find(
      (token) => token.field === field && String(token.value) === String(value),
    );

  const labelFor = (field, value) => {
    const existing = state.chips.find(
      (chip) => chip.field === field && chip.value === value,
    );
    const token = tokenFor(field, value);
    return existing?.label || token?.label || String(value).replaceAll("_", " ");
  };

  const appendHidden = (name, value) => {
    if (!name || name === "q" || value === "") return;
    const node = doc.createElement("input");
    node.type = "hidden";
    node.name = name;
    node.value = value;
    bank.append(node);
  };

  const syncBank = () => {
    bank.replaceChildren();
    const seen = new Set();
    const controlledNames = formControlNames();
    const add = ({ name, value }) => {
      if (controlledNames.has(name)) return;
      const key = `${name}\u0000${value}`;
      if (seen.has(key)) return;
      seen.add(key);
      appendHidden(name, String(value ?? ""));
    };
    staticHidden.forEach(add);
    state.chips.forEach((chip) =>
      add({ name: cfg.fieldParam[chip.field], value: chip.value }),
    );
    state.extraHidden.forEach((hidden) => {
      const hasField = (field) => state.chips.some((chip) => chip.field === field);
      if (hidden.name === "expiry_state" && hidden.value === "active" && !hasField("expires_in_days")) return;
      if (hidden.name === "source_name" && cfg.fieldParam.source_kind && !hasField("source_kind")) return;
      add(hidden);
    });
  };

  const renderChips = () => {
    chipHost.replaceChildren();
    state.chips.forEach((chip, index) => {
      const wrapper = doc.createElement("span");
      wrapper.className = chipClass(chip.tone);
      wrapper.dataset.osspmAskbarChip = "";
      wrapper.dataset.chipField = chip.field;
      wrapper.dataset.chipValue = chip.value;
      wrapper.dataset.chipLabel = chip.label;
      wrapper.dataset.chipTone = chip.tone || "";
      wrapper.setAttribute("role", "group");
      wrapper.setAttribute("aria-label", `Filter ${chip.label}`);

      const key = cfg.keyLabel[chip.field];
      if (key) {
        const keyNode = doc.createElement("span");
        keyNode.className = "osspm-askbar-chip-key";
        keyNode.textContent = key;
        wrapper.append(keyNode);
        const separator = doc.createElement("span");
        separator.className = "osspm-askbar-chip-sep";
        separator.textContent = ":";
        wrapper.append(separator);
      }

      const label = doc.createElement("span");
      label.className = "osspm-askbar-chip-label";
      label.textContent = chip.label;
      wrapper.append(label);

      const remove = doc.createElement("button");
      remove.type = "button";
      remove.className = "osspm-askbar-chip-remove";
      remove.dataset.osspmAskbarChipRemove = "";
      remove.dataset.chipIndex = String(index);
      remove.setAttribute("aria-label", `Remove ${chip.label}`);
      remove.textContent = "×";
      wrapper.append(remove);
      chipHost.append(wrapper);
    });
    syncBank();
  };

  const submit = () => {
    bank.dispatchEvent(new Event("change", { bubbles: true }));
    if (form instanceof HTMLFormElement && !hasHtmxBehavior) form.requestSubmit();
  };

  const closePanel = ({ restoreFocus = false } = {}) => {
    if (!(panel instanceof HTMLElement)) return;
    panel.hidden = true;
    addFilter?.setAttribute("aria-expanded", "false");
    if (requestController) requestController.abort();
    if (restoreFocus && addFilter instanceof HTMLElement) addFilter.focus();
  };

  const localSuggestions = (query) => {
    if (!(suggestions instanceof HTMLElement)) return;
    const normalized = query.trim().toLowerCase();
    const unique = new Map();
    Object.values(cfg.keywordTokens).forEach((token) => {
      const haystack = `${token.label} ${cfg.fieldLabel[token.field] || ""}`.toLowerCase();
      if (normalized && !haystack.includes(normalized)) return;
      unique.set(`${token.field}\u0000${token.value}`, token);
    });
    suggestions.replaceChildren();
    unique.forEach((token) => {
      const button = doc.createElement("button");
      button.type = "button";
      button.className = "osspm-askbar-suggest-item";
      button.dataset.field = token.field;
      button.dataset.value = token.value;
      button.dataset.label = token.label;
      button.dataset.tone = token.tone || "";
      button.textContent = `${cfg.fieldLabel[token.field] || token.field} ${token.label}`;
      suggestions.append(button);
    });
  };

  const loadSuggestions = (query = "") => {
    if (!(suggestions instanceof HTMLElement)) return;
    win.clearTimeout(requestTimer);
    if (!cfg.suggestEndpoint) {
      localSuggestions(query);
      return;
    }
    requestTimer = win.setTimeout(async () => {
      if (requestController) requestController.abort();
      requestController = new AbortController();
      const sequence = ++requestSequence;
      const url = new URL(cfg.suggestEndpoint, win.location.href);
      url.searchParams.set("q", query);
      url.searchParams.set("force", "1");
      url.searchParams.set("filters_only", "1");
      suggestions.setAttribute("aria-busy", "true");
      try {
        const response = await win.fetch(url, {
          headers: { "HX-Request": "true" },
          signal: requestController.signal,
        });
        if (sequence !== requestSequence) return;
        suggestions.innerHTML = response.ok ? await response.text() : "";
      } catch (error) {
        if (error.name !== "AbortError") localSuggestions(query);
      } finally {
        if (sequence === requestSequence) suggestions.removeAttribute("aria-busy");
      }
    }, 80);
  };

  const openPanel = () => {
    if (!(panel instanceof HTMLElement) || !(filterInput instanceof HTMLInputElement)) return;
    panel.hidden = false;
    addFilter?.setAttribute("aria-expanded", "true");
    filterInput.value = "";
    filterInput.focus();
    loadSuggestions();
  };

  const addChip = ({ field, value, label, tone = "" }) => {
    if (!field || field === "search" || value === undefined) return;
    const chip = {
      field: String(field),
      value: String(value),
      label: label || labelFor(String(field), String(value)),
      tone,
    };
    if (singletonFields.has(chip.field)) {
      state.chips = state.chips.filter((current) => current.field !== chip.field);
    }
    if (!state.chips.some((current) => current.field === chip.field && current.value === chip.value)) {
      state.chips.push(chip);
    }
    renderChips();
    closePanel({ restoreFocus: true });
    submit();
  };

  const onChipClick = (event) => {
    const button = event.target.closest("[data-osspm-askbar-chip-remove]");
    if (!button) return;
    const index = Number.parseInt(button.dataset.chipIndex || "-1", 10);
    if (index < 0 || index >= state.chips.length) return;
    state.chips.splice(index, 1);
    renderChips();
    submit();
    searchInput.focus();
  };

  const onSuggestionClick = (event) => {
    const item = event.target.closest("[data-field][data-value]");
    if (!(item instanceof HTMLElement)) return;
    addChip({
      field: item.dataset.field,
      value: item.dataset.value,
      label: item.dataset.label,
      tone: item.dataset.tone,
    });
  };

  const onFilterKeydown = (event) => {
    if (event.key === "Escape") {
      event.preventDefault();
      closePanel({ restoreFocus: true });
      return;
    }
    if (event.key === "Enter") {
      const first = suggestions?.querySelector("[data-field][data-value]");
      if (first instanceof HTMLElement) {
        event.preventDefault();
        first.click();
      }
    }
  };

  const chipsFromURL = (params) => {
    const chips = [];
    for (const [param, field] of Object.entries(fieldByParam)) {
      if (!field || field === "search") continue;
      params.getAll(param).forEach((value) => {
        if (!value) return;
        if (
          field === "expiry_state" &&
          value === "active" &&
          params.has(cfg.fieldParam.expires_in_days || "")
        ) return;
        const token = tokenFor(field, value);
        chips.push({
          field,
          value,
          label: token?.label || labelFor(field, value),
          tone: token?.tone || "",
        });
      });
    }
    return chips;
  };

  const syncControlsFromURL = (params) => {
    namedFormControls().forEach((control) => {
      if (control === searchInput) return;
      const values = params.getAll(control.name);
      if (control instanceof HTMLInputElement) {
        const type = control.type.toLowerCase();
        if (type === "checkbox" || type === "radio") {
          control.checked = values.length > 0 ? values.includes(control.value) : control.defaultChecked;
        } else {
          control.value = values.length > 0 ? values[0] : control.defaultValue;
        }
        return;
      }
      if (control instanceof HTMLSelectElement) {
        if (control.multiple) {
          Array.from(control.options).forEach((option) => {
            option.selected = values.length > 0 ? values.includes(option.value) : option.defaultSelected;
          });
        } else if (values.length > 0) {
          control.value = values[0];
        } else {
          const defaultOption = Array.from(control.options).find((option) => option.defaultSelected);
          control.value = defaultOption?.value || control.options[0]?.value || "";
        }
        return;
      }
      control.value = values.length > 0 ? values[0] : control.defaultValue;
    });
  };

  const onHistoryChange = () => {
    const params = new URL(win.location.href).searchParams;
    searchInput.value = params.get("q") || "";
    syncControlsFromURL(params);
    state.chips = chipsFromURL(params);
    const known = new Set(["q", "page", ...Object.keys(fieldByParam), ...formControlNames()]);
    state.extraHidden = Array.from(params.entries(), ([name, value]) => ({ name, value }))
      .filter(({ name, value }) =>
        historyExtraNames.has(name) &&
        (!known.has(name) || (
          name === "expiry_state" &&
          value === "active" &&
          params.has(cfg.fieldParam.expires_in_days || "")
        )) &&
        !staticKeys.has(`${name}\u0000${value}`),
      );
    renderChips();
    closePanel();
  };

  const onDocumentClick = (event) => {
    if (!el.contains(event.target)) closePanel();
  };

  renderChips();
  chipHost.addEventListener("click", onChipClick);
  addFilter?.addEventListener("click", openPanel);
  filterInput?.addEventListener("input", (event) => loadSuggestions(event.target.value));
  filterInput?.addEventListener("keydown", onFilterKeydown);
  suggestions?.addEventListener("click", onSuggestionClick);
  searchInput.addEventListener("search", () => {
    if (searchInput.value === "") submit();
  });
  doc.addEventListener("click", onDocumentClick);
  doc.addEventListener("htmx:pushedIntoHistory", onHistoryChange);
  doc.addEventListener("htmx:replacedInHistory", onHistoryChange);
  win.addEventListener("popstate", onHistoryChange);

  return () => {
    win.clearTimeout(requestTimer);
    if (requestController) requestController.abort();
    chipHost.removeEventListener("click", onChipClick);
    addFilter?.removeEventListener("click", openPanel);
    filterInput?.removeEventListener("keydown", onFilterKeydown);
    suggestions?.removeEventListener("click", onSuggestionClick);
    doc.removeEventListener("click", onDocumentClick);
    doc.removeEventListener("htmx:pushedIntoHistory", onHistoryChange);
    doc.removeEventListener("htmx:replacedInHistory", onHistoryChange);
    win.removeEventListener("popstate", onHistoryChange);
  };
};

register(
  "osspm_askbar",
  "[data-osspm-askbar]:not([data-osspm_askbar-initialized])",
  initAskbar,
);
