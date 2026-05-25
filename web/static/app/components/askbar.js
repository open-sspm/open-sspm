import { register } from "./registry.js";

// askbar.js
//
// A chip-in-input filter bar. Chips for active filter values live inline with
// the typed text inside one visual control. The bar is data-driven via a JSON
// config on the root element so every list page can reuse the same JS.
//
// Required DOM shape (rendered by the AskBar templ partial):
//
//   <div data-osspm-askbar data-osspm-askbar-config='{...}'>
//     <label data-osspm-askbar-bar>
//       <span data-osspm-askbar-chips>...server-rendered chips + the input</span>
//       <input data-osspm-askbar-input ...>
//     </label>
//     <div data-osspm-askbar-suggest hidden></div>
//     <div data-osspm-askbar-bank data-table-query-trigger></div>
//   </div>
//
// Config JSON shape:
//
//   {
//     "fieldParam":      { "<field>": "<url_param_name>" },
//     "keyLabel":        { "<field>": "<chip key prefix>" },
//     "fieldLabel":      { "<field>": "<suggest list heading>" },
//     "keywordTokens":   { "<word>": { field, value, label, tone? } },
//     "fieldAliases":    { "<alias>": "<field>" },
//     "stopwords":       [ ... ],
//     "singletonFields": [ "<field>", ... ],
//     "freeTextFields":  [ "<field>", ... ],
//     "staticHidden":    [ { name, value }, ... ],
//     "suggestEndpoint": "/askbar/suggestions?scope=...",
//   }
//
// Chip values are always strings; the URL canonicalization happens server-side.

const DEFAULTS = {
  fieldParam: { search: "q" },
  keyLabel: { search: "" },
  fieldLabel: { search: "Search" },
  keywordTokens: {},
  fieldAliases: {},
  stopwords: [],
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
  const out = {};
  for (const key of Object.keys(DEFAULTS)) {
    out[key] = raw[key] !== undefined ? raw[key] : DEFAULTS[key];
  }
  return out;
};

const escapeHtml = (s) =>
  String(s ?? "").replace(
    /[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c],
  );

const readInitialChips = (root) =>
  Array.from(root.querySelectorAll("[data-osspm-askbar-chip]")).map((node) => ({
    field: node.dataset.chipField || "",
    value: node.dataset.chipValue || "",
    label: node.dataset.chipLabel || "",
    tone: node.dataset.chipTone || "",
  }));

export const initAskbar = (el) => {
  if (!(el instanceof HTMLElement)) return () => {};

  const cfg = readConfig(el);
  const FIELD_PARAM = cfg.fieldParam;
  const KEY_LABEL = cfg.keyLabel;
  const FIELD_LABEL = cfg.fieldLabel;
  const KEYWORD_TOKENS = cfg.keywordTokens;
  const FIELD_ALIASES = cfg.fieldAliases;
  const STOPWORDS = new Set(cfg.stopwords);
  const SINGLETON_FIELDS = new Set(cfg.singletonFields);
  const FREE_TEXT_FIELDS = new Set(cfg.freeTextFields);
  const STATIC_HIDDEN = Array.isArray(cfg.staticHidden) ? cfg.staticHidden : [];
  const SUGGEST_ENDPOINT = String(cfg.suggestEndpoint || "");

  const doc = el.ownerDocument;
  const bar = el.querySelector("[data-osspm-askbar-bar]");
  const chipHost = el.querySelector("[data-osspm-askbar-chips]");
  const input = el.querySelector("[data-osspm-askbar-input]");
  const addFilterButton = el.querySelector("[data-osspm-askbar-add-filter]");
  const suggest = el.querySelector("[data-osspm-askbar-suggest]");
  const bank = el.querySelector("[data-osspm-askbar-bank]");
  const form = el.closest("form");
  const win = doc.defaultView || window;

  if (!(bar instanceof HTMLElement)) return () => {};
  if (!(chipHost instanceof HTMLElement)) return () => {};
  if (!(input instanceof HTMLInputElement)) return () => {};
  if (!(bank instanceof HTMLElement)) return () => {};

  const hasVocabulary =
    Object.keys(KEYWORD_TOKENS).length > 0 ||
    Object.keys(FIELD_ALIASES).length > 0;

  const hasHtmxBehavior =
    form instanceof HTMLFormElement &&
    ["hx-get", "hx-post", "hx-put", "hx-patch", "hx-delete"].some((attr) =>
      form.hasAttribute(attr),
    );

  const fieldByParam = Object.fromEntries(
    Object.entries(FIELD_PARAM).map(([field, param]) => [param, field]),
  );
  const staticHiddenKeys = new Set(
    STATIC_HIDDEN.map((hidden) => `${hidden?.name ?? ""}\u0000${hidden?.value ?? ""}`),
  );
  const urlExtraParamNames = new Set([
    "expiry_state",
    "sort_by",
    "sort_dir",
    "source_name",
  ]);

  const hiddenKey = (hidden) => `${hidden.name}\u0000${hidden.value}`;

  const paramsForChips = (chips) => {
    const params = {};
    for (const c of chips) {
      if (c.field === "search") {
        params.q = params.q ? `${params.q} ${c.value}` : c.value;
        continue;
      }
      const param = FIELD_PARAM[c.field];
      if (!param) continue;
      params[param] = c.value;
    }
    return params;
  };

  const hiddenEntriesFromBank = () =>
    Array.from(bank.querySelectorAll("input[name]"), (input) => ({
      name: input.name,
      value: input.value,
    }));

  const namedFormControls = () => {
    if (!(form instanceof HTMLFormElement)) return [];
    return Array.from(
      form.querySelectorAll("input[name], select[name], textarea[name]"),
    ).filter((control) => {
      if (bank.contains(control)) return false;
      if (control === input) return false;
      if (control.disabled) return false;
      if (control instanceof HTMLInputElement) {
        const type = control.type.toLowerCase();
        return !["button", "file", "image", "reset", "submit"].includes(type);
      }
      return true;
    });
  };

  const formControlParamNames = () =>
    new Set(namedFormControls().map((control) => control.name).filter(Boolean));

  const extraHiddenFromEntries = (entries, chips, { url = false } = {}) => {
    const chipParamNames = new Set(Object.keys(paramsForChips(chips)));
    const controlParamNames = formControlParamNames();
    return entries.filter((hidden) => {
      if (!hidden.name) return false;
      if (hidden.name === "page") return false;
      if (url && !urlExtraParamNames.has(hidden.name)) return false;
      if (controlParamNames.has(hidden.name)) return false;
      if (staticHiddenKeys.has(hiddenKey(hidden))) return false;
      return !chipParamNames.has(hidden.name);
    });
  };

  const state = {
    chips: readInitialChips(chipHost),
    insertionIndex: 0,
    extraHidden: [],
  };
  let serverSuggestTimer = 0;
  let serverSuggestController = null;
  let serverSuggestSeq = 0;
  state.insertionIndex = state.chips.length;
  state.extraHidden = extraHiddenFromEntries(hiddenEntriesFromBank(), state.chips);

  const toneFor = (kw) => kw.tone || "";

  const cloneToken = (tok) => ({ ...tok, tone: toneFor(tok) });

  const tokensForField = (field) => {
    const seen = new Set();
    return Object.values(KEYWORD_TOKENS).filter((tok) => {
      if (tok.field !== field) return false;
      const key = `${tok.field}:${tok.value}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    });
  };

  const tokenFromFieldValue = (field, rawValue) => {
    const raw = String(rawValue ?? "").trim();
    let value = raw
      .toLowerCase()
      .replace(/^[<≤]/, "")
      .replace(/^>/, "")
      .trim();
    // Strip the trailing "d" only on numeric day expressions (e.g. "30d", "7d").
    // Stripping unconditionally also mangles values like "not-connected" → "not-connecte".
    if (/^\d+d$/.test(value)) {
      value = value.slice(0, -1);
    }
    if (!value) return null;

    // Canonical keyword values use snake_case, but users type the hyphenated keyword
    // form (e.g. action-required ≡ action_required). Normalize separators for matching.
    const norm = (s) => String(s).toLowerCase().replace(/[-\s]+/g, "_");
    const want = norm(value);

    const exact = tokensForField(field).find(
      (tok) => norm(tok.value) === want || norm(tok.label) === want,
    );
    if (exact) return cloneToken(exact);
    const partial = tokensForField(field).find(
      (tok) =>
        norm(tok.value).startsWith(want) || norm(tok.label).startsWith(want),
    );
    if (partial) return cloneToken(partial);
    if (FREE_TEXT_FIELDS.has(field)) {
      return {
        field,
        value: raw,
        label: raw,
      };
    }
    return null;
  };

  const mergeSearchToken = (out, text) => {
    const value = String(text ?? "").trim();
    if (!value) return;
    const last = out[out.length - 1];
    if (last && last.field === "search") {
      last.value += " " + value;
      last.label = `"${last.value}"`;
    } else {
      out.push({ field: "search", value, label: `"${value}"` });
    }
  };

  const parseToTokens = (text) => {
    let s = text
      .toLowerCase()
      .replace(/needs?\s+action/g, "needs-action")
      .replace(/never\s+seen/g, "never-seen");

    const out = [];
    for (const word of s.split(/\s+/).filter(Boolean)) {
      if (STOPWORDS.has(word)) continue;
      if (word.includes(":")) {
        const colon = word.indexOf(":");
        const k = word.slice(0, colon);
        const v = word.slice(colon + 1);
        const field = FIELD_ALIASES[k];
        if (field) {
          const tok = tokenFromFieldValue(field, v);
          if (tok) out.push(tok);
          // Field is recognized but value didn't match the vocabulary — drop
          // the input silently. Falling back to text search would mangle a
          // typo'd filter (e.g. `src:githubb`) into `q="src:githubb"`.
          continue;
        }
        mergeSearchToken(out, word);
        continue;
      }
      if (KEYWORD_TOKENS[word]) {
        out.push(cloneToken(KEYWORD_TOKENS[word]));
        continue;
      }
      mergeSearchToken(out, word);
    }
    return out;
  };

  const chipNodes = () =>
    Array.from(chipHost.querySelectorAll("[data-osspm-askbar-chip]"));

  const clampIndex = (i) =>
    Math.max(0, Math.min(state.chips.length, Number.isFinite(i) ? i : state.chips.length));

  const focusInputEnd = () => {
    input.focus();
    const pos = input.value.length;
    try {
      input.setSelectionRange(pos, pos);
    } catch (_) {
      /* noop */
    }
  };

  const updateInputCursorWidth = () => {
    const inline = state.insertionIndex < state.chips.length;
    input.dataset.inlineCursor = inline ? "true" : "false";
    if (inline) {
      input.style.setProperty(
        "--cursor-width",
        `${Math.max(1.25, input.value.length + 1)}ch`,
      );
    } else {
      input.style.removeProperty("--cursor-width");
    }
  };

  const placeInputAt = (index, shouldFocus = true) => {
    state.insertionIndex = clampIndex(index);
    const chips = chipNodes();
    chipHost.insertBefore(input, chips[state.insertionIndex] || null);
    updateInputCursorWidth();
    if (shouldFocus) focusInputEnd();
  };

  const moveInsertionCursor = (dir) => {
    if (state.chips.length === 0) return false;
    placeInputAt(state.insertionIndex + dir);
    return true;
  };

  const handleInsertionArrow = (e) => {
    if (
      e.metaKey ||
      e.ctrlKey ||
      (e.key !== "ArrowLeft" && e.key !== "ArrowRight")
    ) {
      return false;
    }
    e.preventDefault();
    hideSuggest();
    return moveInsertionCursor(e.key === "ArrowLeft" ? -1 : 1);
  };

  const hideSuggest = () => {
    if (suggest instanceof HTMLElement) suggest.hidden = true;
  };

  const serverSuggestEnabled = () =>
    SUGGEST_ENDPOINT !== "" &&
    suggest instanceof HTMLElement &&
    typeof win.fetch === "function" &&
    typeof win.AbortController === "function";

  const showServerSuggest = (displayText, force) => {
    if (!(suggest instanceof HTMLElement)) return;
    win.clearTimeout(serverSuggestTimer);
    if (serverSuggestController) {
      serverSuggestController.abort();
      serverSuggestController = null;
    }
    serverSuggestTimer = win.setTimeout(() => {
      const endpoint = new URL(SUGGEST_ENDPOINT, win.location.href);
      endpoint.searchParams.set("q", displayText);
      if (force) endpoint.searchParams.set("force", "1");
      const seq = ++serverSuggestSeq;
      const controller = new win.AbortController();
      serverSuggestController = controller;
      suggest.hidden = false;
      win
        .fetch(`${endpoint.pathname}${endpoint.search}`, {
          headers: { "HX-Request": "true", "Accept": "text/html" },
          signal: controller.signal,
        })
        .then((response) => {
          if (!response.ok) throw new Error(`suggestions failed: ${response.status}`);
          return response.text();
        })
        .then((html) => {
          if (seq !== serverSuggestSeq) return;
          suggest.innerHTML = html;
          onSuggestAfterSwap({ target: suggest });
        })
        .catch((error) => {
          if (error?.name === "AbortError") return;
          if (seq !== serverSuggestSeq) return;
          suggest.innerHTML = `<div class="osspm-askbar-suggest-label">Check syntax</div><div class="osspm-askbar-suggest-error" role="status">Suggestions unavailable.</div>`;
          suggest.hidden = false;
        })
        .finally(() => {
          if (serverSuggestController === controller) {
            serverSuggestController = null;
          }
        });
    }, 80);
  };

  const buildChipHtml = (c, idx) => {
    const tone = c.tone ? ` osspm-askbar-chip-${c.tone}` : "";
    const key = KEY_LABEL[c.field] ?? "";
    const keyHtml = key
      ? `<span class="osspm-askbar-chip-key">${escapeHtml(
          key,
        )}</span><span class="osspm-askbar-chip-sep">:</span>`
      : "";
    return `<span class="osspm-askbar-chip${tone}" data-osspm-askbar-chip data-idx="${idx}" data-chip-field="${escapeHtml(
      c.field,
    )}" data-chip-value="${escapeHtml(c.value)}" data-chip-label="${escapeHtml(
      c.label,
    )}" data-chip-tone="${escapeHtml(
      c.tone || "",
    )}" tabindex="-1" role="group" aria-label="Filter ${escapeHtml(c.label)}">${keyHtml}<span class="osspm-askbar-chip-label">${escapeHtml(
      c.label,
    )}</span><button type="button" class="osspm-askbar-chip-remove" data-osspm-askbar-chip-remove data-idx="${idx}" aria-label="Remove ${escapeHtml(c.label)}">&times;</button></span>`;
  };

  const renderChips = () => {
    chipHost.innerHTML = state.chips.map(buildChipHtml).join("");
    chipHost.append(input);
    state.insertionIndex = clampIndex(state.insertionIndex);
    placeInputAt(state.insertionIndex, false);

    chipHost.querySelectorAll("[data-osspm-askbar-chip]").forEach((chip) => {
      chip.addEventListener("click", (e) => {
        if (e.target.closest("[data-osspm-askbar-chip-remove]")) return;
        placeInputAt(Number(chip.dataset.idx) + 1);
      });
      chip.addEventListener("keydown", (e) => {
        if (handleInsertionArrow(e)) return;
        if (e.key === "Escape") {
          focusInputEnd();
          e.preventDefault();
        }
      });
    });
    chipHost
      .querySelectorAll("[data-osspm-askbar-chip-remove]")
      .forEach((btn) => {
        btn.addEventListener("click", (e) => {
          e.stopPropagation();
          const idx = Number(btn.dataset.idx);
          state.chips.splice(idx, 1);
          if (state.insertionIndex > idx) state.insertionIndex--;
          renderChips();
          syncAndSubmit();
          placeInputAt(Math.min(idx, state.chips.length));
        });
      });
  };

  const hasChipField = (field) => state.chips.some((chip) => chip.field === field);

  const shouldKeepExtraHidden = (hidden) => {
    if (!hidden?.name) return false;
    if (hidden.name === "expiry_state" && hidden.value === "active") {
      return hasChipField("expires_in_days");
    }
    if (hidden.name === "source_name" && FIELD_PARAM.source_kind) {
      return hasChipField("source_kind");
    }
    const field = fieldByParam[hidden.name];
    if (field) return hasChipField(field);
    return true;
  };

  const writeHiddenBankFromState = () => {
    bank.innerHTML = "";
    const params = {};
    const controlParamNames = formControlParamNames();
    for (const hidden of STATIC_HIDDEN) {
      const name = String(hidden?.name ?? "").trim();
      if (!name) continue;
      if (controlParamNames.has(name)) continue;
      params[name] = String(hidden?.value ?? "");
    }
    for (const hidden of state.extraHidden) {
      if (controlParamNames.has(hidden.name)) continue;
      if (!shouldKeepExtraHidden(hidden)) continue;
      params[hidden.name] = hidden.value;
    }
    for (const [name, value] of Object.entries(paramsForChips(state.chips))) {
      params[name] = value;
    }
    for (const [name, value] of Object.entries(params)) {
      const node = doc.createElement("input");
      node.type = "hidden";
      node.name = name;
      node.value = value;
      node.defaultValue = value;
      bank.append(node);
    }
  };

  const syncAndSubmit = () => {
    writeHiddenBankFromState();
    bank.dispatchEvent(new Event("change", { bubbles: true }));
    if (form instanceof HTMLFormElement && !hasHtmxBehavior) {
      if (typeof form.requestSubmit === "function") {
        form.requestSubmit();
      } else {
        form.submit();
      }
    }
  };

  const tokenForFieldValue = (field, value) =>
    Object.values(KEYWORD_TOKENS).find(
      (tok) => tok.field === field && String(tok.value) === String(value),
    );

  const chipFromFieldValue = (field, value) => {
    if (field === "search") {
      return { field: "search", value, label: `"${value}"` };
    }
    const tok = tokenForFieldValue(field, value);
    if (tok) return cloneToken(tok);
    return { field, value, label: value };
  };

  const chipsFromSearch = (search) => {
    const params = new URLSearchParams(search || "");
    const next = [];
    for (const [field, paramName] of Object.entries(FIELD_PARAM)) {
      const value = params.get(paramName);
      if (value === null || value === "") continue;
      // Mirrors CredentialsAskBar: expiry_state=active is implicit when the
      // expires_in_days chip carries the useful signal.
      if (
        field === "expiry_state" &&
        value === "active" &&
        params.get(FIELD_PARAM.expires_in_days || "") !== null &&
        params.get(FIELD_PARAM.expires_in_days || "") !== ""
      ) {
        continue;
      }
      next.push(chipFromFieldValue(field, value));
    }
    return next;
  };

  const extraHiddenFromSearch = (search, chips) => {
    const params = new URLSearchParams(search || "");
    return extraHiddenFromEntries(
      Array.from(params.entries(), ([name, value]) => ({ name, value })),
      chips,
      { url: true },
    );
  };

  const resetSelectControl = (select) => {
    const defaultIndex = Array.from(select.options).findIndex(
      (option) => option.defaultSelected,
    );
    select.selectedIndex = defaultIndex >= 0 ? defaultIndex : 0;
  };

  const syncFormControlsFromSearch = (search) => {
    const params = new URLSearchParams(search || "");
    const controlsByName = new Map();
    for (const control of namedFormControls()) {
      const list = controlsByName.get(control.name) || [];
      list.push(control);
      controlsByName.set(control.name, list);
    }

    for (const [name, controls] of controlsByName) {
      const values = params.getAll(name);
      const hasValue = values.length > 0;
      for (const control of controls) {
        if (control instanceof HTMLInputElement) {
          const type = control.type.toLowerCase();
          if (type === "checkbox" || type === "radio") {
            control.checked = hasValue
              ? values.includes(control.value)
              : control.defaultChecked;
          } else {
            control.value = hasValue ? values[0] : control.defaultValue;
          }
          continue;
        }
        if (control instanceof HTMLSelectElement) {
          if (control.multiple) {
            for (const option of control.options) {
              option.selected = hasValue
                ? values.includes(option.value)
                : option.defaultSelected;
            }
          } else if (hasValue) {
            control.value = values[0];
          } else {
            resetSelectControl(control);
          }
          continue;
        }
        if (control instanceof HTMLTextAreaElement) {
          control.value = hasValue ? values[0] : control.defaultValue;
        }
      }
    }
  };

  const chipKey = (chip) => `${chip.field}\u0000${chip.value}`;

  const chipsSetEqual = (a, b) => {
    if (a.length !== b.length) return false;
    const seen = new Set(a.map(chipKey));
    return b.every((chip) => seen.has(chipKey(chip)));
  };

  const syncChipsFromURL = () => {
    const search = win.location.search;
    const next = chipsFromSearch(search);
    const extraHidden = extraHiddenFromSearch(search, next);
    syncFormControlsFromSearch(search);
    if (!chipsSetEqual(state.chips, next)) {
      state.chips = next;
      state.insertionIndex = state.chips.length;
      input.value = "";
      renderChips();
      hideSuggest();
    }
    state.extraHidden = extraHidden;
    writeHiddenBankFromState();
  };

  const onHistoryURLChange = syncChipsFromURL;

  const addChip = (c) => {
    const chip = cloneToken(c);
    if (
      state.chips.some(
        (x) => x.field === chip.field && String(x.value) === String(chip.value),
      )
    ) {
      return;
    }
    let insertAt = clampIndex(state.insertionIndex);
    if (SINGLETON_FIELDS.has(chip.field)) {
      state.chips = state.chips.filter((x, idx) => {
        const keep = x.field !== chip.field;
        if (!keep && idx < insertAt) insertAt--;
        return keep;
      });
    }
    state.chips.splice(insertAt, 0, chip);
    state.insertionIndex = insertAt + 1;
    renderChips();
    syncAndSubmit();
    placeInputAt(state.insertionIndex);
  };

  const showSuggest = (text, force = false) => {
    if (!(suggest instanceof HTMLElement)) return;
    if (!hasVocabulary) {
      hideSuggest();
      return;
    }
    const displayText = text.trim();
    const t = displayText.toLowerCase();
    if (t === "" && !force) {
      hideSuggest();
      return;
    }
    if (serverSuggestEnabled()) {
      showServerSuggest(displayText, force);
      return;
    }

    const colonIdx = t.indexOf(":");
    const fieldPart = colonIdx >= 0 ? t.slice(0, colonIdx) : t;
    const valuePart = colonIdx >= 0 ? t.slice(colonIdx + 1) : "";
    const fieldFromPart = FIELD_ALIASES[fieldPart];

    const seen = new Set();
    const items = [];
    const push = (tok, kw, rank) => {
      const key = `${tok.field}:${tok.value}`;
      if (seen.has(key)) return;
      seen.add(key);
      items.push({ tok, kw, rank });
    };

    if (colonIdx >= 0 && fieldFromPart) {
      for (const [kw, tok] of Object.entries(KEYWORD_TOKENS)) {
        if (tok.field !== fieldFromPart) continue;
        const key = String(kw).toLowerCase();
        const lbl = String(tok.label).toLowerCase();
        const val = String(tok.value).toLowerCase();
        if (!valuePart) push(tok, kw, 0);
        else if (
          key.startsWith(valuePart) ||
          lbl.startsWith(valuePart) ||
          val.startsWith(valuePart)
        )
          push(tok, kw, 0);
        else if (lbl.includes(valuePart)) push(tok, kw, 1);
      }
    } else if (force && t === "") {
      for (const [kw, tok] of Object.entries(KEYWORD_TOKENS)) {
        push(tok, kw, 0);
      }
    } else {
      for (const [kw, tok] of Object.entries(KEYWORD_TOKENS)) {
        if (kw.startsWith(t)) push(tok, kw, 0);
      }
      for (const [alias, field] of Object.entries(FIELD_ALIASES)) {
        if (alias.startsWith(t) && alias.length > 1) {
          for (const [kw, tok] of Object.entries(KEYWORD_TOKENS)) {
            if (tok.field === field) push(tok, kw, 2);
          }
        }
      }
      if (t.length >= 2) {
        for (const [kw, tok] of Object.entries(KEYWORD_TOKENS)) {
          if (String(tok.label).toLowerCase().startsWith(t)) push(tok, kw, 3);
        }
      }
    }

    items.sort((a, b) => a.rank - b.rank);
    const limited = items.slice(0, 8);

    let html = "";
    if (limited.length > 0) {
      html += `<div class="osspm-askbar-suggest-label">Suggestions</div>`;
      html += limited
        .map(
          (it, i) => `
        <div class="osspm-askbar-suggest-item ${i === 0 ? "is-hl" : ""}" data-kw="${escapeHtml(
          it.kw,
        )}" data-field="${escapeHtml(it.tok.field)}" data-value="${escapeHtml(
          it.tok.value,
        )}" data-label="${escapeHtml(it.tok.label)}" data-tone="${escapeHtml(
          it.tok.tone || "",
        )}">
          <span class="osspm-askbar-suggest-key">${escapeHtml(
            FIELD_LABEL[it.tok.field] || it.tok.field,
          )}</span>
          <span class="osspm-askbar-suggest-val">${escapeHtml(it.tok.label)}</span>
        </div>`,
        )
        .join("");
      if (displayText) {
        html += `<div class="osspm-askbar-suggest-label">Or</div>
          <div class="osspm-askbar-suggest-item" data-action="freetext" data-value="${escapeHtml(
            displayText,
          )}">
            <span class="osspm-askbar-suggest-key">Search</span>
            <span class="osspm-askbar-suggest-val">"${escapeHtml(displayText)}"</span>
          </div>`;
      }
    } else {
      html = `<div class="osspm-askbar-suggest-label">Free text</div>
        <div class="osspm-askbar-suggest-item is-hl" data-action="freetext" data-value="${escapeHtml(
          displayText,
        )}">
          <span class="osspm-askbar-suggest-val">Add "${escapeHtml(
            displayText,
          )}" as text search</span>
        </div>`;
    }
    suggest.innerHTML = html;
    suggest.hidden = false;
  };

  const onSuggestClick = (event) => {
    if (!(suggest instanceof HTMLElement)) return;
    if (!(event.target instanceof Element)) return;
    const node = event.target.closest(".osspm-askbar-suggest-item");
    if (!(node instanceof HTMLElement) || !suggest.contains(node)) return;

    if (node.dataset.action === "freetext") {
      const text = String(node.dataset.value || input.value || "").trim();
      if (text) addChip({ field: "search", value: text, label: `"${text}"` });
    } else if (node.dataset.field && node.dataset.value) {
      addChip({
        field: node.dataset.field,
        value: node.dataset.value,
        label: node.dataset.label || node.dataset.value,
        tone: node.dataset.tone || "",
      });
    } else {
      const tok = KEYWORD_TOKENS[node.dataset.kw];
      if (tok) addChip({ ...tok });
    }
    input.value = "";
    updateInputCursorWidth();
    hideSuggest();
    focusInputEnd();
  };

  const onSuggestAfterSwap = (event) => {
    if (event.target !== suggest) return;
    suggest.hidden = suggest.textContent.trim() === "";
  };

  const suggestVisible = () =>
    suggest instanceof HTMLElement && suggest.hidden === false;

  const moveSuggestHl = (dir) => {
    if (!(suggest instanceof HTMLElement)) return false;
    const items = suggest.querySelectorAll(".osspm-askbar-suggest-item");
    if (items.length === 0) return false;
    let idx = Array.from(items).findIndex((el) => el.classList.contains("is-hl"));
    if (idx === -1) idx = dir > 0 ? -1 : items.length;
    idx = Math.max(0, Math.min(items.length - 1, idx + dir));
    items.forEach((el) => el.classList.remove("is-hl"));
    items[idx].classList.add("is-hl");
    items[idx].scrollIntoView({ block: "nearest" });
    return true;
  };

  const activateSuggestHl = () => {
    if (!(suggest instanceof HTMLElement)) return false;
    const hl = suggest.querySelector(".osspm-askbar-suggest-item.is-hl");
    if (!hl) return false;
    hl.click();
    return true;
  };

  const shouldActivateSuggestionOnEnter = () => {
    const txt = input.value.trim();
    if (!txt) return false;
    const colonIdx = txt.indexOf(":");
    if (colonIdx > 0 && FIELD_ALIASES[txt.slice(0, colonIdx).toLowerCase()]) {
      return false;
    }
    return !/\s/.test(txt);
  };

  const onBarClick = (e) => {
    if (e.target === bar) {
      placeInputAt(state.chips.length);
      return;
    }
    if (!e.target.closest("[data-osspm-askbar-chip]")) {
      focusInputEnd();
    }
  };

  const onInputInput = (e) => {
    updateInputCursorWidth();
    showSuggest(e.target.value);
  };

  const onAddFilterClick = (e) => {
    e.preventDefault();
    e.stopPropagation();
    placeInputAt(state.chips.length);
    showSuggest(input.value, true);
  };

  const onInputKeydown = (e) => {
    if (input.value === "" && handleInsertionArrow(e)) return;
    if (suggestVisible() && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
      if (moveSuggestHl(e.key === "ArrowDown" ? 1 : -1)) e.preventDefault();
      return;
    }
    if (
      suggestVisible() &&
      (e.key === "Tab" || (e.key === "Enter" && shouldActivateSuggestionOnEnter()))
    ) {
      if (activateSuggestHl()) {
        e.preventDefault();
        return;
      }
    }
    if (e.key === "Enter") {
      const txt = input.value.trim();
      if (txt === "") {
        e.preventDefault();
        return;
      }
      e.preventDefault();
      const lc = txt.toLowerCase();
      if (KEYWORD_TOKENS[lc]) {
        addChip(KEYWORD_TOKENS[lc]);
      } else if (hasVocabulary) {
        parseToTokens(txt).forEach(addChip);
      } else {
        addChip({ field: "search", value: txt, label: `"${txt}"` });
      }
      input.value = "";
      updateInputCursorWidth();
      hideSuggest();
      return;
    }
    if (e.key === "Backspace" && input.value === "" && state.insertionIndex > 0) {
      state.chips.splice(state.insertionIndex - 1, 1);
      state.insertionIndex--;
      renderChips();
      syncAndSubmit();
      placeInputAt(state.insertionIndex);
      return;
    }
    if (
      e.key === "Delete" &&
      input.value === "" &&
      state.insertionIndex < state.chips.length
    ) {
      state.chips.splice(state.insertionIndex, 1);
      renderChips();
      syncAndSubmit();
      placeInputAt(state.insertionIndex);
      return;
    }
    if (e.key === "Escape") {
      hideSuggest();
    }
  };

  const onDocClick = (e) => {
    if (!el.contains(e.target)) hideSuggest();
  };

  renderChips();

  bar.addEventListener("click", onBarClick);
  input.addEventListener("input", onInputInput);
  input.addEventListener("keydown", onInputKeydown);
  if (addFilterButton instanceof HTMLElement) {
    addFilterButton.addEventListener("click", onAddFilterClick);
  }
  if (suggest instanceof HTMLElement) {
    suggest.addEventListener("click", onSuggestClick);
    suggest.addEventListener("htmx:afterSwap", onSuggestAfterSwap);
  }
  doc.addEventListener("click", onDocClick);
  doc.addEventListener("htmx:pushedIntoHistory", onHistoryURLChange);
  doc.addEventListener("htmx:replacedInHistory", onHistoryURLChange);
  win.addEventListener("popstate", onHistoryURLChange);

  return () => {
    if (addFilterButton instanceof HTMLElement) {
      addFilterButton.removeEventListener("click", onAddFilterClick);
    }
    win.clearTimeout(serverSuggestTimer);
    if (serverSuggestController) {
      serverSuggestController.abort();
      serverSuggestController = null;
    }
    if (suggest instanceof HTMLElement) {
      suggest.removeEventListener("click", onSuggestClick);
      suggest.removeEventListener("htmx:afterSwap", onSuggestAfterSwap);
    }
    doc.removeEventListener("click", onDocClick);
    doc.removeEventListener("htmx:pushedIntoHistory", onHistoryURLChange);
    doc.removeEventListener("htmx:replacedInHistory", onHistoryURLChange);
    win.removeEventListener("popstate", onHistoryURLChange);
  };
};

register(
  "osspm_askbar",
  "[data-osspm-askbar]:not([data-osspm_askbar-initialized])",
  initAskbar,
);
