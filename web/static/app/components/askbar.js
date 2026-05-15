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

  const doc = el.ownerDocument;
  const bar = el.querySelector("[data-osspm-askbar-bar]");
  const chipHost = el.querySelector("[data-osspm-askbar-chips]");
  const input = el.querySelector("[data-osspm-askbar-input]");
  const addFilterButton = el.querySelector("[data-osspm-askbar-add-filter]");
  const suggest = el.querySelector("[data-osspm-askbar-suggest]");
  const bank = el.querySelector("[data-osspm-askbar-bank]");
  const form = el.closest("form");

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

  const state = {
    chips: readInitialChips(chipHost),
    insertionIndex: 0,
  };
  state.insertionIndex = state.chips.length;

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

  const syncAndSubmit = () => {
    bank.innerHTML = "";
    const params = {};
    for (const hidden of STATIC_HIDDEN) {
      const name = String(hidden?.name ?? "").trim();
      if (!name) continue;
      params[name] = String(hidden?.value ?? "");
    }
    for (const c of state.chips) {
      if (c.field === "search") {
        params.q = params.q ? `${params.q} ${c.value}` : c.value;
        continue;
      }
      const param = FIELD_PARAM[c.field];
      if (!param) continue;
      params[param] = c.value;
    }
    for (const [name, value] of Object.entries(params)) {
      const node = doc.createElement("input");
      node.type = "hidden";
      node.name = name;
      node.value = value;
      node.defaultValue = value;
      bank.append(node);
    }
    bank.dispatchEvent(new Event("change", { bubbles: true }));
    if (form instanceof HTMLFormElement && !hasHtmxBehavior) {
      if (typeof form.requestSubmit === "function") {
        form.requestSubmit();
      } else {
        form.submit();
      }
    }
  };

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
          <div class="osspm-askbar-suggest-item" data-action="freetext">
            <span class="osspm-askbar-suggest-key">Search</span>
            <span class="osspm-askbar-suggest-val">"${escapeHtml(displayText)}"</span>
          </div>`;
      }
    } else {
      html = `<div class="osspm-askbar-suggest-label">Free text</div>
        <div class="osspm-askbar-suggest-item is-hl" data-action="freetext">
          <span class="osspm-askbar-suggest-val">Add "${escapeHtml(
            displayText,
          )}" as text search</span>
        </div>`;
    }
    suggest.innerHTML = html;
    suggest.hidden = false;
    bindSuggest(displayText);
  };

  const bindSuggest = (text) => {
    if (!(suggest instanceof HTMLElement)) return;
    suggest.querySelectorAll(".osspm-askbar-suggest-item").forEach((node) => {
      node.addEventListener("click", () => {
        if (node.dataset.action === "freetext") {
          addChip({ field: "search", value: text, label: `"${text}"` });
        } else {
          const tok = KEYWORD_TOKENS[node.dataset.kw];
          if (tok) addChip({ ...tok });
        }
        input.value = "";
        updateInputCursorWidth();
        hideSuggest();
        focusInputEnd();
      });
    });
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
  doc.addEventListener("click", onDocClick);

  return () => {
    if (addFilterButton instanceof HTMLElement) {
      addFilterButton.removeEventListener("click", onAddFilterClick);
    }
    doc.removeEventListener("click", onDocClick);
  };
};

register(
  "osspm_askbar",
  "[data-osspm-askbar]:not([data-osspm_askbar-initialized])",
  initAskbar,
);
