import { register } from "./registry.js";

const FLOATING_TRANSITION_MS = 120;
const VIEWPORT_PADDING = 8;
const CHIP_REMOVE_ICON = `
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" class="size-3.5" aria-hidden="true">
    <path fill-rule="evenodd" d="M4.293 4.293a1 1 0 0 1 1.414 0L10 8.586l4.293-4.293a1 1 0 1 1 1.414 1.414L11.414 10l4.293 4.293a1 1 0 0 1-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 0 1-1.414-1.414L8.586 10 4.293 5.707a1 1 0 0 1 0-1.414Z" clip-rule="evenodd"/>
  </svg>
`;

let pendingFocus = null;

const escapeSelector = (value) => {
  if (globalThis.CSS && typeof globalThis.CSS.escape === "function") {
    return globalThis.CSS.escape(value);
  }
  return String(value).replace(/["\\]/g, "\\$&");
};

const hasHtmxBehavior = (form) =>
  form instanceof HTMLFormElement &&
  ["hx-get", "hx-post", "hx-put", "hx-patch", "hx-delete"].some((attr) =>
    form.hasAttribute(attr),
  );

const parseField = (el) => ({
  id: el.dataset.fieldId || "",
  label: el.dataset.fieldLabel || "",
  kind: el.dataset.fieldKind || "",
  activeValue: el.dataset.fieldActiveValue || "",
  inputNames: Array.from(
    el.querySelectorAll("[data-table-query-input-name]"),
    (input) => input.getAttribute("value") || "",
  ).filter(Boolean),
  options: Array.from(el.querySelectorAll("[data-table-query-option]"), (optionEl) => ({
    value: optionEl.dataset.optionValue || "",
    label: optionEl.dataset.optionLabel || "",
    controls: Array.from(
      optionEl.querySelectorAll("[data-table-query-option-control]"),
      (input) => ({
        name: input.dataset.controlName || "",
        value: input.getAttribute("value") || "",
      }),
    ).filter((control) => control.name),
  })).filter((option) => option.value),
});

const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

export const positionFloatingPanel = (panel, anchor, { align = "start" } = {}) => {
  if (!(panel instanceof HTMLElement) || !(anchor instanceof HTMLElement)) return;

  const anchorRect = anchor.getBoundingClientRect();
  const panelRect = panel.getBoundingClientRect();
  const maxLeft = Math.max(
    VIEWPORT_PADDING,
    window.innerWidth - panelRect.width - VIEWPORT_PADDING,
  );
  const maxTop = Math.max(
    VIEWPORT_PADDING,
    window.innerHeight - panelRect.height - VIEWPORT_PADDING,
  );

  let left =
    align === "end"
      ? anchorRect.right - panelRect.width
      : anchorRect.left;
  left = clamp(left, VIEWPORT_PADDING, maxLeft);

  let top = anchorRect.bottom + 8;
  if (top > maxTop) {
    top = anchorRect.top - panelRect.height - 8;
  }
  top = clamp(top, VIEWPORT_PADDING, maxTop);

  panel.style.left = `${left}px`;
  panel.style.top = `${top}px`;
};

const removeNamedControls = (bank, inputNames) => {
  if (!(bank instanceof HTMLElement)) return;
  const names = new Set(inputNames.filter(Boolean));
  if (names.size === 0) return;

  bank.querySelectorAll("[data-table-query-control]").forEach((input) => {
    if (!(input instanceof HTMLInputElement)) return;
    if (!names.has(input.name)) return;
    input.remove();
  });
};

const upsertControls = (bank, controls) => {
  if (!(bank instanceof HTMLElement)) return;

  controls.forEach((control) => {
    if (!control?.name) return;
    const existing = bank.querySelector(
      `input[data-table-query-control-name="${escapeSelector(control.name)}"]`,
    );
    if (existing instanceof HTMLInputElement) {
      existing.value = control.value || "";
      existing.setAttribute("value", control.value || "");
      return;
    }

    const input = bank.ownerDocument.createElement("input");
    input.type = "hidden";
    input.name = control.name;
    input.value = control.value || "";
    input.dataset.tableQueryControl = "";
    input.dataset.tableQueryControlName = control.name;
    bank.append(input);
  });
};

const requestFormSubmission = (
  form,
  trigger,
  searchInput,
  { omitEmptySearch = false } = {},
) => {
  if (!(form instanceof HTMLFormElement)) return;

  if (hasHtmxBehavior(form) && trigger instanceof HTMLElement) {
    trigger.dispatchEvent(new Event("change", { bubbles: true }));
    return;
  }

  if (searchInput instanceof HTMLInputElement) {
    searchInput.value = searchInput.defaultValue;
  }

  const shouldOmitSearch =
    omitEmptySearch &&
    searchInput instanceof HTMLInputElement &&
    searchInput.defaultValue === "" &&
    (searchInput.getAttribute("name") || "").trim() !== "";

  const originalSearchName = shouldOmitSearch ? searchInput.getAttribute("name") : null;
  if (shouldOmitSearch) {
    searchInput.removeAttribute("name");
  }

  const submit = () => {
    if (typeof form.requestSubmit === "function") {
      form.requestSubmit();
      return;
    }

    form.submit();
  };

  try {
    submit();
  } finally {
    if (shouldOmitSearch && originalSearchName) {
      searchInput.setAttribute("name", originalSearchName);
    }
  }
};

const restorePendingFocus = (root) => {
  if (!(root instanceof HTMLElement) || !pendingFocus) return;

  let target = null;
  if (pendingFocus.kind === "search") {
    target = root.querySelector('input[type="search"][name]');
  }
  if (!target && pendingFocus.kind === "add") {
    target = root.querySelector("[data-table-query-add-trigger]");
  }
  if (!target && pendingFocus.kind === "chip" && pendingFocus.fieldId) {
    target = root.querySelector(
      `[data-table-query-chip-trigger][data-field-id="${escapeSelector(pendingFocus.fieldId)}"]`,
    );
  }
  if (!target) {
    target =
      root.querySelector("[data-table-query-add-trigger]") ||
      root.querySelector('input[type="search"][name]');
  }

  if (target instanceof HTMLElement) {
    target.focus();
  }

  pendingFocus = null;
};

export const initTableQueryBar = (el) => {
  if (!(el instanceof HTMLElement)) return () => {};

  const doc = el.ownerDocument;
  const win = doc.defaultView || window;
  const form = el.closest("form");
  const bank = el.querySelector("[data-table-query-controls]");
  const searchInput = el.querySelector('input[type="search"][name]');
  const clearSearchButton = el.querySelector("[data-table-query-clear-search]");
  const addTrigger = el.querySelector("[data-table-query-add-trigger]");
  const clearFiltersButton = el.querySelector("[data-table-query-clear-filters]");
  const chipList = el.querySelector("[data-table-query-chip-list]");
  const picker = el.querySelector("[data-table-query-picker]");
  const pickerSearchInput = el.querySelector(
    "[data-table-query-picker-search-input]",
  );
  const pickerItems = Array.from(
    el.querySelectorAll("[data-table-query-picker-item]"),
  );
  const editor = el.querySelector("[data-table-query-editor]");
  const editorTitle = el.querySelector("[data-table-query-editor-title]");
  const editorSelect = el.querySelector("[data-table-query-editor-select]");
  const editorCloseButton = el.querySelector("[data-table-query-editor-close]");
  const fieldMap = new Map(
    Array.from(el.querySelectorAll("[data-table-query-field]"), (fieldEl) => {
      const field = parseField(fieldEl);
      return [field.id, field];
    }),
  );

  let pickerAnchor = null;
  let editorAnchor = null;
  let editorFieldId = "";
  let draftFieldId = "";
  let draftChip = null;
  let pickerCloseTimer = 0;
  let editorCloseTimer = 0;

  const clearFloatingTimeout = (type) => {
    if (type === "picker" && pickerCloseTimer) {
      win.clearTimeout(pickerCloseTimer);
      pickerCloseTimer = 0;
    }
    if (type === "editor" && editorCloseTimer) {
      win.clearTimeout(editorCloseTimer);
      editorCloseTimer = 0;
    }
  };

  const closePicker = ({ restoreFocus = true } = {}) => {
    if (!(picker instanceof HTMLElement) || picker.hidden) return;
    clearFloatingTimeout("picker");
    picker.dataset.state = "closed";
    if (addTrigger instanceof HTMLElement) {
      addTrigger.setAttribute("aria-expanded", "false");
    }
    pickerCloseTimer = win.setTimeout(() => {
      picker.hidden = true;
      picker.style.left = "";
      picker.style.top = "";
    }, FLOATING_TRANSITION_MS);
    if (restoreFocus && pickerAnchor instanceof HTMLElement) {
      pickerAnchor.focus();
    }
    pickerAnchor = null;
  };

  const removeDraftChip = ({ restoreFocus = false } = {}) => {
    if (!(draftChip instanceof HTMLElement)) return;
    draftChip.remove();
    draftChip = null;
    draftFieldId = "";

    if (restoreFocus && addTrigger instanceof HTMLElement) {
      addTrigger.focus();
    }
  };

  const closeEditor = ({ restoreFocus = true, discardDraft = false } = {}) => {
    if (!(editor instanceof HTMLElement) || editor.hidden) return;
    clearFloatingTimeout("editor");
    editor.dataset.state = "closed";
    const focusTarget = editorAnchor;
    const shouldDiscardDraft = discardDraft && editorFieldId !== "" && editorFieldId === draftFieldId;
    if (focusTarget instanceof HTMLElement) {
      focusTarget.setAttribute("aria-expanded", "false");
    }
    editorCloseTimer = win.setTimeout(() => {
      editor.hidden = true;
      editor.style.left = "";
      editor.style.top = "";
    }, FLOATING_TRANSITION_MS);
    if (shouldDiscardDraft) {
      removeDraftChip();
    }
    if (restoreFocus) {
      if (shouldDiscardDraft && addTrigger instanceof HTMLElement) {
        addTrigger.focus();
      } else if (focusTarget instanceof HTMLElement) {
        focusTarget.focus();
      }
    }
    editorAnchor = null;
    editorFieldId = "";
  };

  const openFloating = (panel, anchor, options) => {
    if (!(panel instanceof HTMLElement) || !(anchor instanceof HTMLElement)) return;
    panel.hidden = false;
    panel.dataset.state = "closed";
    positionFloatingPanel(panel, anchor, options);
    win.requestAnimationFrame(() => {
      panel.dataset.state = "open";
    });
  };

  const submitFilterChange = () => {
    if (!(form instanceof HTMLFormElement) || !(bank instanceof HTMLElement)) return;

    const pageInput = bank.querySelector('input[name="page"]');
    if (pageInput instanceof HTMLInputElement) {
      pageInput.remove();
    }

    requestFormSubmission(form, bank, searchInput);
  };

  const createDraftChip = (fieldId) => {
    const field = fieldMap.get(fieldId);
    if (!field) return null;
    if (!(chipList instanceof HTMLElement)) return null;

    removeDraftChip();

    const chip = doc.createElement("div");
    chip.className = "osspm-table-query-chip";
    chip.dataset.tableQueryChip = "";
    chip.dataset.tableQueryDraftChip = "";
    chip.dataset.fieldId = fieldId;

    const trigger = doc.createElement("button");
    trigger.type = "button";
    trigger.className = "osspm-table-query-chip-trigger";
    trigger.setAttribute("aria-expanded", "false");
    trigger.setAttribute("aria-haspopup", "dialog");
    trigger.dataset.tableQueryChipTrigger = "";
    trigger.dataset.tableQueryDraftChipTrigger = "";
    trigger.dataset.fieldId = fieldId;
    const label = doc.createElement("span");
    label.textContent = field.label;
    trigger.append(label);
    trigger.addEventListener("click", () => {
      openEditor(fieldId, trigger, "draft");
    });

    const removeButton = doc.createElement("button");
    removeButton.type = "button";
    removeButton.className = "osspm-table-query-chip-remove";
    removeButton.setAttribute("aria-label", `Remove ${field.label} filter`);
    removeButton.dataset.tableQueryChipRemove = "";
    removeButton.dataset.tableQueryDraftChipRemove = "";
    removeButton.dataset.fieldId = fieldId;
    removeButton.innerHTML = CHIP_REMOVE_ICON;
    removeButton.addEventListener("click", () => {
      closeEditor({ restoreFocus: false });
      removeDraftChip({ restoreFocus: true });
    });

    chip.append(trigger, removeButton);
    chipList.append(chip);
    draftFieldId = fieldId;
    draftChip = chip;

    return trigger;
  };

  const applyFieldOption = (fieldId, optionValue, focusKind) => {
    const field = fieldMap.get(fieldId);
    if (!field) return;
    const option = field.options.find((candidate) => candidate.value === optionValue);
    if (!option) return;

    removeNamedControls(bank, field.inputNames);
    upsertControls(bank, option.controls);

    pendingFocus = { kind: focusKind === "draft" ? "chip" : focusKind, fieldId };
    closeEditor({ restoreFocus: false });
    closePicker({ restoreFocus: false });
    if (focusKind === "draft") {
      removeDraftChip();
    }
    submitFilterChange();
  };

  const clearField = (fieldId) => {
    const field = fieldMap.get(fieldId);
    if (!field) return;

    if (fieldId === draftFieldId) {
      closeEditor({ restoreFocus: false });
      removeDraftChip({ restoreFocus: true });
      return;
    }

    removeNamedControls(bank, field.inputNames);
    pendingFocus = { kind: "chip", fieldId };
    closeEditor({ restoreFocus: false });
    closePicker({ restoreFocus: false });
    submitFilterChange();
  };

  const openEditor = (fieldId, anchor, focusKind = "chip") => {
    const field = fieldMap.get(fieldId);
    if (!field) return;
    if (!(editor instanceof HTMLElement)) return;
    if (!(editorTitle instanceof HTMLElement)) return;
    if (!(editorSelect instanceof HTMLSelectElement)) return;
    if (!(anchor instanceof HTMLElement)) return;

    if (focusKind !== "draft" && draftFieldId) {
      removeDraftChip();
    }

    closePicker({ restoreFocus: false });
    clearFloatingTimeout("editor");

    editorTitle.textContent = field.label;
    editorSelect.innerHTML = "";
    const needsPlaceholder = !field.activeValue;
    if (needsPlaceholder) {
      const placeholderOption = doc.createElement("option");
      placeholderOption.value = "";
      placeholderOption.textContent = `Select ${field.label}`;
      placeholderOption.disabled = true;
      placeholderOption.selected = true;
      editorSelect.append(placeholderOption);
    }
    field.options.forEach((option) => {
      const optionEl = doc.createElement("option");
      optionEl.value = option.value;
      optionEl.textContent = option.label;
      editorSelect.append(optionEl);
    });

    if (field.activeValue && field.options.some((option) => option.value === field.activeValue)) {
      editorSelect.value = field.activeValue;
    } else if (!needsPlaceholder && field.options.length > 0) {
      editorSelect.value = field.options[0].value;
    } else if (needsPlaceholder) {
      editorSelect.value = "";
    }

    editorAnchor?.setAttribute("aria-expanded", "false");
    editorAnchor = anchor;
    editorFieldId = fieldId;
    editorAnchor.setAttribute("aria-expanded", "true");
    editor.dataset.focusKind = focusKind;

    openFloating(editor, anchor, { align: "start" });
    win.requestAnimationFrame(() => {
      editorSelect.focus();
    });
  };

  const openPicker = (anchor) => {
    if (!(picker instanceof HTMLElement)) return;
    if (!(anchor instanceof HTMLElement)) return;
    clearFloatingTimeout("picker");
    closeEditor({ restoreFocus: false });
    pickerAnchor = anchor;
    openFloating(picker, anchor, { align: "end" });
    anchor.setAttribute("aria-expanded", "true");
    if (pickerSearchInput instanceof HTMLInputElement) {
      pickerSearchInput.value = "";
      pickerItems.forEach((item) => {
        if (!(item instanceof HTMLElement)) return;
        item.hidden = false;
      });
      win.requestAnimationFrame(() => {
        pickerSearchInput.focus();
      });
    }
  };

  const updateOpenPositions = () => {
    if (pickerAnchor instanceof HTMLElement && picker instanceof HTMLElement && !picker.hidden) {
      positionFloatingPanel(picker, pickerAnchor, { align: "end" });
    }
    if (editorAnchor instanceof HTMLElement && editor instanceof HTMLElement && !editor.hidden) {
      positionFloatingPanel(editor, editorAnchor, { align: "start" });
    }
  };

  const onDocumentClick = (event) => {
    if (el.contains(event.target)) return;
    closePicker();
    closeEditor({ discardDraft: true });
    removeDraftChip({ restoreFocus: true });
  };

  const onDocumentKeydown = (event) => {
    if (event.key !== "Escape") return;
    if (editor instanceof HTMLElement && !editor.hidden) {
      event.preventDefault();
      closeEditor({ discardDraft: true });
      return;
    }
    if (picker instanceof HTMLElement && !picker.hidden) {
      event.preventDefault();
      closePicker();
      removeDraftChip({ restoreFocus: true });
      return;
    }
    if (draftChip instanceof HTMLElement) {
      event.preventDefault();
      removeDraftChip({ restoreFocus: true });
    }
  };

  const onAddTriggerClick = () => {
    if (!(addTrigger instanceof HTMLElement)) return;
    if (picker instanceof HTMLElement && !picker.hidden) {
      closePicker();
      return;
    }
    openPicker(addTrigger);
  };

  const onClearFiltersClick = () => {
    const allNames = Array.from(fieldMap.values()).flatMap((field) => field.inputNames);
    removeNamedControls(bank, allNames);
    pendingFocus = { kind: "add" };
    closePicker({ restoreFocus: false });
    closeEditor({ restoreFocus: false });
    removeDraftChip();
    submitFilterChange();
  };

  const onClearSearchClick = () => {
    if (!(searchInput instanceof HTMLInputElement)) return;
    searchInput.value = "";
    searchInput.defaultValue = "";
    pendingFocus = { kind: "search" };

    closePicker({ restoreFocus: false });
    closeEditor({ restoreFocus: false });
    removeDraftChip();

    if (bank instanceof HTMLElement) {
      const pageInput = bank.querySelector('input[name="page"]');
      if (pageInput instanceof HTMLInputElement) {
        pageInput.remove();
      }
    }

    if (form instanceof HTMLFormElement) {
      requestFormSubmission(form, bank, searchInput, { omitEmptySearch: true });
    }
  };

  const onPickerSearchInput = (event) => {
    const value =
      event.currentTarget instanceof HTMLInputElement
        ? event.currentTarget.value.trim().toLowerCase()
        : "";
    pickerItems.forEach((item) => {
      if (!(item instanceof HTMLElement)) return;
      const label =
        item
          .querySelector(".osspm-table-query-picker-item-label")
          ?.textContent?.trim()
          .toLowerCase() || "";
      item.hidden = value !== "" && !label.includes(value);
    });
  };

  const onEditorChange = (event) => {
    if (!(event.currentTarget instanceof HTMLSelectElement)) return;
    if (!editorFieldId) return;
    const focusKind = editor?.dataset.focusKind || "chip";
    applyFieldOption(editorFieldId, event.currentTarget.value, focusKind);
  };

  const chipRemoveButtons = Array.from(
    el.querySelectorAll("[data-table-query-chip-remove]"),
  );
  chipRemoveButtons.forEach((button) => {
    button.addEventListener("click", () => {
      if (!(button instanceof HTMLElement)) return;
      clearField(button.dataset.fieldId || "");
    });
  });

  const chipTriggers = Array.from(el.querySelectorAll("[data-table-query-chip-trigger]"));
  chipTriggers.forEach((button) => {
    button.addEventListener("click", () => {
      if (!(button instanceof HTMLElement)) return;
      openEditor(button.dataset.fieldId || "", button, "chip");
    });
  });

  pickerItems.forEach((item) => {
    item.addEventListener("click", () => {
      if (!(item instanceof HTMLElement)) return;
      const draftTrigger = createDraftChip(item.dataset.fieldId || "");
      if (draftTrigger instanceof HTMLElement) {
        openEditor(item.dataset.fieldId || "", draftTrigger, "draft");
      }
    });
  });

  addTrigger?.addEventListener("click", onAddTriggerClick);
  clearFiltersButton?.addEventListener("click", onClearFiltersClick);
  clearSearchButton?.addEventListener("click", onClearSearchClick);
  pickerSearchInput?.addEventListener("input", onPickerSearchInput);
  editorSelect?.addEventListener("change", onEditorChange);
  editorCloseButton?.addEventListener("click", () => closeEditor({ discardDraft: true }));

  doc.addEventListener("click", onDocumentClick);
  doc.addEventListener("keydown", onDocumentKeydown);
  win.addEventListener("resize", updateOpenPositions);
  doc.addEventListener("scroll", updateOpenPositions, true);

  restorePendingFocus(el);

  return () => {
    clearFloatingTimeout("picker");
    clearFloatingTimeout("editor");
    doc.removeEventListener("click", onDocumentClick);
    doc.removeEventListener("keydown", onDocumentKeydown);
    win.removeEventListener("resize", updateOpenPositions);
    doc.removeEventListener("scroll", updateOpenPositions, true);
  };
};

register(
  "table_query_bar",
  "[data-table-query-bar]:not([data-table_query_bar-initialized])",
  initTableQueryBar,
);
