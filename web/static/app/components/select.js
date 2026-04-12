/**
 * Custom Select component
 *
 * Replaces div-based .select widgets with single/multi-select,
 * search filtering, keyboard navigation, and hidden input sync.
 */
import { register } from "./registry.js";

const init = (el) => {
  const trigger = el.querySelector("button[aria-expanded]");
  const popover = el.querySelector("[data-popover]");
  if (!trigger || !popover) return;

  const listbox = popover.querySelector('[role="listbox"]');
  if (!listbox) return;

  const searchInput = popover.querySelector('input[role="combobox"]');
  const isMulti = listbox.getAttribute("aria-multiselectable") === "true";

  const getOptions = () =>
    Array.from(
      listbox.querySelectorAll(
        '[role="option"]:not([aria-hidden="true"]):not([aria-disabled="true"]):not([disabled])',
      ),
    );

  let activeOption = null;
  const isOpen = () => popover.getAttribute("aria-hidden") !== "true";

  const clearActive = () => {
    if (activeOption) {
      activeOption.classList.remove("active");
      activeOption = null;
    }
  };

  const setActive = (option) => {
    clearActive();
    if (!option) return;
    activeOption = option;
    activeOption.classList.add("active");
    activeOption.scrollIntoView({ block: "nearest" });
  };

  const open = () => {
    document.dispatchEvent(
      new CustomEvent("osspm:popover", { detail: { source: el } }),
    );
    popover.setAttribute("aria-hidden", "false");
    trigger.setAttribute("aria-expanded", "true");
    if (searchInput) {
      searchInput.value = "";
      searchInput.focus();
      filterOptions();
    }
    const options = getOptions();
    const selected = options.find(
      (o) => o.getAttribute("aria-selected") === "true",
    );
    if (selected) setActive(selected);
    else if (options.length > 0) setActive(options[0]);
  };

  const close = () => {
    clearActive();
    popover.setAttribute("aria-hidden", "true");
    trigger.setAttribute("aria-expanded", "false");
    trigger.focus();
  };

  const syncTriggerLabel = () => {
    const label = trigger.querySelector("span:first-child");
    if (!label) return;

    const selected = Array.from(
      listbox.querySelectorAll('[role="option"][aria-selected="true"]'),
    );

    if (selected.length === 0) {
      label.textContent = trigger.dataset.placeholder || "Select...";
      return;
    }

    if (isMulti) {
      label.textContent = selected.map((o) => o.textContent.trim()).join(", ");
    } else {
      label.textContent = selected[0].textContent.trim();
    }
  };

  const syncHiddenInputs = () => {
    const inputName = el.dataset.name || trigger.dataset.name;
    if (!inputName) return;

    // Remove old hidden inputs
    el.querySelectorAll(`input[type="hidden"][name="${inputName}"]`).forEach(
      (i) => i.remove(),
    );

    const selected = Array.from(
      listbox.querySelectorAll('[role="option"][aria-selected="true"]'),
    );

    selected.forEach((option) => {
      const input = document.createElement("input");
      input.type = "hidden";
      input.name = inputName;
      input.value = option.dataset.value ?? option.textContent.trim();
      el.append(input);
    });
  };

  const selectOption = (option) => {
    if (isMulti) {
      const selected = option.getAttribute("aria-selected") === "true";
      option.setAttribute("aria-selected", String(!selected));
    } else {
      listbox
        .querySelectorAll('[role="option"]')
        .forEach((o) => o.setAttribute("aria-selected", "false"));
      option.setAttribute("aria-selected", "true");
      close();
    }
    syncTriggerLabel();
    syncHiddenInputs();
    trigger.dispatchEvent(new Event("change", { bubbles: true }));
  };

  const filterOptions = () => {
    if (!searchInput) return;
    const query = searchInput.value.trim().toLowerCase();
    const options = listbox.querySelectorAll("[data-value]");

    options.forEach((option) => {
      const text = option.textContent.trim().toLowerCase();
      const keywords = (option.dataset.keywords || "").toLowerCase();
      const match = !query || text.includes(query) || keywords.includes(query);
      option.setAttribute("aria-hidden", String(!match));
    });

    clearActive();
    const visible = getOptions();
    if (visible.length > 0) setActive(visible[0]);
  };

  // Trigger click
  trigger.addEventListener("click", (e) => {
    e.preventDefault();
    if (isOpen()) close();
    else open();
  });

  // Option click
  listbox.addEventListener("click", (e) => {
    const option = e.target.closest('[role="option"]');
    if (!option || option.matches("[aria-disabled='true'], [disabled]")) return;
    selectOption(option);
  });

  // Search input
  if (searchInput) {
    searchInput.addEventListener("input", filterOptions);
  }

  // Keyboard
  const handleKeydown = (e) => {
    if (!isOpen()) {
      if (
        e.key === "ArrowDown" ||
        e.key === "ArrowUp" ||
        e.key === "Enter" ||
        e.key === " "
      ) {
        if (document.activeElement === trigger) {
          e.preventDefault();
          open();
        }
      }
      return;
    }

    const options = getOptions();
    if (options.length === 0 && e.key !== "Escape") return;

    const currentIndex = activeOption ? options.indexOf(activeOption) : -1;

    switch (e.key) {
      case "Escape":
        e.preventDefault();
        close();
        break;
      case "ArrowDown": {
        e.preventDefault();
        const next = currentIndex < options.length - 1 ? currentIndex + 1 : 0;
        setActive(options[next]);
        break;
      }
      case "ArrowUp": {
        e.preventDefault();
        const prev =
          currentIndex > 0 ? currentIndex - 1 : options.length - 1;
        setActive(options[prev]);
        break;
      }
      case "Home": {
        e.preventDefault();
        setActive(options[0]);
        break;
      }
      case "End": {
        e.preventDefault();
        setActive(options[options.length - 1]);
        break;
      }
      case "Enter":
      case " ": {
        if (searchInput && e.key === " ") break;
        e.preventDefault();
        if (activeOption) selectOption(activeOption);
        break;
      }
    }
  };

  el.addEventListener("keydown", handleKeydown);

  // Click outside
  document.addEventListener("click", (e) => {
    if (!isOpen()) return;
    if (el.contains(e.target)) return;
    close();
  });

  // Close when another popover opens
  document.addEventListener("osspm:popover", (e) => {
    if (e.detail?.source !== el && isOpen()) close();
  });

  // Mouse hover
  listbox.addEventListener("mousemove", (e) => {
    const option = e.target.closest('[role="option"]');
    if (option && !option.matches("[aria-disabled='true'], [disabled]")) {
      setActive(option);
    }
  });

  listbox.addEventListener("mouseleave", () => {
    clearActive();
  });

  // Initial sync
  syncTriggerLabel();
};

register("select", "*:not(select).select:not([data-select-initialized])", init);
