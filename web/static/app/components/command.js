/**
 * Command component
 *
 * Input-based filtering and keyboard navigation for command menus.
 */
import { register } from "./registry.js";

const init = (el) => {
  const input = el.querySelector("header input");
  const menu = el.querySelector('[role="menu"]');
  if (!input || !menu) return;

  const getItems = () =>
    Array.from(
      menu.querySelectorAll(
        '[role="menuitem"]:not([aria-hidden="true"]):not([aria-disabled="true"]):not([disabled])',
      ),
    );

  let activeItem = null;

  const clearActive = () => {
    if (activeItem) {
      activeItem.classList.remove("active");
      activeItem = null;
    }
  };

  const setActive = (item) => {
    clearActive();
    if (!item) return;
    activeItem = item;
    activeItem.classList.add("active");
    activeItem.scrollIntoView({ block: "nearest" });
  };

  const filterItems = () => {
    const query = input.value.trim().toLowerCase();
    const items = menu.querySelectorAll("[data-filter]");

    items.forEach((item) => {
      const filter = (item.dataset.filter || "").toLowerCase();
      const keywords = (item.dataset.keywords || "").toLowerCase();
      const match =
        !query || filter.includes(query) || keywords.includes(query);
      item.setAttribute("aria-hidden", String(!match));
    });

    clearActive();
    const visible = getItems();
    if (visible.length > 0) setActive(visible[0]);
  };

  input.addEventListener("input", filterItems);

  input.addEventListener("keydown", (e) => {
    const items = getItems();
    if (items.length === 0) return;

    const currentIndex = activeItem ? items.indexOf(activeItem) : -1;

    switch (e.key) {
      case "ArrowDown": {
        e.preventDefault();
        const next =
          currentIndex < items.length - 1 ? currentIndex + 1 : 0;
        setActive(items[next]);
        break;
      }
      case "ArrowUp": {
        e.preventDefault();
        const prev =
          currentIndex > 0 ? currentIndex - 1 : items.length - 1;
        setActive(items[prev]);
        break;
      }
      case "Home": {
        e.preventDefault();
        setActive(items[0]);
        break;
      }
      case "End": {
        e.preventDefault();
        setActive(items[items.length - 1]);
        break;
      }
      case "Enter": {
        e.preventDefault();
        if (activeItem) {
          const link = activeItem.querySelector("a") || activeItem;
          link.click();
        }
        break;
      }
    }
  });
};

register("command", ".command:not([data-command-initialized])", init);
