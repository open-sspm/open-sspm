/**
 * Dropdown Menu component
 *
 * Popover-based dropdown with keyboard navigation for menu items.
 */
import { register } from "./registry.js";

const init = (el) => {
  const doc = el.ownerDocument;
  const trigger = el.querySelector("[aria-controls], [aria-expanded]");
  const popover = el.querySelector("[data-popover]");
  if (!trigger || !popover) return;

  const getItems = () =>
    Array.from(
      popover.querySelectorAll(
        '[role="menuitem"]:not([aria-hidden="true"]):not([aria-disabled="true"]):not([disabled]),' +
        '[role="menuitemcheckbox"]:not([aria-hidden="true"]):not([aria-disabled="true"]):not([disabled]),' +
        '[role="menuitemradio"]:not([aria-hidden="true"]):not([aria-disabled="true"]):not([disabled])',
      ),
    );

  let activeItem = null;

  const isOpen = () => popover.getAttribute("aria-hidden") !== "true";

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

  const open = () => {
    doc.dispatchEvent(
      new CustomEvent("osspm:popover", { detail: { source: el } }),
    );
    popover.setAttribute("aria-hidden", "false");
    trigger.setAttribute("aria-expanded", "true");
    const items = getItems();
    if (items.length > 0) setActive(items[0]);
  };

  const close = () => {
    clearActive();
    popover.setAttribute("aria-hidden", "true");
    trigger.setAttribute("aria-expanded", "false");
  };

  const onTriggerClick = (e) => {
    e.preventDefault();
    if (isOpen()) close();
    else open();
  };
  trigger.addEventListener("click", onTriggerClick);

  const onKeydown = (e) => {
    if (!isOpen()) return;

    const items = getItems();
    if (items.length === 0) return;

    const currentIndex = activeItem ? items.indexOf(activeItem) : -1;

    switch (e.key) {
      case "Escape":
        e.preventDefault();
        close();
        trigger.focus();
        break;
      case "ArrowDown": {
        e.preventDefault();
        const next = currentIndex < items.length - 1 ? currentIndex + 1 : 0;
        setActive(items[next]);
        break;
      }
      case "ArrowUp": {
        e.preventDefault();
        const prev = currentIndex > 0 ? currentIndex - 1 : items.length - 1;
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
      case "Enter":
      case " ": {
        e.preventDefault();
        if (activeItem) {
          const link = activeItem.querySelector("a") || activeItem;
          link.click();
        }
        close();
        break;
      }
    }
  };
  doc.addEventListener("keydown", onKeydown);

  const onDocumentClick = (e) => {
    if (!isOpen()) return;
    if (el.contains(e.target)) return;
    close();
  };
  doc.addEventListener("click", onDocumentClick);

  // Close when another popover opens
  const onPopover = (e) => {
    if (e.detail?.source !== el && isOpen()) close();
  };
  doc.addEventListener("osspm:popover", onPopover);

  // Mouse hover activates items
  const onMouseMove = (e) => {
    const item = e.target.closest(
      '[role="menuitem"], [role="menuitemcheckbox"], [role="menuitemradio"]',
    );
    if (item && !item.matches("[aria-disabled='true'], [disabled]")) {
      setActive(item);
    }
  };
  popover.addEventListener("mousemove", onMouseMove);

  const onMouseLeave = () => {
    clearActive();
  };
  popover.addEventListener("mouseleave", onMouseLeave);

  return () => {
    trigger.removeEventListener("click", onTriggerClick);
    doc.removeEventListener("keydown", onKeydown);
    doc.removeEventListener("click", onDocumentClick);
    doc.removeEventListener("osspm:popover", onPopover);
    popover.removeEventListener("mousemove", onMouseMove);
    popover.removeEventListener("mouseleave", onMouseLeave);
  };
};

register(
  "dropdown-menu",
  ".dropdown-menu:not([data-dropdown-menu-initialized])",
  init,
);
