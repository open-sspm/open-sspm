/**
 * Tabs component
 *
 * Arrow key navigation and panel management for tab interfaces.
 */
import { register } from "./registry.js";

const init = (el) => {
  const tablist = el.querySelector('[role="tablist"]');
  if (!tablist) return;

  const tabs = Array.from(tablist.querySelectorAll('[role="tab"]'));
  if (tabs.length === 0) return;

  const activate = (tab) => {
    tabs.forEach((t) => {
      const selected = t === tab;
      t.setAttribute("aria-selected", String(selected));
      t.tabIndex = selected ? 0 : -1;
      const panelId = t.getAttribute("aria-controls");
      if (panelId) {
        const panel = el.querySelector(`#${panelId}`);
        if (panel) panel.hidden = !selected;
      }
    });
    tab.focus();
  };

  // Initialize: ensure first selected tab is active
  const selected = tabs.find((t) => t.getAttribute("aria-selected") === "true") || tabs[0];
  activate(selected);

  tablist.addEventListener("keydown", (e) => {
    const currentIndex = tabs.indexOf(document.activeElement);
    if (currentIndex === -1) return;

    let nextIndex;
    switch (e.key) {
      case "ArrowRight":
      case "ArrowDown":
        e.preventDefault();
        nextIndex = (currentIndex + 1) % tabs.length;
        activate(tabs[nextIndex]);
        break;
      case "ArrowLeft":
      case "ArrowUp":
        e.preventDefault();
        nextIndex = (currentIndex - 1 + tabs.length) % tabs.length;
        activate(tabs[nextIndex]);
        break;
      case "Home":
        e.preventDefault();
        activate(tabs[0]);
        break;
      case "End":
        e.preventDefault();
        activate(tabs[tabs.length - 1]);
        break;
    }
  });

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => activate(tab));
  });
};

register("tabs", ".tabs:not([data-tabs-initialized])", init);
