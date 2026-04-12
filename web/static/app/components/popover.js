/**
 * Popover component
 *
 * Click-toggle popover with Escape close and click-outside close.
 * Dispatches osspm:popover events for coordination between popovers.
 */
import { register } from "./registry.js";

const init = (el) => {
  const trigger = el.querySelector("[aria-controls], [aria-expanded]");
  const popover = el.querySelector("[data-popover]");
  if (!trigger || !popover) return;

  const isOpen = () => popover.getAttribute("aria-hidden") !== "true";

  const open = () => {
    document.dispatchEvent(
      new CustomEvent("osspm:popover", { detail: { source: el } }),
    );
    popover.setAttribute("aria-hidden", "false");
    trigger.setAttribute("aria-expanded", "true");
  };

  const close = () => {
    popover.setAttribute("aria-hidden", "true");
    trigger.setAttribute("aria-expanded", "false");
  };

  trigger.addEventListener("click", (e) => {
    e.preventDefault();
    if (isOpen()) close();
    else open();
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape" && isOpen()) {
      e.preventDefault();
      close();
      trigger.focus();
    }
  });

  document.addEventListener("click", (e) => {
    if (!isOpen()) return;
    if (el.contains(e.target)) return;
    close();
  });

  // Close when another popover opens
  document.addEventListener("osspm:popover", (e) => {
    if (e.detail?.source !== el && isOpen()) close();
  });
};

register("popover", ".popover:not([data-popover-initialized])", init);
