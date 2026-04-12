/**
 * Popover component
 *
 * Click-toggle popover with Escape close and click-outside close.
 * Dispatches osspm:popover events for coordination between popovers.
 */
import { register } from "./registry.js";

const init = (el) => {
  const doc = el.ownerDocument;
  const trigger = el.querySelector("[aria-controls], [aria-expanded]");
  const popover = el.querySelector("[data-popover]");
  if (!trigger || !popover) return;

  const isOpen = () => popover.getAttribute("aria-hidden") !== "true";

  const open = () => {
    doc.dispatchEvent(
      new CustomEvent("osspm:popover", { detail: { source: el } }),
    );
    popover.setAttribute("aria-hidden", "false");
    trigger.setAttribute("aria-expanded", "true");
  };

  const close = () => {
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
    if (e.key === "Escape" && isOpen()) {
      e.preventDefault();
      close();
      trigger.focus();
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

  return () => {
    trigger.removeEventListener("click", onTriggerClick);
    doc.removeEventListener("keydown", onKeydown);
    doc.removeEventListener("click", onDocumentClick);
    doc.removeEventListener("osspm:popover", onPopover);
  };
};

register("popover", ".popover:not([data-popover-initialized])", init);
