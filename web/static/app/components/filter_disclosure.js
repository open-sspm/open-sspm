/**
 * Filter Disclosure component
 *
 * Adds click-outside-to-close and Escape-to-close behavior
 * to native <details>/<summary> filter disclosure elements.
 */
import { register } from "./registry.js";

const init = (el) => {
  const doc = el.ownerDocument;

  const onDocumentClick = (e) => {
    if (!el.open) return;
    if (el.contains(e.target)) return;
    el.removeAttribute("open");
  };

  const onKeydown = (e) => {
    if (!el.open) return;
    if (e.key !== "Escape") return;
    e.preventDefault();
    el.removeAttribute("open");
    const summary = el.querySelector("summary");
    if (summary instanceof HTMLElement) summary.focus();
  };

  doc.addEventListener("click", onDocumentClick);
  doc.addEventListener("keydown", onKeydown);

  return () => {
    doc.removeEventListener("click", onDocumentClick);
    doc.removeEventListener("keydown", onKeydown);
  };
};

register(
  "filter_disclosure",
  ".osspm-filters-disclosure:not([data-filter_disclosure-initialized])",
  init,
);