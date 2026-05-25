/**
 * Page title sync component.
 *
 * The header h1 (#page-title) is layout chrome that displays the current page
 * title. Since boosted navigations swap only #main and history restores cache
 * only the swap target, the h1 must be kept in sync from document.title (which
 * htmx maintains correctly for both boosted nav and history restore).
 *
 * The site title suffix (" · Open-SSPM") is stripped to recover the bare page
 * title that the server originally rendered into data.Title.
 */
import { register } from "./registry.js";

const SITE_SUFFIX = " · Open-SSPM";

const derivePageTitle = (docTitle) => {
  const trimmed = (docTitle || "").trim();
  if (trimmed.endsWith(SITE_SUFFIX)) {
    return trimmed.slice(0, -SITE_SUFFIX.length);
  }
  return trimmed;
};

const init = (el) => {
  const doc = el.ownerDocument;
  const view = doc.defaultView;
  const viewTarget =
    view &&
    typeof view.addEventListener === "function" &&
    typeof view.removeEventListener === "function"
      ? view
      : null;
  const sync = () => {
    const next = derivePageTitle(doc.title);
    if (next && next !== el.textContent) el.textContent = next;
  };

  // Initial render is already correct (server-rendered from data.Title), but
  // resync defensively in case the server-rendered values drift apart.
  sync();

  doc.addEventListener("htmx:afterSettle", sync);
  viewTarget?.addEventListener("popstate", sync);

  return () => {
    if (typeof doc.removeEventListener === "function") {
      doc.removeEventListener("htmx:afterSettle", sync);
    }
    if (typeof viewTarget?.removeEventListener === "function") {
      viewTarget.removeEventListener("popstate", sync);
    }
  };
};

register("page-title", "#page-title:not([data-page-title-initialized])", init);
