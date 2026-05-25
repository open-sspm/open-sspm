/**
 * Boost-link scope.
 *
 * The layout's <body> has hx-boost="true" so internal navigation becomes
 * htmx-driven. We want plain navigation links to swap into #main only — never
 * the whole body — so the sidebar element persists across page changes.
 *
 * Putting hx-target / hx-select / hx-swap on <body> would do that via
 * inheritance, but htmx inherits those attributes into *every* descendant
 * request, including filter forms and pagination whose fragment responses
 * don't contain #main — that breaks them (an empty #main is selected and the
 * fragment target is blanked).
 *
 * Instead we set the boost attributes on the link itself during the capture
 * phase of the click, just before htmx's own click handler reads them.
 * Anything htmx-driven that's not a plain navigation link (explicit hx-target,
 * hx-get, hx-post, etc.) is left alone — it uses its own swap configuration.
 *
 * Form submissions are deliberately *not* decorated: a boosted form without
 * hx-target falls back to htmx's body-swap default, which is what we want for
 * full-page transitions like logout. Anything that needs fragment swapping
 * already declares its own hx-target.
 */

const BOOST_TARGET = "#main";
const BOOST_SELECT = "#main";
const BOOST_SWAP = "outerHTML show:window:top";

const HX_OWN_ATTRS = [
  "hx-target",
  "hx-select",
  "hx-get",
  "hx-post",
  "hx-put",
  "hx-patch",
  "hx-delete",
];

const isBoostEligible = (link, event) => {
  if (!(link instanceof HTMLAnchorElement)) return false;
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return false;
  if (typeof event.button === "number" && event.button !== 0) return false;
  if (link.getAttribute("hx-boost") === "false") return false;
  if (HX_OWN_ATTRS.some((attr) => link.hasAttribute(attr))) return false;

  const linkTarget = link.getAttribute("target");
  if (linkTarget && linkTarget !== "_self") return false;
  if (link.hasAttribute("download")) return false;

  const href = link.getAttribute("href") || "";
  if (!href) return false;
  if (href.startsWith("#")) return false;

  try {
    const url = new URL(href, location.href);
    if (url.protocol !== "http:" && url.protocol !== "https:") return false;
    if (url.origin !== location.origin) return false;
  } catch (_) {
    return false;
  }

  return true;
};

const decorateForBoost = (link) => {
  link.setAttribute("hx-target", BOOST_TARGET);
  link.setAttribute("hx-select", BOOST_SELECT);
  link.setAttribute("hx-swap", BOOST_SWAP);
};

const handleClickCapture = (event) => {
  if (event.defaultPrevented) return;
  const target = event.target instanceof Element ? event.target : null;
  const link = target?.closest("a[href]");
  if (!link) return;
  if (!isBoostEligible(link, event)) return;
  decorateForBoost(link);
};

export const bindBoostScopeOnce = (root = document) => {
  const html = root.documentElement || document.documentElement;
  if (html.dataset.openSspmAppBoostBound === "true") return;
  html.dataset.openSspmAppBoostBound = "true";
  document.addEventListener("click", handleClickCapture, true);
};
