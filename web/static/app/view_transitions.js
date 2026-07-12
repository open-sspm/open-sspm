const REDUCED_MOTION_QUERY = "(prefers-reduced-motion: reduce)";
const APP_MAIN_SELECTOR = "main#main";

const prefersReducedMotion = () => window.matchMedia?.(REDUCED_MOTION_QUERY)?.matches === true;

const isAppNavigation = (detail) => {
  const target = detail?.target;
  return detail?.boosted === true && target instanceof HTMLElement && target.matches(APP_MAIN_SELECTOR);
};

const shouldAllowTransition = (detail) => isAppNavigation(detail) && !prefersReducedMotion();

// Keep transitions at the app-navigation boundary even if a future request
// opts in globally or declares transition:true on a fragment update.
const handleBeforeTransition = (event) => {
  if (!shouldAllowTransition(event.detail)) event.preventDefault();
};

export const bindViewTransitionPolicyOnce = (root = document) => {
  const html = root.documentElement;
  if (!html || html.dataset.openSspmViewTransitionPolicyBound === "true") return;

  html.dataset.openSspmViewTransitionPolicyBound = "true";
  root.addEventListener("htmx:beforeTransition", handleBeforeTransition);
};
