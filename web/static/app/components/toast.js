/**
 * Toaster component
 *
 * Auto-dismiss toasts with pause-on-hover. Listens for osspm:toast events
 * and handles DOM-appended toast elements.
 */
import { register } from "./registry.js";

const DURATIONS = {
  success: 3000,
  error: 5000,
  warning: 5000,
  info: 3000,
};

const TOAST_ICON_SVG = {
  success:
    '<svg aria-hidden="true" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><path d="m9 12 2 2 4-4"></path></svg>',
  error:
    '<svg aria-hidden="true" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><path d="m15 9-6 6"></path><path d="m9 9 6 6"></path></svg>',
  warning:
    '<svg aria-hidden="true" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m21.73 18-8-14a2 2 0 0 0-3.48 0l-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.73-3"></path><path d="M12 9v4"></path><path d="M12 17h.01"></path></svg>',
  info:
    '<svg aria-hidden="true" xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><path d="M12 16v-4"></path><path d="M12 8h.01"></path></svg>',
};

const buildToast = ({ category, title, description }) => {
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.setAttribute("role", category === "error" ? "alert" : "status");
  toast.setAttribute("aria-atomic", "true");
  toast.dataset.category = category;

  const content = document.createElement("div");
  content.className = "toast-content";

  const svg = TOAST_ICON_SVG[category] || TOAST_ICON_SVG.info;
  const tpl = document.createElement("template");
  tpl.innerHTML = svg;
  const icon = tpl.content.firstElementChild;
  if (icon) content.append(icon);

  const section = document.createElement("section");
  if (title) {
    const h = document.createElement("h2");
    h.textContent = title;
    section.append(h);
  }
  if (description) {
    const p = document.createElement("p");
    p.textContent = description;
    section.append(p);
  }
  content.append(section);
  toast.append(content);

  return toast;
};

const manageToast = (toast) => {
  const category = toast.dataset.category || "info";
  const duration = DURATIONS[category] || DURATIONS.info;

  let timer = null;
  let remaining = duration;
  let startTime = Date.now();

  const dismiss = () => {
    toast.setAttribute("aria-hidden", "true");
    setTimeout(() => toast.remove(), 350);
  };

  const startTimer = () => {
    startTime = Date.now();
    timer = setTimeout(dismiss, remaining);
  };

  const pauseTimer = () => {
    if (timer) {
      clearTimeout(timer);
      remaining -= Date.now() - startTime;
      if (remaining < 0) remaining = 0;
    }
  };

  toast.addEventListener("mouseenter", pauseTimer);
  toast.addEventListener("mouseleave", startTimer);

  startTimer();
};

const init = (el) => {
  const doc = el.ownerDocument;

  // Manage any existing toasts
  el.querySelectorAll(".toast:not([aria-hidden])").forEach(manageToast);

  // Watch for new toast elements added to the toaster
  const observer = new MutationObserver((mutations) => {
    for (const m of mutations) {
      for (const node of m.addedNodes) {
        if (node instanceof HTMLElement && node.classList.contains("toast")) {
          manageToast(node);
        }
      }
    }
  });
  observer.observe(el, { childList: true });

  // Listen for programmatic toast events
  const onToast = (event) => {
    const config = event.detail?.config;
    if (!config) return;
    const toast = buildToast(config);
    el.append(toast);
  };
  doc.addEventListener("osspm:toast", onToast);

  return () => {
    observer.disconnect();
    doc.removeEventListener("osspm:toast", onToast);
  };
};

register("toaster", ".toaster:not([data-toaster-initialized])", init);
