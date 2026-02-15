const VALID_TOAST_CATEGORIES = new Set(["success", "error", "warning", "info"]);

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

const normalizeToastCategory = (category) => {
  const normalized = (category || "").trim().toLowerCase();
  if (VALID_TOAST_CATEGORIES.has(normalized)) return normalized;
  return "info";
};

const createToastIcon = (category) => {
  const template = document.createElement("template");
  template.innerHTML = TOAST_ICON_SVG[category] || TOAST_ICON_SVG.info;
  return template.content.firstElementChild;
};

const buildToastElement = ({ category, title, description }) => {
  const toast = document.createElement("div");
  toast.className = "toast";
  toast.setAttribute("role", category === "error" ? "alert" : "status");
  toast.setAttribute("aria-atomic", "true");
  toast.dataset.category = category;

  const content = document.createElement("div");
  content.className = "toast-content";

  const icon = createToastIcon(category);
  if (icon instanceof Element) {
    content.append(icon);
  }

  const section = document.createElement("section");
  if (title) {
    const heading = document.createElement("h2");
    heading.textContent = title;
    section.append(heading);
  }
  if (description) {
    const body = document.createElement("p");
    body.textContent = description;
    section.append(body);
  }
  content.append(section);
  toast.append(content);

  return toast;
};

const dispatchToastEvent = ({ category, title, description }) => {
  document.dispatchEvent(
    new CustomEvent("basecoat:toast", {
      detail: {
        config: {
          category,
          title,
          description,
        },
      },
    }),
  );
};

export const showFlashToast = () => {
  const flashToast = document.getElementById("flash-toast");
  if (!(flashToast instanceof HTMLElement)) return;
  if (flashToast.dataset.processed === "true") return;
  flashToast.dataset.processed = "true";

  const category = normalizeToastCategory(flashToast.dataset.category);
  const title = (flashToast.dataset.title || "").trim();
  const description = (flashToast.dataset.description || "").trim();
  if (!title && !description) {
    flashToast.remove();
    return;
  }

  const payload = { category, title, description };
  const toaster = document.getElementById("toaster");
  if (toaster instanceof HTMLElement) {
    toaster.append(buildToastElement(payload));
    flashToast.remove();
    return;
  }

  dispatchToastEvent(payload);
  flashToast.remove();
};
