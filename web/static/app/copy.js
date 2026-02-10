const emitCopyToast = (category, title, description = "") => {
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

const copyText = async (text) => {
  if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
    await navigator.clipboard.writeText(text);
    return;
  }

  throw new Error("Clipboard API unavailable");
};

const copyLabel = (element) => {
  const label = (element.dataset.copyLabel || "").trim();
  if (label === "") {
    return "Value";
  }
  return label.replace(/^Copy\s+/i, "");
};

export const wireCopyButtons = (root = document) => {
  root.querySelectorAll("[data-copy-text]").forEach((element) => {
    if (!(element instanceof HTMLButtonElement)) return;
    if (element.dataset.copyBound === "true") return;

    element.addEventListener("click", async (event) => {
      event.preventDefault();

      const text = (element.dataset.copyText || "").trim();
      if (!text) return;

      const label = copyLabel(element);
      try {
        await copyText(text);
        emitCopyToast("success", "Copied to clipboard", label);
      } catch (_) {
        emitCopyToast("error", "Copy failed", "Clipboard is unavailable in this browser.");
      }
    });

    element.dataset.copyBound = "true";
  });
};
