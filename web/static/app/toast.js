export const showFlashToast = () => {
  const toastEl = document.getElementById("flash-toast");
  if (!(toastEl instanceof HTMLElement)) return;

  const category = (toastEl.dataset.category || "info").trim() || "info";
  const title = (toastEl.dataset.title || "").trim();
  const description = (toastEl.dataset.description || "").trim();
  if (!title && !description) {
    toastEl.remove();
    return;
  }

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

  toastEl.remove();
};
