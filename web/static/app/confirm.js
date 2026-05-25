// Styled replacement for the browser-native confirm/prompt that htmx triggers
// via hx-confirm. We hijack htmx:confirm, present our own <dialog>, and only
// then let htmx fire the original request.
//
// Two attribute pairs are supported on the triggering element:
//
//   hx-confirm="Are you sure?"
//     → renders the question as the dialog body; "Confirm" accepts.
//
//   data-osspm-confirm-title  ("Reject candidate?")
//   data-osspm-confirm-body   ("This will mark the candidate rejected.")
//   data-osspm-confirm-accept-label ("Reject")
//   data-osspm-confirm-tone   ("danger" → red button)
//   data-osspm-confirm-reason ("Optional reason")  (presence enables textarea)
//   data-osspm-confirm-reason-name ("reason")      (form field name; default: "reason")
//
// Any combination of these may be present. If at least one data-osspm-confirm-*
// attribute is set we treat it as a structured confirm. Otherwise we fall back
// to the bare hx-confirm question.

const SELECTORS = Object.freeze({
  dialog: "[data-osspm-confirm-dialog]",
  title: "[data-osspm-confirm-title]",
  body: "[data-osspm-confirm-body]",
  description: "[data-osspm-confirm-description]",
  accept: "[data-osspm-confirm-accept]",
  cancel: "[data-osspm-confirm-cancel]",
  reasonWrap: "[data-osspm-confirm-reason]",
  reasonLabel: "[data-osspm-confirm-reason-label]",
  reasonInput: "[data-osspm-confirm-reason-input]",
});

const findDialog = () => document.querySelector(SELECTORS.dialog);

const setText = (root, sel, text) => {
  const el = root.querySelector(sel);
  if (el) el.textContent = text;
};

const setupAcceptButton = (acceptBtn, label, tone) => {
  if (!acceptBtn) return;
  acceptBtn.textContent = label || "Confirm";
  acceptBtn.classList.remove("btn-primary", "btn-danger", "btn-warning");
  if (tone === "danger") acceptBtn.classList.add("btn-danger");
  else if (tone === "warning") acceptBtn.classList.add("btn-warning");
  else acceptBtn.classList.add("btn-primary");
};

const setupReason = (dialog, elt) => {
  const wrap = dialog.querySelector(SELECTORS.reasonWrap);
  const label = dialog.querySelector(SELECTORS.reasonLabel);
  const input = dialog.querySelector(SELECTORS.reasonInput);
  if (!wrap || !input) return null;

  const reasonLabel = elt?.dataset?.osspmConfirmReason || "";
  if (!reasonLabel) {
    wrap.hidden = true;
    input.value = "";
    return null;
  }

  wrap.hidden = false;
  if (label) label.textContent = reasonLabel;
  const name = elt?.dataset?.osspmConfirmReasonName || "reason";
  input.name = name;
  input.value = "";
  return { name, input };
};

const closeDialog = (dialog) => {
  if (!dialog) return;
  try {
    if (typeof dialog.close === "function") dialog.close();
    else dialog.removeAttribute("open");
  } catch (_) {
    dialog.removeAttribute("open");
  }
};

const openDialog = (dialog) => {
  if (!dialog) return;
  try {
    if (typeof dialog.showModal === "function") dialog.showModal();
    else dialog.setAttribute("open", "");
  } catch (_) {
    dialog.setAttribute("open", "");
  }
};

const dispatchConfirm = (event) => {
  const detail = event?.detail;
  if (!detail || typeof detail.issueRequest !== "function") return;
  if (!detail.question && !detail.elt?.dataset?.osspmConfirmBody) return;

  const dialog = findDialog();
  if (!dialog) return; // graceful fallback: htmx will use native confirm

  event.preventDefault();

  const elt = detail.elt;
  const ds = elt?.dataset || {};
  const title = ds.osspmConfirmTitle || "Are you sure?";
  const body = ds.osspmConfirmBody || detail.question || "";
  const acceptLabel = ds.osspmConfirmAcceptLabel || "Confirm";
  const tone = ds.osspmConfirmTone || "";

  setText(dialog, SELECTORS.title, title);
  setText(dialog, SELECTORS.body, body);
  setText(dialog, SELECTORS.description, body);
  setupAcceptButton(dialog.querySelector(SELECTORS.accept), acceptLabel, tone);
  const reason = setupReason(dialog, elt);

  let resolved = false;
  const accept = () => {
    if (resolved) return;
    resolved = true;
    if (reason?.input && reason.input.value !== "") {
      pendingReasonOverride = { name: reason.name, value: reason.input.value };
      detail.issueRequest(true);
    } else {
      detail.issueRequest(true);
    }
    closeDialog(dialog);
  };
  const cancel = () => {
    if (resolved) return;
    resolved = true;
    closeDialog(dialog);
  };

  const acceptBtn = dialog.querySelector(SELECTORS.accept);
  const cancelBtns = dialog.querySelectorAll(SELECTORS.cancel);
  const onAcceptClick = (e) => {
    e.preventDefault();
    accept();
  };
  const onCancelClick = (e) => {
    e.preventDefault();
    cancel();
  };
  const onClose = () => cancel();
  acceptBtn?.addEventListener("click", onAcceptClick, { once: true });
  cancelBtns.forEach((b) => b.addEventListener("click", onCancelClick, { once: true }));
  dialog.addEventListener("close", onClose, { once: true });

  openDialog(dialog);
  // Focus the reason input when shown, else the accept button.
  setTimeout(() => {
    if (reason?.input) reason.input.focus();
    else acceptBtn?.focus();
  }, 0);
};

// issueRequest accepts an optional parameters object (htmx 2.x): we extend the
// FormData with our reason field. If running an older htmx that doesn't accept
// the second arg, the form's reason input is still present as a normal field
// only when a form posts; for non-form confirms we add it via configRequest.
let pendingReasonOverride = null;
const stashReasonForNextRequest = (event) => {
  if (!pendingReasonOverride) return;
  const detail = event?.detail;
  if (!detail?.parameters) return;
  detail.parameters[pendingReasonOverride.name] = pendingReasonOverride.value;
  pendingReasonOverride = null;
};

export const bindConfirmListener = () => {
  if (document.documentElement.dataset.osspmConfirmBound === "true") return;
  document.documentElement.dataset.osspmConfirmBound = "true";
  document.body.addEventListener("htmx:confirm", dispatchConfirm);
  document.body.addEventListener("htmx:configRequest", stashReasonForNextRequest);
};
