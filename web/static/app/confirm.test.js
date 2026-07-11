import { beforeEach, describe, expect, it, vi } from "vitest";

import { bindConfirmListener } from "open-sspm-app/confirm.js";

const renderConfirmShell = () => {
  document.body.innerHTML = `
    <dialog data-osspm-confirm-dialog>
      <h2 data-osspm-confirm-title></h2>
      <p data-osspm-confirm-body></p>
      <p data-osspm-confirm-description></p>
      <label data-osspm-confirm-reason hidden>
        <span data-osspm-confirm-reason-label></span>
        <textarea data-osspm-confirm-reason-input></textarea>
      </label>
      <button type="button" data-osspm-confirm-cancel>Cancel</button>
      <button type="button" class="btn-primary" data-osspm-confirm-accept>Confirm</button>
    </dialog>
    <button
      id="reject"
      hx-confirm="Reject candidate?"
      data-osspm-confirm-title="Reject candidate?"
      data-osspm-confirm-body="This will mark the candidate rejected."
      data-osspm-confirm-reason="Review note"
      data-osspm-confirm-reason-name="review_note"
    >Reject</button>
  `;
  const dialog = document.querySelector("dialog");
  Object.defineProperty(dialog, "showModal", {
    value: function showModalStub() {
      this.setAttribute("open", "");
    },
    configurable: true,
  });
  Object.defineProperty(dialog, "close", {
    value: function closeStub() {
      this.removeAttribute("open");
      this.dispatchEvent(new Event("close"));
    },
    configurable: true,
  });
};

describe("confirm dialog", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    delete document.documentElement.dataset.osspmConfirmBound;
    vi.restoreAllMocks();
  });

  it("preserves the configured review note in the next HTMX request", () => {
    renderConfirmShell();
    bindConfirmListener();

    const issuedDetail = { parameters: {} };
    const issueRequest = vi.fn(() => {
      document.body.dispatchEvent(new CustomEvent("htmx:configRequest", { bubbles: true, detail: issuedDetail }));
    });
    const trigger = document.getElementById("reject");
    const confirmEvent = new CustomEvent("htmx:confirm", {
      bubbles: true,
      cancelable: true,
      detail: {
        elt: trigger,
        question: "Reject candidate?",
        issueRequest,
      },
    });

    document.body.dispatchEvent(confirmEvent);

    const reasonInput = document.querySelector("[data-osspm-confirm-reason-input]");
    reasonInput.value = "Looks unrelated.";
    document.querySelector("[data-osspm-confirm-accept]").click();

    expect(confirmEvent.defaultPrevented).toBe(true);
    expect(issueRequest).toHaveBeenCalledWith(true);
    expect(issuedDetail.parameters).toEqual({ review_note: "Looks unrelated." });
  });

  it.each([
    ["danger", "btn-danger"],
    ["warning", "btn-warning"],
    ["", "btn-primary"],
  ])("applies the %s confirmation tone", (tone, expectedClass) => {
    renderConfirmShell();
    const trigger = document.getElementById("reject");
    if (tone) trigger.dataset.osspmConfirmTone = tone;
    bindConfirmListener();

    const confirmEvent = new CustomEvent("htmx:confirm", {
      bubbles: true,
      cancelable: true,
      detail: {
        elt: trigger,
        question: trigger.getAttribute("hx-confirm"),
        issueRequest: vi.fn(),
      },
    });

    document.body.dispatchEvent(confirmEvent);

    const accept = document.querySelector("[data-osspm-confirm-accept]");
    expect(accept.classList.contains(expectedClass)).toBe(true);
    expect(
      ["btn-primary", "btn-danger", "btn-warning"].filter((className) =>
        accept.classList.contains(className),
      ),
    ).toEqual([expectedClass]);
  });
});
