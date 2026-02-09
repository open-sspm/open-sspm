import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  closeDialog,
  openDialog,
  openServerDialogs,
  wireDialogCloseButtons,
  wireDialogCloseNavigation,
} from "open-sspm-app/dialogs.js";

const waitForAsyncWork = async () => {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 25));
};

describe("dialogs", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
  });

  it("falls back to the open attribute when showModal throws", () => {
    const dialog = document.createElement("dialog");
    const showModal = vi.fn(() => {
      throw new Error("boom");
    });
    Object.defineProperty(dialog, "showModal", { value: showModal, configurable: true });

    document.body.appendChild(dialog);

    openDialog(dialog);

    expect(showModal).toHaveBeenCalledTimes(1);
    expect(dialog.hasAttribute("open")).toBe(true);
  });

  it("restores focus after close when dialog does not use close navigation", async () => {
    const trigger = document.createElement("button");
    trigger.type = "button";
    document.body.appendChild(trigger);

    const dialog = document.createElement("dialog");
    Object.defineProperty(dialog, "close", {
      value: function closeStub() {
        this.removeAttribute("open");
      },
      configurable: true,
    });
    document.body.appendChild(dialog);

    trigger.focus();
    openDialog(dialog);

    const focusSpy = vi.spyOn(trigger, "focus");
    closeDialog(dialog);
    await waitForAsyncWork();

    expect(focusSpy).toHaveBeenCalled();
  });

  it("does not restore focus for dialogs with data-close-href", async () => {
    const trigger = document.createElement("button");
    trigger.type = "button";
    document.body.appendChild(trigger);

    const dialog = document.createElement("dialog");
    dialog.setAttribute("data-close-href", "/settings/connectors");
    Object.defineProperty(dialog, "close", {
      value: function closeStub() {
        this.removeAttribute("open");
      },
      configurable: true,
    });
    document.body.appendChild(dialog);

    trigger.focus();
    openDialog(dialog);

    const focusSpy = vi.spyOn(trigger, "focus");
    closeDialog(dialog);
    await waitForAsyncWork();

    expect(focusSpy).not.toHaveBeenCalled();
  });

  it("binds dialog close buttons only once", () => {
    const root = document.createElement("div");
    root.innerHTML = `
      <dialog open>
        <button type="button" data-dialog-close>Close</button>
      </dialog>
    `;
    document.body.appendChild(root);

    const dialog = root.querySelector("dialog");
    const closeSpy = vi.fn();
    Object.defineProperty(dialog, "close", { value: closeSpy, configurable: true });

    wireDialogCloseButtons(root);
    wireDialogCloseButtons(root);

    const closeButton = root.querySelector("[data-dialog-close]");
    closeButton.click();

    expect(closeSpy).toHaveBeenCalledTimes(1);
  });

  it("opens server dialogs in provided root and consumes data-open", () => {
    const root = document.createElement("div");
    root.innerHTML = `<dialog id="inside" data-open></dialog>`;
    document.body.appendChild(root);

    const outside = document.createElement("dialog");
    outside.id = "outside";
    outside.setAttribute("data-open", "");
    document.body.appendChild(outside);

    const insideDialog = root.querySelector("#inside");
    const insideShowModal = vi.fn(function insideShowModalStub() {
      this.setAttribute("open", "");
    });
    Object.defineProperty(insideDialog, "showModal", {
      value: insideShowModal,
      configurable: true,
    });

    const outsideShowModal = vi.fn(function outsideShowModalStub() {
      this.setAttribute("open", "");
    });
    Object.defineProperty(outside, "showModal", {
      value: outsideShowModal,
      configurable: true,
    });

    openServerDialogs(root);
    openServerDialogs(root);

    expect(insideShowModal).toHaveBeenCalledTimes(1);
    expect(outsideShowModal).toHaveBeenCalledTimes(0);
    expect(insideDialog.hasAttribute("data-open")).toBe(false);
  });

  it("binds close-navigation handlers only within the provided root", () => {
    const root = document.createElement("div");
    root.innerHTML = `<dialog id="inside" data-close-href="/settings/connector-health"></dialog>`;
    document.body.appendChild(root);

    const outside = document.createElement("dialog");
    outside.id = "outside";
    outside.setAttribute("data-close-href", "/settings/connectors");
    document.body.appendChild(outside);

    const insideDialog = root.querySelector("#inside");
    wireDialogCloseNavigation(root);

    expect(insideDialog.dataset.closeNavBound).toBe("true");
    expect(outside.dataset.closeNavBound).toBeUndefined();
  });
});
