import { beforeEach, describe, expect, it, vi } from "vitest";

import { wireSidebarToggle } from "open-sspm-app/sidebar.js";

const waitForAsyncWork = async () => {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
};

describe("sidebar", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
  });

  it("syncs toggle aria state and label with sidebar visibility", async () => {
    document.body.innerHTML = `
      <aside id="app-sidebar" data-breakpoint="1024" aria-hidden="true">
        <nav><a href="#">Link</a></nav>
      </aside>
      <button id="sidebar-toggle" type="button"></button>
    `;

    const sidebar = document.getElementById("app-sidebar");
    const toggle = document.getElementById("sidebar-toggle");

    wireSidebarToggle(document);

    expect(toggle.getAttribute("aria-expanded")).toBe("false");
    expect(toggle.getAttribute("aria-label")).toBe("Open navigation");

    sidebar.setAttribute("aria-hidden", "false");
    await waitForAsyncWork();

    expect(toggle.getAttribute("aria-expanded")).toBe("true");
    expect(toggle.getAttribute("aria-label")).toBe("Close navigation");
  });

  it("dispatches a close action on Escape in mobile view", () => {
    Object.defineProperty(window, "innerWidth", { value: 500, configurable: true, writable: true });
    document.body.innerHTML = `
      <aside id="app-sidebar" data-breakpoint="1024" aria-hidden="false">
        <nav><a href="#">Link</a></nav>
      </aside>
      <button id="sidebar-toggle" type="button"></button>
    `;

    const listener = vi.fn();
    document.addEventListener("basecoat:sidebar", listener);

    wireSidebarToggle(document);

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));

    expect(listener).toHaveBeenCalledWith(
      expect.objectContaining({
        detail: expect.objectContaining({ id: "app-sidebar", action: "close" }),
      }),
    );

    document.removeEventListener("basecoat:sidebar", listener);
  });

  it("does not close sidebar on Escape when a dialog is open", () => {
    Object.defineProperty(window, "innerWidth", { value: 500, configurable: true, writable: true });
    document.body.innerHTML = `
      <aside id="app-sidebar" data-breakpoint="1024" aria-hidden="false">
        <nav><a href="#">Link</a></nav>
      </aside>
      <button id="sidebar-toggle" type="button"></button>
      <dialog open><button id="dialog-button" type="button">Inside</button></dialog>
    `;

    const listener = vi.fn();
    document.addEventListener("basecoat:sidebar", listener);

    wireSidebarToggle(document);

    const dialogButton = document.getElementById("dialog-button");
    dialogButton.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));

    expect(listener).not.toHaveBeenCalledWith(
      expect.objectContaining({
        detail: expect.objectContaining({ action: "close" }),
      }),
    );

    document.removeEventListener("basecoat:sidebar", listener);
  });
});
