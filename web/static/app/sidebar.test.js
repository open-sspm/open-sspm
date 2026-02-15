import { beforeEach, describe, expect, it, vi } from "vitest";

import { wireSidebarToggle } from "open-sspm-app/sidebar.js";

const DESKTOP_SIDEBAR_STATE_KEY = "openSspm.sidebar.desktopOpen";

const waitForAsyncWork = async () => {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
};

const createLocalStorageMock = () => {
  const values = new Map();
  return {
    getItem(key) {
      const normalized = String(key);
      return values.has(normalized) ? values.get(normalized) : null;
    },
    setItem(key, value) {
      values.set(String(key), String(value));
    },
    removeItem(key) {
      values.delete(String(key));
    },
    clear() {
      values.clear();
    },
  };
};

describe("sidebar", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    const localStorageMock = createLocalStorageMock();
    Object.defineProperty(window, "localStorage", { value: localStorageMock, configurable: true });
    Object.defineProperty(globalThis, "localStorage", { value: localStorageMock, configurable: true });
    localStorage.clear();
    vi.restoreAllMocks();
  });

  it("syncs toggle aria state and label with sidebar visibility", async () => {
    document.body.innerHTML = `
      <aside id="app-sidebar" data-breakpoint="1024" data-sidebar-initialized="true" aria-hidden="true">
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
      <aside id="app-sidebar" data-breakpoint="1024" data-sidebar-initialized="true" aria-hidden="false">
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
      <aside id="app-sidebar" data-breakpoint="1024" data-sidebar-initialized="true" aria-hidden="false">
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

  it("persists desktop sidebar state when visibility changes", async () => {
    Object.defineProperty(window, "innerWidth", { value: 1280, configurable: true, writable: true });
    document.body.innerHTML = `
      <aside id="app-sidebar" data-breakpoint="1024" data-sidebar-initialized="true" aria-hidden="false">
        <nav><a href="#">Link</a></nav>
      </aside>
      <button id="sidebar-toggle" type="button"></button>
    `;

    const sidebar = document.getElementById("app-sidebar");

    wireSidebarToggle(document);
    await waitForAsyncWork();

    expect(localStorage.getItem(DESKTOP_SIDEBAR_STATE_KEY)).toBe("true");

    sidebar.setAttribute("aria-hidden", "true");
    await waitForAsyncWork();
    expect(localStorage.getItem(DESKTOP_SIDEBAR_STATE_KEY)).toBe("false");

    sidebar.setAttribute("aria-hidden", "false");
    await waitForAsyncWork();
    expect(localStorage.getItem(DESKTOP_SIDEBAR_STATE_KEY)).toBe("true");
  });

  it("does not overwrite desktop preference from mobile interactions", async () => {
    Object.defineProperty(window, "innerWidth", { value: 500, configurable: true, writable: true });
    localStorage.setItem(DESKTOP_SIDEBAR_STATE_KEY, "true");
    document.body.innerHTML = `
      <aside id="app-sidebar" data-breakpoint="1024" data-sidebar-initialized="true" aria-hidden="false">
        <nav><a href="#">Link</a></nav>
      </aside>
      <button id="sidebar-toggle" type="button"></button>
    `;

    const sidebar = document.getElementById("app-sidebar");

    wireSidebarToggle(document);
    await waitForAsyncWork();

    sidebar.setAttribute("aria-hidden", "true");
    await waitForAsyncWork();

    expect(localStorage.getItem(DESKTOP_SIDEBAR_STATE_KEY)).toBe("true");
  });

  it("dispatches explicit close action on init when desktop preference differs", async () => {
    Object.defineProperty(window, "innerWidth", { value: 1280, configurable: true, writable: true });
    localStorage.setItem(DESKTOP_SIDEBAR_STATE_KEY, "false");
    document.body.innerHTML = `
      <aside id="app-sidebar" data-breakpoint="1024" data-sidebar-initialized="true" aria-hidden="false">
        <nav><a href="#">Link</a></nav>
      </aside>
      <button id="sidebar-toggle" type="button"></button>
    `;

    const listener = vi.fn();
    document.addEventListener("basecoat:sidebar", listener);

    wireSidebarToggle(document);
    await waitForAsyncWork();

    expect(listener).toHaveBeenCalledWith(
      expect.objectContaining({
        detail: expect.objectContaining({ id: "app-sidebar", action: "close" }),
      }),
    );

    document.removeEventListener("basecoat:sidebar", listener);
  });
});
