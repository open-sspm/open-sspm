import { beforeEach, describe, expect, it, vi } from "vitest";

import { wireSidebarToggle } from "open-sspm-app/components/sidebar.js";
import { start, stop } from "open-sspm-app/components/registry.js";

const DESKTOP_SIDEBAR_STATE_KEY = "openSspm.sidebar.desktopOpen";

const waitForAsyncWork = async () => {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
  if (typeof window.requestAnimationFrame === "function") {
    await new Promise((resolve) => window.requestAnimationFrame(() => resolve()));
    return;
  }
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
    stop();
    document.body.innerHTML = "";
    document.documentElement.removeAttribute("data-mobile-sidebar-open");
    const localStorageMock = createLocalStorageMock();
    Object.defineProperty(window, "localStorage", { value: localStorageMock, configurable: true });
    Object.defineProperty(globalThis, "localStorage", { value: localStorageMock, configurable: true });
    localStorage.clear();
    vi.restoreAllMocks();
  });

  const renderIntegratedMobileSidebar = () => {
    Object.defineProperty(window, "innerWidth", { value: 500, configurable: true, writable: true });
    document.body.innerHTML = `
      <aside
        id="app-sidebar"
        class="sidebar"
        data-breakpoint="1024"
        data-initial-mobile-open="false"
      >
        <nav>
          <button type="button" data-sidebar-mobile-close>Close</button>
          <a id="mobile-destination" href="/identities">Identities</a>
        </nav>
      </aside>
      <div data-sidebar-content>
        <button id="sidebar-toggle" type="button">Navigation</button>
        <main id="main" tabindex="-1" data-main-content><a href="/background">Background</a></main>
      </div>
    `;

    document.getElementById("mobile-destination").addEventListener("click", (event) => {
      event.preventDefault();
    });
    start();
    wireSidebarToggle(document);

    const toggle = document.getElementById("sidebar-toggle");
    toggle.click();

    return {
      sidebar: document.getElementById("app-sidebar"),
      content: document.querySelector("[data-sidebar-content]"),
      close: document.querySelector("[data-sidebar-mobile-close]"),
      destination: document.getElementById("mobile-destination"),
      main: document.getElementById("main"),
      toggle,
    };
  };

  it("treats an open mobile sidebar as a modal surface", async () => {
    const { content, close } = renderIntegratedMobileSidebar();
    await waitForAsyncWork();

    expect(content.inert).toBe(true);
    expect(document.documentElement.getAttribute("data-mobile-sidebar-open")).toBe("true");
    expect(document.activeElement).toBe(close);
  });

  it("closes after mobile navigation and moves focus to main content", async () => {
    const { sidebar, content, destination, main, toggle } = renderIntegratedMobileSidebar();
    await waitForAsyncWork();

    destination.click();
    await waitForAsyncWork();

    expect(sidebar.getAttribute("aria-hidden")).toBe("true");
    expect(content.inert).toBe(false);
    expect(document.documentElement.hasAttribute("data-mobile-sidebar-open")).toBe(false);
    expect(document.activeElement).toBe(main);
    expect(document.activeElement).not.toBe(toggle);
  });

  it("closes on backdrop click and returns focus to the toggle", async () => {
    const { sidebar, content, toggle } = renderIntegratedMobileSidebar();
    await waitForAsyncWork();

    sidebar.click();
    await waitForAsyncWork();

    expect(sidebar.getAttribute("aria-hidden")).toBe("true");
    expect(content.inert).toBe(false);
    expect(document.activeElement).toBe(toggle);
  });

  it("closes from the mobile close control and returns focus to the toggle", async () => {
    const { sidebar, close, toggle } = renderIntegratedMobileSidebar();
    await waitForAsyncWork();

    close.click();
    await waitForAsyncWork();

    expect(sidebar.getAttribute("aria-hidden")).toBe("true");
    expect(document.activeElement).toBe(toggle);
  });

  it("closes on Escape and returns focus to the toggle", async () => {
    const { sidebar, toggle } = renderIntegratedMobileSidebar();
    await waitForAsyncWork();

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await waitForAsyncWork();

    expect(sidebar.getAttribute("aria-hidden")).toBe("true");
    expect(document.activeElement).toBe(toggle);
  });

  it("closes an open desktop sidebar when the viewport crosses into mobile", async () => {
    Object.defineProperty(window, "innerWidth", { value: 1280, configurable: true, writable: true });
    document.body.innerHTML = `
      <aside
        id="app-sidebar"
        class="sidebar"
        data-breakpoint="1024"
        data-initial-open="true"
        data-initial-mobile-open="false"
      >
        <nav><a href="#">Link</a></nav>
      </aside>
      <div data-sidebar-content>
        <button id="sidebar-toggle" type="button">Navigation</button>
        <main></main>
      </div>
    `;

    start();
    wireSidebarToggle(document);
    await waitForAsyncWork();

    const sidebar = document.getElementById("app-sidebar");
    const content = document.querySelector("[data-sidebar-content]");
    expect(sidebar.getAttribute("aria-hidden")).toBe("false");

    window.innerWidth = 900;
    window.dispatchEvent(new Event("resize"));
    await waitForAsyncWork();

    expect(sidebar.getAttribute("aria-hidden")).toBe("true");
    expect(content.inert).toBe(false);
    expect(document.documentElement.hasAttribute("data-mobile-sidebar-open")).toBe(false);
    expect(localStorage.getItem(DESKTOP_SIDEBAR_STATE_KEY)).toBe("true");
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
    document.addEventListener("osspm:sidebar", listener);

    wireSidebarToggle(document);

    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));

    expect(listener).toHaveBeenCalledWith(
      expect.objectContaining({
        detail: expect.objectContaining({ id: "app-sidebar", action: "close" }),
      }),
    );

    document.removeEventListener("osspm:sidebar", listener);
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
    document.addEventListener("osspm:sidebar", listener);

    wireSidebarToggle(document);

    const dialogButton = document.getElementById("dialog-button");
    dialogButton.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));

    expect(listener).not.toHaveBeenCalledWith(
      expect.objectContaining({
        detail: expect.objectContaining({ action: "close" }),
      }),
    );

    document.removeEventListener("osspm:sidebar", listener);
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
    document.addEventListener("osspm:sidebar", listener);

    wireSidebarToggle(document);
    await waitForAsyncWork();

    expect(listener).toHaveBeenCalledWith(
      expect.objectContaining({
        detail: expect.objectContaining({ id: "app-sidebar", action: "close" }),
      }),
    );

    document.removeEventListener("osspm:sidebar", listener);
  });
});
