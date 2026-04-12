import { beforeEach, describe, expect, it } from "vitest";

import "open-sspm-app/components/sidebar.js";
import { start, stop } from "open-sspm-app/components/registry.js";

describe("sidebar component", () => {
  beforeEach(() => {
    stop();
    document.body.innerHTML = "";
  });

  it("makes closed desktop navigation inert", () => {
    Object.defineProperty(window, "innerWidth", {
      value: 1280,
      configurable: true,
      writable: true,
    });
    document.body.innerHTML = `
      <aside
        id="app-sidebar"
        class="sidebar"
        data-breakpoint="1024"
        data-initial-open="false"
      >
        <nav><a href="#">Link</a></nav>
      </aside>
    `;

    start();

    const sidebar = document.getElementById("app-sidebar");
    const nav = sidebar.querySelector("nav");

    expect(sidebar.getAttribute("aria-hidden")).toBe("true");
    expect(nav.inert).toBe(true);
  });

  it("keeps open desktop navigation interactive and restores inert when closed", () => {
    Object.defineProperty(window, "innerWidth", {
      value: 1280,
      configurable: true,
      writable: true,
    });
    document.body.innerHTML = `
      <aside
        id="app-sidebar"
        class="sidebar"
        data-breakpoint="1024"
        data-initial-open="true"
      >
        <nav><a href="#">Link</a></nav>
      </aside>
    `;

    start();

    const sidebar = document.getElementById("app-sidebar");
    const nav = sidebar.querySelector("nav");

    expect(sidebar.getAttribute("aria-hidden")).toBe("false");
    expect(nav.inert).toBe(false);

    document.dispatchEvent(
      new CustomEvent("osspm:sidebar", {
        detail: { id: "app-sidebar", action: "close" },
      }),
    );

    expect(sidebar.getAttribute("aria-hidden")).toBe("true");
    expect(nav.inert).toBe(true);
  });
});
