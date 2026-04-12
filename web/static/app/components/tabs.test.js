import { beforeEach, describe, expect, it } from "vitest";

import "open-sspm-app/components/tabs.js";
import { start, stop } from "open-sspm-app/components/registry.js";

describe("tabs component", () => {
  beforeEach(() => {
    stop();
    document.body.innerHTML = "";
  });

  it("does not move focus during initialization", () => {
    document.body.innerHTML = `
      <button id="before" type="button">Before</button>
      <div class="tabs">
        <div role="tablist">
          <button id="tab-1" role="tab" aria-selected="true" aria-controls="panel-1">Tab 1</button>
          <button id="tab-2" role="tab" aria-selected="false" aria-controls="panel-2">Tab 2</button>
        </div>
        <section id="panel-1">Panel 1</section>
        <section id="panel-2">Panel 2</section>
      </div>
    `;

    const before = document.getElementById("before");
    before.focus();

    start();

    expect(document.activeElement).toBe(before);
    expect(document.getElementById("tab-1").getAttribute("aria-selected")).toBe("true");
    expect(document.getElementById("panel-1").hidden).toBe(false);
    expect(document.getElementById("panel-2").hidden).toBe(true);
  });

  it("moves focus when the user activates a tab with the keyboard", () => {
    document.body.innerHTML = `
      <div class="tabs">
        <div role="tablist">
          <button id="tab-1" role="tab" aria-selected="true" aria-controls="panel-1">Tab 1</button>
          <button id="tab-2" role="tab" aria-selected="false" aria-controls="panel-2">Tab 2</button>
        </div>
        <section id="panel-1">Panel 1</section>
        <section id="panel-2">Panel 2</section>
      </div>
    `;

    start();

    const tablist = document.querySelector('[role="tablist"]');
    const firstTab = document.getElementById("tab-1");
    const secondTab = document.getElementById("tab-2");
    firstTab.focus();

    tablist.dispatchEvent(
      new KeyboardEvent("keydown", { key: "ArrowRight", bubbles: true }),
    );

    expect(document.activeElement).toBe(secondTab);
    expect(secondTab.getAttribute("aria-selected")).toBe("true");
    expect(document.getElementById("panel-1").hidden).toBe(true);
    expect(document.getElementById("panel-2").hidden).toBe(false);
  });
});
