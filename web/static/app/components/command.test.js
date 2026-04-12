import { beforeEach, describe, expect, it } from "vitest";

import "open-sspm-app/components/command.js";
import { start, stop } from "open-sspm-app/components/registry.js";

describe("command component", () => {
  beforeEach(() => {
    stop();
    document.body.innerHTML = "";
    HTMLElement.prototype.scrollIntoView ??= () => {};
  });

  it("keeps forced items visible while filtering", () => {
    document.body.innerHTML = `
      <div class="command">
        <header><input type="text" /></header>
        <div role="menu">
          <button role="menuitem" data-filter="aws github" aria-hidden="false">
            Match
          </button>
          <button role="menuitem" data-filter="notice" data-force aria-hidden="false">
            Forced notice
          </button>
          <button role="menuitem" data-filter="okta" aria-hidden="false">
            Hidden item
          </button>
        </div>
      </div>
    `;

    start();

    const input = document.querySelector("header input");
    const [match, forced, hidden] = document.querySelectorAll('[role="menuitem"]');

    input.value = "git";
    input.dispatchEvent(new Event("input", { bubbles: true }));

    expect(match.getAttribute("aria-hidden")).toBe("false");
    expect(forced.getAttribute("aria-hidden")).toBe("false");
    expect(hidden.getAttribute("aria-hidden")).toBe("true");
  });
});
