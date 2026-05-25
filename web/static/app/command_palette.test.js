import { beforeEach, describe, expect, it } from "vitest";

import { wireCommandPalette } from "open-sspm-app/command_palette.js";

describe("command palette", () => {
  beforeEach(() => {
    document.documentElement.dataset.commandPaletteListenersBound = "";
    document.body.innerHTML = "";
  });

  it("closes explicitly on Escape", () => {
    document.body.innerHTML = `
      <button data-command-palette-trigger>Search</button>
      <dialog id="command-palette" open>
        <div>
          <div class="command">
            <header><input id="command-search-input" /></header>
            <div role="menu">
              <a role="menuitem" href="/settings">Settings</a>
            </div>
          </div>
        </div>
      </dialog>
    `;

    const dialog = document.getElementById("command-palette");
    dialog.close = () => {
      dialog.removeAttribute("open");
      dialog.dispatchEvent(new Event("close"));
    };

    wireCommandPalette();

    const event = new KeyboardEvent("keydown", {
      key: "Escape",
      bubbles: true,
      cancelable: true,
    });
    dialog.dispatchEvent(event);

    expect(event.defaultPrevented).toBe(true);
    expect(dialog.hasAttribute("open")).toBe(false);
  });
});
