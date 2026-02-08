import { beforeEach, describe, expect, it, vi } from "vitest";

import { triggerVisibleLazyHx, wireAutosubmit } from "open-sspm-app/fragment.js";

describe("fragment", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
    delete window.htmx;
  });

  it("uses requestSubmit when available", () => {
    document.body.innerHTML = `
      <form id="f">
        <select id="s" data-autosubmit>
          <option>One</option>
        </select>
      </form>
    `;

    const form = document.getElementById("f");
    const requestSubmitSpy = vi.fn();
    Object.defineProperty(form, "requestSubmit", { value: requestSubmitSpy, configurable: true });

    wireAutosubmit(document);

    const select = document.getElementById("s");
    select.dispatchEvent(new Event("change", { bubbles: true }));

    expect(requestSubmitSpy).toHaveBeenCalledTimes(1);
  });

  it("falls back to submit when requestSubmit is unavailable", () => {
    document.body.innerHTML = `
      <form id="f">
        <input id="i" data-autosubmit />
      </form>
    `;

    const form = document.getElementById("f");
    Object.defineProperty(form, "requestSubmit", { value: undefined, configurable: true });

    const submitSpy = vi.fn();
    Object.defineProperty(form, "submit", { value: submitSpy, configurable: true });

    wireAutosubmit(document);

    const input = document.getElementById("i");
    input.dispatchEvent(new Event("change", { bubbles: true }));

    expect(submitSpy).toHaveBeenCalledTimes(1);
  });

  it("triggers lazy panels only when their panel is visible", () => {
    document.body.innerHTML = `
      <button id="a" data-hx-lazy-panel="panel-a"></button>
      <section id="panel-a"></section>
      <button id="b" data-hx-lazy-panel="panel-b"></button>
      <section id="panel-b" hidden></section>
    `;

    const triggerSpy = vi.fn();
    window.htmx = { trigger: triggerSpy };

    triggerVisibleLazyHx(document);

    expect(triggerSpy).toHaveBeenCalledTimes(1);
    expect(triggerSpy).toHaveBeenCalledWith(document.getElementById("a"), "oss-panel-visible");
    expect(document.getElementById("a").dataset.hxLazyLoaded).toBe("true");
    expect(document.getElementById("b").dataset.hxLazyLoaded).toBeUndefined();
  });
});
