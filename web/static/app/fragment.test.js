import { beforeEach, describe, expect, it, vi } from "vitest";

import { initFragment, triggerVisibleLazyHx, wireAutosubmit, wireRowLinks } from "open-sspm-app/fragment.js";

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

  it("adds keyboard semantics and supports modifier-click row navigation", () => {
    document.body.innerHTML = `
      <table>
        <tbody>
          <tr id="row" data-row-href="/credentials/42"><td>Credential 42</td></tr>
        </tbody>
      </table>
    `;

    const openSpy = vi.spyOn(window, "open").mockImplementation(() => null);

    wireRowLinks(document);

    const row = document.getElementById("row");
    row.dispatchEvent(new MouseEvent("click", { bubbles: true, button: 0, ctrlKey: true }));

    expect(row.getAttribute("role")).toBe("link");
    expect(row.getAttribute("tabindex")).toBe("0");
    expect(openSpy).toHaveBeenCalledWith("/credentials/42", "_blank", "noopener");
  });

  it("does not hijack clicks on interactive elements inside row links", () => {
    document.body.innerHTML = `
      <table>
        <tbody>
          <tr data-row-href="/credentials/42">
            <td><a id="inner" href="/credentials/42">Credential 42</a></td>
          </tr>
        </tbody>
      </table>
    `;

    const openSpy = vi.spyOn(window, "open").mockImplementation(() => null);

    wireRowLinks(document);

    const inner = document.getElementById("inner");
    inner.addEventListener("click", (event) => event.preventDefault());
    inner.dispatchEvent(new MouseEvent("click", { bubbles: true, cancelable: true, button: 0, ctrlKey: true }));

    expect(openSpy).not.toHaveBeenCalled();
  });

  it("opens a new tab for modifier-click row navigation", () => {
    document.body.innerHTML = `
      <table>
        <tbody>
          <tr id="row" data-row-href="/app-assets/9"><td>Asset 9</td></tr>
        </tbody>
      </table>
    `;

    const openSpy = vi.spyOn(window, "open").mockImplementation(() => null);

    wireRowLinks(document);

    const row = document.getElementById("row");
    row.dispatchEvent(new MouseEvent("click", { bubbles: true, button: 0, ctrlKey: true }));

    expect(openSpy).toHaveBeenCalledWith("/app-assets/9", "_blank", "noopener");
  });

  it("auto-opens dialogs swapped into a fragment root once", () => {
    const root = document.createElement("div");
    root.innerHTML = `
      <dialog id="connector-health-errors-modal" data-open>
        <button type="button" data-dialog-close>Close</button>
      </dialog>
    `;
    document.body.appendChild(root);

    const dialog = root.querySelector("dialog");
    const showModalSpy = vi.fn(function showModalStub() {
      this.setAttribute("open", "");
    });
    Object.defineProperty(dialog, "showModal", { value: showModalSpy, configurable: true });

    initFragment(root);
    initFragment(root);

    expect(showModalSpy).toHaveBeenCalledTimes(1);
    expect(dialog.hasAttribute("data-open")).toBe(false);
  });
});
