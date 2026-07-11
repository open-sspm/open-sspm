import { beforeEach, describe, expect, it, vi } from "vitest";

import {
  initFragment,
  markLazyHxLoaded,
  markLazyHxPending,
  triggerVisibleLazyHx,
  wireAutosubmit,
  wireDiscoveryGovernanceDisposition,
  wireRowLinks,
} from "open-sspm-app/fragment.js";

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

  it("shows replacement fields immediately when discovery disposition changes to replace", () => {
    document.body.innerHTML = `
      <form>
        <select id="disposition" data-discovery-review-disposition>
          <option value="under_review" selected>Under review</option>
          <option value="replace">Replace</option>
        </select>
        <div id="replacement-fields" data-discovery-replacement-fields hidden>
          <input type="search" name="replacement_query" />
        </div>
      </form>
    `;

    wireDiscoveryGovernanceDisposition(document);

    const disposition = document.getElementById("disposition");
    const replacementFields = document.getElementById("replacement-fields");

    expect(replacementFields.hidden).toBe(true);

    disposition.value = "replace";
    disposition.dispatchEvent(new Event("change", { bubbles: true }));

    expect(replacementFields.hidden).toBe(false);
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

  it("uses the primary row anchor for normal row navigation", () => {
    document.body.innerHTML = `
      <table>
        <tbody>
          <tr id="row" data-row-href="/credentials/42">
            <td><a id="primary" href="/credentials/42">Credential 42</a></td>
            <td><a id="secondary" href="/credentials/42/export">Export</a></td>
          </tr>
        </tbody>
      </table>
    `;

    const row = document.getElementById("row");
    const primary = document.getElementById("primary");
    const clickSpy = vi.fn();
    Object.defineProperty(primary, "click", { value: clickSpy, configurable: true });

    wireRowLinks(document);

    row.dispatchEvent(new MouseEvent("click", { bubbles: true, button: 0 }));

    expect(clickSpy).toHaveBeenCalledTimes(1);
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

  it("triggers visible lazy HTMX panels and skips pending or loaded panels", () => {
    document.body.innerHTML = `
      <section id="visible" data-hx-lazy-load data-hx-lazy-panel="visible"></section>
      <section id="pending" data-hx-lazy-load data-hx-lazy-panel="pending"></section>
      <section id="loaded" data-hx-lazy-load data-hx-lazy-panel="loaded"></section>
      <section id="failed" data-hx-lazy-load data-hx-lazy-panel="failed" data-hx-lazy-error></section>
    `;
    const visible = document.getElementById("visible");
    const pending = document.getElementById("pending");
    const loaded = document.getElementById("loaded");
    const trigger = vi.fn();
    window.htmx = { trigger };

    markLazyHxPending(pending);
    markLazyHxLoaded(loaded);
    triggerVisibleLazyHx(document);

    expect(trigger).toHaveBeenCalledTimes(1);
    expect(trigger).toHaveBeenCalledWith(visible, "oss-panel-visible");
  });

  it("does not trigger open-only lazy panels inside closed details", () => {
    document.body.innerHTML = `
      <details>
        <summary>More</summary>
        <section id="lazy" data-hx-lazy-load data-hx-lazy-open-only="true" data-hx-lazy-panel="lazy"></section>
      </details>
    `;
    const trigger = vi.fn();
    window.htmx = { trigger };

    triggerVisibleLazyHx(document);

    expect(trigger).not.toHaveBeenCalled();
  });
});
