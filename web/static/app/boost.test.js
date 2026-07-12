import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { bindBoostScopeOnce } from "open-sspm-app/boost.js";

const fireClick = (target, opts = {}) => {
  const event = new MouseEvent("click", {
    bubbles: true,
    cancelable: true,
    button: 0,
    ...opts,
  });
  target.dispatchEvent(event);
  return event;
};

describe("boost link scope", () => {
  beforeEach(() => {
    document.documentElement.dataset.openSspmAppBoostBound = "";
    document.body.innerHTML = "";
    bindBoostScopeOnce();
  });

  afterEach(() => {
    delete document.documentElement.dataset.openSspmAppBoostBound;
  });

  it("decorates a plain same-origin link with #main boost attributes", () => {
    document.body.innerHTML = `<a id="link" href="/identities">Identities</a>`;
    const link = document.getElementById("link");

    fireClick(link);

    expect(link.getAttribute("hx-target")).toBe("#main");
    expect(link.getAttribute("hx-select")).toBe("#main");
    expect(link.getAttribute("hx-swap")).toBe("outerHTML show:window:top transition:true");
  });

  it("does not decorate links that already have their own hx-target", () => {
    document.body.innerHTML = `<a id="link" href="/x" hx-get="/x" hx-target="#panel">x</a>`;
    const link = document.getElementById("link");

    fireClick(link);

    expect(link.getAttribute("hx-target")).toBe("#panel");
    expect(link.getAttribute("hx-select")).toBe(null);
    expect(link.getAttribute("hx-swap")).toBe(null);
  });

  it("does not decorate links that already have their own hx-select", () => {
    document.body.innerHTML = `<a id="link" href="/x" hx-select="#panel">x</a>`;
    const link = document.getElementById("link");

    fireClick(link);

    expect(link.getAttribute("hx-select")).toBe("#panel");
    expect(link.getAttribute("hx-target")).toBe(null);
    expect(link.getAttribute("hx-swap")).toBe(null);
  });

  it("does not decorate hash links, non-http links, or external links", () => {
    document.body.innerHTML = `
      <a id="hash" href="#section">hash</a>
      <a id="mail" href="mailto:a@b.c">mail</a>
      <a id="data" href="data:text/html,hello">data</a>
      <a id="vbscript" href="vbscript:msgbox(1)">vbscript</a>
      <a id="ext" href="https://other.example.com/foo">ext</a>
    `;

    fireClick(document.getElementById("hash"));
    fireClick(document.getElementById("mail"));
    fireClick(document.getElementById("data"));
    fireClick(document.getElementById("vbscript"));
    fireClick(document.getElementById("ext"));

    for (const id of ["hash", "mail", "data", "vbscript", "ext"]) {
      const link = document.getElementById(id);
      expect(link.getAttribute("hx-target")).toBe(null);
      expect(link.getAttribute("hx-select")).toBe(null);
    }
  });

  it("does not decorate when the user holds a modifier key (open-in-new-tab semantics)", () => {
    document.body.innerHTML = `<a id="link" href="/foo">foo</a>`;
    const link = document.getElementById("link");

    fireClick(link, { metaKey: true });

    expect(link.getAttribute("hx-target")).toBe(null);
  });

  it("does not decorate links opted out via hx-boost=false", () => {
    document.body.innerHTML = `<a id="link" href="/foo" hx-boost="false">foo</a>`;
    const link = document.getElementById("link");

    fireClick(link);

    expect(link.getAttribute("hx-target")).toBe(null);
  });

  it("does not decorate links with target=_blank or download", () => {
    document.body.innerHTML = `
      <a id="blank" href="/foo" target="_blank">blank</a>
      <a id="dl" href="/foo" download>dl</a>
    `;

    fireClick(document.getElementById("blank"));
    fireClick(document.getElementById("dl"));

    expect(document.getElementById("blank").getAttribute("hx-target")).toBe(null);
    expect(document.getElementById("dl").getAttribute("hx-target")).toBe(null);
  });

  it("decorates links nested inside other elements via event delegation", () => {
    document.body.innerHTML = `<div><span><a id="link" href="/nested">nested</a></span></div>`;
    const inner = document.querySelector("span");

    // Click bubbles up through inner, but our capture handler walks via closest()
    fireClick(inner);
    // No link clicked directly — verify nothing happens
    expect(document.getElementById("link").getAttribute("hx-target")).toBe(null);

    // Click the link itself, even if event.target is a descendant
    fireClick(document.getElementById("link"));
    expect(document.getElementById("link").getAttribute("hx-target")).toBe("#main");
  });
});
