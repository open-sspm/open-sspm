import { beforeEach, describe, expect, it } from "vitest";

import "open-sspm-app/components/page_title.js";
import { start, stop } from "open-sspm-app/components/registry.js";

describe("page_title component", () => {
  beforeEach(() => {
    stop();
    document.body.innerHTML = "";
    document.title = "Posture · Open-SSPM";
  });

  it("syncs h1 text from document.title on htmx:afterSettle, stripping site suffix", () => {
    document.body.innerHTML = `<h1 id="page-title">Posture</h1>`;
    start();

    document.title = "Identities · Open-SSPM";
    document.dispatchEvent(new CustomEvent("htmx:afterSettle"));

    expect(document.getElementById("page-title").textContent).toBe("Identities");
  });

  it("syncs h1 text on popstate (history back/forward)", () => {
    document.body.innerHTML = `<h1 id="page-title">Identities</h1>`;
    start();

    document.title = "Non-Human Identities · Open-SSPM";
    window.dispatchEvent(new PopStateEvent("popstate"));

    expect(document.getElementById("page-title").textContent).toBe("Non-Human Identities");
  });

  it("leaves h1 untouched when document.title lacks the site suffix", () => {
    document.body.innerHTML = `<h1 id="page-title">Apps</h1>`;
    start();

    document.title = "Standalone Title";
    document.dispatchEvent(new CustomEvent("htmx:afterSettle"));

    expect(document.getElementById("page-title").textContent).toBe("Standalone Title");
  });
});
