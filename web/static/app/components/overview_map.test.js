import { beforeEach, describe, expect, it } from "vitest";

import "open-sspm-app/components/overview_map.js";
import { start, stop } from "open-sspm-app/components/registry.js";

const renderOverviewMap = () => {
  document.body.innerHTML = `
    <div data-overview-map-surface>
      <svg>
        <path data-overview-map-edge="0"></path>
        <path data-overview-map-edge="1"></path>
      </svg>
      <a href="/identities?source_kind=okta" data-overview-map-node="0" style="--overview-map-accent: #155e75">
        Okta
      </a>
      <a href="/identities?source_kind=github" data-overview-map-node="1" style="--overview-map-accent: #111827">
        GitHub
      </a>
    </div>
  `;

  return document.querySelector("[data-overview-map-surface]");
};

describe("overview map component", () => {
  beforeEach(() => {
    stop();
    document.body.innerHTML = "";
  });

  it("activates the matching node and edge on focus", () => {
    const surface = renderOverviewMap();
    start();

    const node = surface.querySelector('[data-overview-map-node="1"]');
    node.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));

    expect(surface.dataset.overviewMapActive).toBe("1");
    expect(node.hasAttribute("data-overview-map-active")).toBe(true);
    expect(surface.querySelector('[data-overview-map-edge="1"]').hasAttribute("data-overview-map-active")).toBe(true);
    expect(surface.querySelector('[data-overview-map-edge="0"]').hasAttribute("data-overview-map-active")).toBe(false);
  });

  it("clears active state when focus leaves a map node", () => {
    const surface = renderOverviewMap();
    start();

    const node = surface.querySelector('[data-overview-map-node="0"]');
    node.dispatchEvent(new FocusEvent("focusin", { bubbles: true }));
    node.dispatchEvent(new FocusEvent("focusout", { bubbles: true, relatedTarget: document.body }));

    expect(surface.dataset.overviewMapActive).toBeUndefined();
    expect(node.hasAttribute("data-overview-map-active")).toBe(false);
    expect(surface.querySelector('[data-overview-map-edge="0"]').hasAttribute("data-overview-map-active")).toBe(false);
  });
});
