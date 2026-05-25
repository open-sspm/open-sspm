import { beforeEach, describe, expect, it } from "vitest";

import "open-sspm-app/components/sidebar_nav.js";
import { start, stop } from "open-sspm-app/components/registry.js";

const renderNav = (initialOpenSections = []) => {
  const isOpen = (id) => (initialOpenSections.includes(id) ? "open" : "");
  document.body.innerHTML = `
    <div role="group" data-sidebar-nav>
      <ul>
        <li><a href="/" data-active-match="exact">Posture</a></li>
        <li>
          <details ${isOpen("apps")} data-active-prefixes="/discovery /assigned-apps">
            <summary>Apps &amp; Discovery</summary>
            <ul>
              <li><a href="/discovery/apps">Apps</a></li>
              <li><a href="/assigned-apps">Okta Assigned Apps</a></li>
            </ul>
          </details>
        </li>
        <li><a href="/identities">Identities</a></li>
        <li>
          <details ${isOpen("findings")} data-active-prefixes="/findings">
            <summary>Findings</summary>
            <ul>
              <li><a href="/findings" data-active-match="exact">Overview</a></li>
              <li><a href="/findings/leaked-creds">Leaked Creds</a></li>
            </ul>
          </details>
        </li>
      </ul>
    </div>
  `;
};

const navigate = (path) => {
  history.pushState({}, "", path);
  document.dispatchEvent(new CustomEvent("htmx:afterSettle"));
};

describe("sidebar_nav component", () => {
  beforeEach(() => {
    stop();
    history.replaceState({}, "", "/");
    document.body.innerHTML = "";
  });

  it("marks the exact home link active on /", () => {
    history.replaceState({}, "", "/");
    renderNav();
    start();

    const home = document.querySelector('a[href="/"]');
    const identities = document.querySelector('a[href="/identities"]');
    expect(home.getAttribute("aria-current")).toBe("page");
    expect(identities.getAttribute("aria-current")).toBe(null);
  });

  it("marks prefix-matched links active and opens their containing details", () => {
    history.replaceState({}, "", "/discovery/apps");
    renderNav();
    start();

    const apps = document.querySelector('a[href="/discovery/apps"]');
    const appsDetails = document.querySelector('details[data-active-prefixes^="/discovery"]');
    const appsSummary = appsDetails.querySelector("summary");

    expect(apps.getAttribute("aria-current")).toBe("page");
    expect(appsSummary.getAttribute("aria-current")).toBe("page");
    expect(appsDetails.open).toBe(true);
  });

  it("updates active state on htmx:afterSettle without re-rendering markup", () => {
    history.replaceState({}, "", "/identities");
    renderNav();
    start();

    expect(document.querySelector('a[href="/identities"]').getAttribute("aria-current")).toBe("page");
    expect(document.querySelector('a[href="/discovery/apps"]').getAttribute("aria-current")).toBe(null);

    navigate("/discovery/apps");

    expect(document.querySelector('a[href="/identities"]').getAttribute("aria-current")).toBe(null);
    expect(document.querySelector('a[href="/discovery/apps"]').getAttribute("aria-current")).toBe("page");
  });

  it("auto-opens a section when navigating into it but never auto-closes", () => {
    history.replaceState({}, "", "/identities");
    renderNav(["apps"]); // user has Apps & Discovery open even though they're elsewhere
    start();

    const apps = document.querySelector('details[data-active-prefixes^="/discovery"]');
    const findings = document.querySelector('details[data-active-prefixes="/findings"]');
    expect(apps.open).toBe(true);
    expect(findings.open).toBe(false);

    navigate("/findings/leaked-creds");

    // findings auto-opens, apps stays open (user agency preserved)
    expect(findings.open).toBe(true);
    expect(apps.open).toBe(true);
  });

  it("treats exact-match links as exact even when their href is a prefix of the path", () => {
    history.replaceState({}, "", "/findings/leaked-creds");
    renderNav();
    start();

    const overview = document.querySelector('a[href="/findings"][data-active-match="exact"]');
    const leaked = document.querySelector('a[href="/findings/leaked-creds"]');
    expect(overview.getAttribute("aria-current")).toBe(null);
    expect(leaked.getAttribute("aria-current")).toBe("page");
  });
});
