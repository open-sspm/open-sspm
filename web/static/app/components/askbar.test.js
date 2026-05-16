import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { initAskbar } from "open-sspm-app/components/askbar.js";

const config = {
  fieldParam: {
    search: "q",
    credential_kind: "credential_kind",
    expiry_state: "expiry_state",
    expires_in_days: "expires_in_days",
    owner: "owner",
    asset: "asset",
    newer_days: "newer_days",
    status: "status",
    row_state: "row_state",
  },
  keyLabel: {
    search: "",
    credential_kind: "kind",
    expiry_state: "expiry",
    expires_in_days: "expires",
    owner: "owner",
    asset: "asset",
    newer_days: "newer",
    status: "status",
    row_state: "state",
  },
  fieldLabel: {
    credential_kind: "Credential kind",
    expiry_state: "Expiry",
    expires_in_days: "Expires in",
    owner: "Owner",
    asset: "Asset",
    newer_days: "Newer than",
    status: "Status",
    row_state: "State",
  },
  keywordTokens: {
    pat: {
      field: "credential_kind",
      value: "github_pat_request,github_pat_fine_grained",
      label: "PAT",
    },
    "30d": {
      field: "expires_in_days",
      value: "30",
      label: "< 30d",
      tone: "warn",
    },
    "newer-7d": {
      field: "newer_days",
      value: "7",
      label: "7d",
    },
    revoked: {
      field: "status",
      value: "revoked",
      label: "revoked",
      tone: "danger",
    },
    expired: {
      field: "expiry_state",
      value: "expired",
      label: "expired",
      tone: "danger",
    },
    "action-required": {
      field: "row_state",
      value: "action_required",
      label: "needs action",
      tone: "danger",
    },
  },
  fieldAliases: {
    kind: "credential_kind",
    expiry: "expiry_state",
    expires: "expires_in_days",
    owner: "owner",
    asset: "asset",
    newer: "newer_days",
    status: "status",
    row: "row_state",
    state: "row_state",
  },
  stopwords: [],
  singletonFields: [
    "credential_kind",
    "expiry_state",
    "expires_in_days",
    "owner",
    "asset",
    "newer_days",
    "status",
    "row_state",
  ],
  freeTextFields: ["owner", "asset"],
  staticHidden: [
    { name: "source_kind", value: "google_workspace" },
    { name: "asset_kind", value: "google_oauth_client" },
  ],
};

const renderAskbar = ({ htmx = true, extraControls = "" } = {}) => {
  document.body.innerHTML = `
    <form ${htmx ? 'hx-get="/items"' : ""}>
      <div data-osspm-askbar data-osspm-askbar-config='${JSON.stringify(config)}'>
        <label data-osspm-askbar-bar>
          <span data-osspm-askbar-chips></span>
          <input data-osspm-askbar-input placeholder="Search" />
          <button type="button" data-osspm-askbar-add-filter>Filter</button>
        </label>
        <div data-osspm-askbar-suggest hidden></div>
        <div data-osspm-askbar-bank data-table-query-trigger></div>
      </div>
      ${extraControls}
    </form>
  `;
  return document.querySelector("[data-osspm-askbar]");
};

const bankValues = (root) =>
  Object.fromEntries(
    Array.from(root.querySelectorAll("[data-osspm-askbar-bank] input")).map(
      (input) => [input.name, input.value],
    ),
  );

const bankDefaultValues = (root) =>
  Object.fromEntries(
    Array.from(root.querySelectorAll("[data-osspm-askbar-bank] input")).map(
      (input) => [input.name, input.defaultValue],
    ),
  );

const chipLabels = (root) =>
  Array.from(root.querySelectorAll(".osspm-askbar-chip-label"), (node) =>
    node.textContent.trim(),
  );

describe("askbar", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
  });

  afterEach(() => {
    document.body.innerHTML = "";
    window.history.replaceState({}, "", "/");
    vi.restoreAllMocks();
  });

  it("parses field grammar into canonical filter inputs", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "expires:<30d owner:me kind:PAT asset:GitHub newer:7d";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(bankValues(root)).toEqual({
      source_kind: "google_workspace",
      asset_kind: "google_oauth_client",
      expires_in_days: "30",
      owner: "me",
      credential_kind: "github_pat_request,github_pat_fine_grained",
      asset: "github",
      newer_days: "7",
    });
  });

  it("parses multi-token grammar on Enter even when suggestions are open", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "expires:<30d owner:me kind:PAT asset:GitHub newer:7d";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(bankValues(root)).toEqual({
      source_kind: "google_workspace",
      asset_kind: "google_oauth_client",
      expires_in_days: "30",
      owner: "me",
      credential_kind: "github_pat_request,github_pat_fine_grained",
      asset: "github",
      newer_days: "7",
    });
  });

  it("parses field grammar on Enter instead of treating it as text search", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "expires:<30d";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(bankValues(root)).toMatchObject({
      source_kind: "google_workspace",
      asset_kind: "google_oauth_client",
      expires_in_days: "30",
    });
    expect(bankValues(root)).not.toHaveProperty("q");
  });

  it("parses free-text field grammar on Enter", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "asset:GitHub";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(bankValues(root)).toMatchObject({
      source_kind: "google_workspace",
      asset_kind: "google_oauth_client",
      asset: "github",
    });
    expect(bankValues(root)).not.toHaveProperty("q");
  });

  it("opens suggestions from the add filter button with an empty input", () => {
    const root = renderAskbar();
    initAskbar(root);

    root.querySelector("[data-osspm-askbar-add-filter]").click();

    const suggest = root.querySelector("[data-osspm-askbar-suggest]");
    expect(suggest.hidden).toBe(false);
    expect(suggest.textContent).toContain("Suggestions");
    expect(suggest.textContent).toContain("< 30d");
  });

  it("keeps static scope inputs when chips are rewritten", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "kind:PAT";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(bankValues(root)).toMatchObject({
      source_kind: "google_workspace",
      asset_kind: "google_oauth_client",
      credential_kind: "github_pat_request,github_pat_fine_grained",
    });
  });

  it("writes dynamic hidden values as defaults for htmx query preservation", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "notakey";
    input.dispatchEvent(new InputEvent("input", { bubbles: true }));
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(bankValues(root)).toMatchObject({ q: "notakey" });
    expect(bankDefaultValues(root)).toMatchObject({ q: "notakey" });
  });

  it("matches field:value where the canonical value uses underscores against the hyphenated keyword form", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "state:action-required";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(bankValues(root)).toMatchObject({ row_state: "action_required" });
    expect(bankValues(root)).not.toHaveProperty("q");
  });

  it("does not strip a trailing 'd' from non-numeric field:value input", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "status:revoked";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(bankValues(root)).toMatchObject({ status: "revoked" });
    expect(bankValues(root)).not.toHaveProperty("q");
  });

  it("drops field:value silently when the field is recognized but the value is unknown", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "kind:bogusvalue";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    const values = bankValues(root);
    expect(values).not.toHaveProperty("credential_kind");
    expect(values).not.toHaveProperty("q");
  });

  it("writes the chip label on the remove button aria-label for screen readers", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "kind:PAT";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    const removeBtn = root.querySelector("[data-osspm-askbar-chip-remove]");
    expect(removeBtn?.getAttribute("aria-label")).toBe("Remove PAT");
  });

  it("reconciles chips and hidden inputs when htmx pushes a new URL", () => {
    const root = renderAskbar();
    initAskbar(root);
    const bank = root.querySelector("[data-osspm-askbar-bank]");
    const changeSpy = vi.fn();
    bank.addEventListener("change", changeSpy);

    window.history.pushState({}, "", "/items?q=alice&credential_kind=github_pat_request%2Cgithub_pat_fine_grained&status=revoked");
    document.dispatchEvent(new CustomEvent("htmx:pushedIntoHistory", { bubbles: true }));

    expect(chipLabels(root)).toEqual(['"alice"', "PAT", "revoked"]);
    expect(bankValues(root)).toMatchObject({
      source_kind: "google_workspace",
      asset_kind: "google_oauth_client",
      q: "alice",
      credential_kind: "github_pat_request,github_pat_fine_grained",
      status: "revoked",
    });
    expect(changeSpy).not.toHaveBeenCalled();
  });

  it("reconciles chips on browser history navigation without submitting", () => {
    const root = renderAskbar();
    initAskbar(root);
    const bank = root.querySelector("[data-osspm-askbar-bank]");
    const changeSpy = vi.fn();
    bank.addEventListener("change", changeSpy);

    window.history.pushState({}, "", "/items?status=revoked");
    window.dispatchEvent(new PopStateEvent("popstate"));

    expect(chipLabels(root)).toEqual(["revoked"]);
    expect(bankValues(root)).toMatchObject({
      source_kind: "google_workspace",
      asset_kind: "google_oauth_client",
      status: "revoked",
    });
    expect(changeSpy).not.toHaveBeenCalled();
  });

  it("does not rewrite existing chips for an idempotent history event", () => {
    const root = renderAskbar();
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "status:revoked";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    const firstChip = root.querySelector("[data-osspm-askbar-chip]");

    window.history.pushState({}, "", "/items?status=revoked");
    document.dispatchEvent(new CustomEvent("htmx:pushedIntoHistory", { bubbles: true }));

    expect(root.querySelector("[data-osspm-askbar-chip]")).toBe(firstChip);
    expect(chipLabels(root)).toEqual(["revoked"]);
  });

  it("suppresses implicit active expiry chips when expires_in_days is in the URL", () => {
    const root = renderAskbar();
    initAskbar(root);

    window.history.pushState({}, "", "/items?expiry_state=active&expires_in_days=30");
    document.dispatchEvent(new CustomEvent("htmx:pushedIntoHistory", { bubbles: true }));

    expect(chipLabels(root)).toEqual(["< 30d"]);
    expect(bankValues(root)).toMatchObject({
      source_kind: "google_workspace",
      asset_kind: "google_oauth_client",
      expiry_state: "active",
      expires_in_days: "30",
    });
  });

  it("syncs ordinary form controls from htmx history without duplicating hidden params", () => {
    const root = renderAskbar({
      extraControls: `
        <select name="sort_by" aria-label="Sort credentials">
          <option value="expires_soonest" selected>Expires soonest</option>
          <option value="highest_risk">Highest risk</option>
        </select>
      `,
    });
    initAskbar(root);
    const form = root.closest("form");
    const sort = form.querySelector("[name=sort_by]");

    window.history.pushState({}, "", "/items?q=alice&sort_by=highest_risk");
    document.dispatchEvent(new CustomEvent("htmx:pushedIntoHistory", { bubbles: true }));

    expect(sort.value).toBe("highest_risk");
    expect(chipLabels(root)).toEqual(['"alice"']);
    expect(bankValues(root)).not.toHaveProperty("sort_by");
    expect(new FormData(form).getAll("sort_by")).toEqual(["highest_risk"]);

    window.history.pushState({}, "", "/items?q=alice");
    document.dispatchEvent(new CustomEvent("htmx:pushedIntoHistory", { bubbles: true }));

    expect(sort.value).toBe("expires_soonest");
    expect(bankValues(root)).not.toHaveProperty("sort_by");
    expect(new FormData(form).getAll("sort_by")).toEqual(["expires_soonest"]);
  });

  it("submits a non-htmx form after a filter change", () => {
    const root = renderAskbar({ htmx: false });
    const form = root.closest("form");
    const submitSpy = vi.fn((event) => event.preventDefault());
    const requestSubmitSpy = vi.spyOn(form, "requestSubmit").mockImplementation(() => {
      form.dispatchEvent(new Event("submit", { bubbles: true, cancelable: true }));
    });
    form.addEventListener("submit", submitSpy);
    initAskbar(root);

    const input = root.querySelector("[data-osspm-askbar-input]");
    input.value = "notakey";
    input.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));

    expect(requestSubmitSpy).toHaveBeenCalledOnce();
    expect(submitSpy).toHaveBeenCalledOnce();
  });
});
