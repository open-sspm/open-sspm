import { beforeEach, describe, expect, it, vi } from "vitest";

import { showFlashToast } from "open-sspm-app/toast.js";

describe("toast", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
  });

  it("dispatches basecoat:toast and removes flash toast", () => {
    document.body.innerHTML = `
      <div id="flash-toast" data-category="success" data-title="Saved" data-description="Changes were saved"></div>
    `;

    const listener = vi.fn();
    document.addEventListener("basecoat:toast", listener);

    showFlashToast();

    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener.mock.calls[0][0].detail.config).toEqual({
      category: "success",
      title: "Saved",
      description: "Changes were saved",
    });
    expect(document.getElementById("flash-toast")).toBeNull();

    document.removeEventListener("basecoat:toast", listener);
  });

  it("removes empty flash toast without dispatching", () => {
    document.body.innerHTML = `<div id="flash-toast" data-category="info"></div>`;

    const listener = vi.fn();
    document.addEventListener("basecoat:toast", listener);

    showFlashToast();

    expect(listener).not.toHaveBeenCalled();
    expect(document.getElementById("flash-toast")).toBeNull();

    document.removeEventListener("basecoat:toast", listener);
  });
});
