import { beforeEach, describe, expect, it, vi } from "vitest";

import { showFlashToast } from "open-sspm-app/toast.js";

describe("toast", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
  });

  it("appends flash toast to the toaster container", () => {
    document.body.innerHTML = `
      <section id="toaster" class="toaster" data-align="end"></section>
      <div id="flash-toast" data-category="success" data-title="Saved" data-description="Changes were saved"></div>
    `;

    showFlashToast();

    const toast = document.querySelector("#toaster .toast");
    expect(toast).not.toBeNull();
    expect(toast?.getAttribute("data-category")).toBe("success");
    expect(toast?.querySelector("h2")?.textContent).toBe("Saved");
    expect(toast?.querySelector("p")?.textContent).toBe("Changes were saved");
    expect(document.getElementById("flash-toast")).toBeNull();
  });

  it("removes empty flash toast without rendering a toast", () => {
    document.body.innerHTML = `
      <section id="toaster" class="toaster" data-align="end"></section>
      <div id="flash-toast" data-category="info"></div>
    `;

    const listener = vi.fn();
    document.addEventListener("basecoat:toast", listener);

    showFlashToast();

    expect(document.querySelector("#toaster .toast")).toBeNull();
    expect(listener).not.toHaveBeenCalled();
    expect(document.getElementById("flash-toast")).toBeNull();

    document.removeEventListener("basecoat:toast", listener);
  });

  it("does not duplicate toasts when called more than once", () => {
    document.body.innerHTML = `
      <section id="toaster" class="toaster" data-align="end"></section>
      <div id="flash-toast" data-category="success" data-title="Saved" data-description="Changes were saved"></div>
    `;

    showFlashToast();
    showFlashToast();

    expect(document.querySelectorAll("#toaster .toast")).toHaveLength(1);
    expect(document.getElementById("flash-toast")).toBeNull();
  });

  it("falls back to dispatching basecoat:toast when toaster is missing", () => {
    document.body.innerHTML = `
      <div id="flash-toast" data-category="warning" data-title="Heads up" data-description="Try again soon"></div>
    `;

    const listener = vi.fn();
    document.addEventListener("basecoat:toast", listener);

    showFlashToast();

    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener.mock.calls[0][0].detail.config).toEqual({
      category: "warning",
      title: "Heads up",
      description: "Try again soon",
    });
    expect(document.getElementById("flash-toast")).toBeNull();

    document.removeEventListener("basecoat:toast", listener);
  });
});
