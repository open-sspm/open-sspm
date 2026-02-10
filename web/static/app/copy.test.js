import { beforeEach, describe, expect, it, vi } from "vitest";

import { wireCopyButtons } from "open-sspm-app/copy.js";

const waitForAsyncWork = async () => {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
};

describe("copy", () => {
  beforeEach(() => {
    document.body.innerHTML = "";
    vi.restoreAllMocks();
  });

  it("copies text and emits success toast", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });

    document.body.innerHTML = `<button type="button" data-copy-text="tenant-1234" data-copy-label="Copy source ID">Copy</button>`;
    const listener = vi.fn();
    document.addEventListener("basecoat:toast", listener);

    wireCopyButtons(document);
    const button = document.querySelector("button");
    button.click();
    await waitForAsyncWork();

    expect(writeText).toHaveBeenCalledWith("tenant-1234");
    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener.mock.calls[0][0].detail.config).toEqual({
      category: "success",
      title: "Copied to clipboard",
      description: "source ID",
    });

    document.removeEventListener("basecoat:toast", listener);
  });

  it("emits error toast when clipboard write fails", async () => {
    const writeText = vi.fn().mockRejectedValue(new Error("denied"));
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });

    document.body.innerHTML = `<button type="button" data-copy-text="asset-999">Copy</button>`;
    const listener = vi.fn();
    document.addEventListener("basecoat:toast", listener);

    wireCopyButtons(document);
    const button = document.querySelector("button");
    button.click();
    await waitForAsyncWork();

    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener.mock.calls[0][0].detail.config).toEqual({
      category: "error",
      title: "Copy failed",
      description: "Clipboard is unavailable in this browser.",
    });

    document.removeEventListener("basecoat:toast", listener);
  });

  it("does not bind handlers multiple times", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });

    document.body.innerHTML = `<button type="button" data-copy-text="value-1">Copy</button>`;
    wireCopyButtons(document);
    wireCopyButtons(document);

    const button = document.querySelector("button");
    button.click();
    await waitForAsyncWork();

    expect(writeText).toHaveBeenCalledTimes(1);
  });
});
