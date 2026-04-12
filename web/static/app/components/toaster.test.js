import { beforeEach, describe, expect, it } from "vitest";

import "open-sspm-app/components/toast.js";
import { start, stop } from "open-sspm-app/components/registry.js";

const waitForObserver = async () => {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
};

describe("toaster component", () => {
  beforeEach(() => {
    stop();
    document.body.innerHTML = "";
  });

  it("removes document toast listeners when a toaster is detached", async () => {
    document.body.innerHTML = `<section id="toaster-a" class="toaster"></section>`;
    start();

    const oldToaster = document.getElementById("toaster-a");
    oldToaster.remove();
    await waitForObserver();

    const newToaster = document.createElement("section");
    newToaster.id = "toaster-b";
    newToaster.className = "toaster";
    document.body.append(newToaster);
    await waitForObserver();

    document.dispatchEvent(
      new CustomEvent("osspm:toast", {
        detail: {
          config: { category: "success", title: "Saved", description: "Done" },
        },
      }),
    );

    expect(oldToaster.querySelectorAll(".toast")).toHaveLength(0);
    expect(newToaster.querySelectorAll(".toast")).toHaveLength(1);
  });
});
