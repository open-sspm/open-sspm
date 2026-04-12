import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { register, start, stop } from "open-sspm-app/components/registry.js";

const waitForObserver = async () => {
  await Promise.resolve();
  await new Promise((resolve) => setTimeout(resolve, 0));
};

let componentId = 0;

const registerTestComponent = (baseName = "dropdown-menu") => {
  componentId += 1;
  const name = `${baseName}-${componentId}`;
  const init = vi.fn();

  register(name, `.${name}:not([data-${name}-initialized])`, init);

  return { name, init };
};

const registerCleanupComponent = (baseName = "popover") => {
  componentId += 1;
  const name = `${baseName}-cleanup-${componentId}`;
  const cleanup = vi.fn();
  const init = vi.fn(() => cleanup);

  register(name, `.${name}:not([data-${name}-initialized])`, init);

  return { name, init, cleanup };
};

describe("component registry", () => {
  beforeEach(() => {
    stop();
    document.body.innerHTML = "";
  });

  afterEach(() => {
    stop();
    document.body.innerHTML = "";
  });

  it("marks kebab-case components with the expected initialized attribute", () => {
    const { name, init } = registerTestComponent();
    document.body.innerHTML = `<div class="${name}"></div>`;

    start();

    const el = document.querySelector(`.${name}`);
    expect(init).toHaveBeenCalledTimes(1);
    expect(el.getAttribute(`data-${name}-initialized`)).toBe("true");
  });

  it("initializes dynamically added components exactly once", async () => {
    const { name, init } = registerTestComponent("popover");
    start();

    const el = document.createElement("div");
    el.className = name;
    document.body.append(el);
    await waitForObserver();

    expect(init).toHaveBeenCalledTimes(1);
    expect(el.getAttribute(`data-${name}-initialized`)).toBe("true");
  });

  it("does not re-initialize a kebab-case component when the same node is re-added", async () => {
    const { name, init } = registerTestComponent();
    const el = document.createElement("div");
    el.className = name;
    document.body.append(el);

    start();

    const host = document.createElement("section");
    document.body.append(host);
    host.append(el);
    await waitForObserver();

    expect(init).toHaveBeenCalledTimes(1);
  });

  it("does not re-initialize a kebab-case component when it is re-added inside a wrapper", async () => {
    const { name, init } = registerTestComponent();
    const el = document.createElement("div");
    el.className = name;
    document.body.append(el);

    start();

    const wrapper = document.createElement("section");
    wrapper.append(el);
    document.body.append(wrapper);
    await waitForObserver();

    expect(init).toHaveBeenCalledTimes(1);
  });

  it("runs registered cleanup when an initialized node is removed", async () => {
    const { name, init, cleanup } = registerCleanupComponent();
    document.body.innerHTML = `<div class="${name}"></div>`;

    start();

    const el = document.querySelector(`.${name}`);
    el.remove();
    await waitForObserver();

    expect(init).toHaveBeenCalledTimes(1);
    expect(cleanup).toHaveBeenCalledTimes(1);
    expect(el.hasAttribute(`data-${name}-initialized`)).toBe(false);
  });
});
