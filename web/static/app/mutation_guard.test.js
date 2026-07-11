import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";

let htmx;
let requests;
let restoreXPathExpression;

class ControlledXMLHttpRequest {
  constructor() {
    this.headers = {};
    this.response = "";
    this.responseText = "";
    this.responseURL = "";
    this.status = 0;
    this.upload = { addEventListener: vi.fn() };
  }

  addEventListener() {}

  getAllResponseHeaders() {
    return "";
  }

  getResponseHeader() {
    return null;
  }

  open(method, url) {
    this.method = method;
    this.url = url;
    this.responseURL = new URL(url, window.location.href).href;
  }

  overrideMimeType() {}

  setRequestHeader(name, value) {
    this.headers[name] = value;
  }

  send() {
    requests.push(this);
  }

  respond(status = 204, body = "") {
    this.status = status;
    this.response = body;
    this.responseText = body;
    this.onload?.();
  }

  fail() {
    this.onerror?.();
  }
}

describe("HTMX mutation guards", () => {
  beforeAll(async () => {
    vi.stubGlobal("XMLHttpRequest", ControlledXMLHttpRequest);
    const createExpression = XPathEvaluator.prototype.createExpression;
    XPathEvaluator.prototype.createExpression = function createCompatibleExpression(expression, resolver) {
      const compiled = createExpression.call(this, expression, resolver);
      const evaluate = compiled.evaluate.bind(compiled);
      compiled.evaluate = (contextNode, type = XPathResult.ANY_TYPE, result = null) => evaluate(contextNode, type, result);
      return compiled;
    };
    restoreXPathExpression = () => {
      XPathEvaluator.prototype.createExpression = createExpression;
    };
    ({ default: htmx } = await import("htmx.org"));
  });

  beforeEach(() => {
    document.body.innerHTML = "";
    requests = [];
  });

  afterAll(() => {
    restoreXPathExpression?.();
    vi.unstubAllGlobals();
  });

  it("drops a repeated form submission and restores its submit control", () => {
    document.body.innerHTML = `
      <form hx-post="/save" hx-sync="this:drop"
            hx-disabled-elt="find button[type='submit']:not(:disabled), find input[type='submit']:not(:disabled)">
        <input name="name" value="Example" />
        <button type="submit">Save</button>
        <input type="submit" value="Unavailable" disabled />
      </form>
    `;
    htmx.process(document.body);

    const form = document.querySelector("form");
    const save = document.querySelector("button[type='submit']");
    const unavailable = document.querySelector("input[type='submit']");
    form.requestSubmit(save);
    form.requestSubmit();

    expect(requests).toHaveLength(1);
    expect(save.disabled).toBe(true);
    expect(unavailable.disabled).toBe(true);

    requests[0].respond();

    expect(save.disabled).toBe(false);
    expect(unavailable.disabled).toBe(true);
  });

  it("shares one lock across separate forms in a connector row", () => {
    document.body.innerHTML = `
      <table><tbody>
        <tr id="connector-row-okta">
          <td>
            <form id="toggle" hx-post="/toggle" hx-sync="#connector-row-okta:drop"
                  hx-disabled-elt="#connector-row-okta input[type='checkbox']:not(:disabled)">
              <input type="checkbox" name="enabled" />
            </form>
          </td>
          <td>
            <form id="authoritative" hx-post="/authoritative" hx-sync="#connector-row-okta:drop"
                  hx-disabled-elt="#connector-row-okta input[type='checkbox']:not(:disabled)">
              <input type="checkbox" name="authoritative" />
            </form>
          </td>
        </tr>
      </tbody></table>
    `;
    htmx.process(document.body);

    const toggle = document.getElementById("toggle");
    const authoritative = document.getElementById("authoritative");
    toggle.requestSubmit();
    authoritative.requestSubmit();

    expect(requests).toHaveLength(1);
    document.querySelectorAll("input[type='checkbox']").forEach((input) => {
      expect(input.disabled).toBe(true);
    });

    requests[0].respond();

    document.querySelectorAll("input[type='checkbox']").forEach((input) => {
      expect(input.disabled).toBe(false);
    });
  });

  it("restores controls after a transport failure", () => {
    document.body.innerHTML = `
      <form hx-post="/save" hx-sync="this:drop" hx-disabled-elt="find button[type='submit']:not(:disabled)">
        <button type="submit">Save</button>
      </form>
    `;
    htmx.process(document.body);

    const form = document.querySelector("form");
    const save = document.querySelector("button");
    form.requestSubmit(save);
    expect(save.disabled).toBe(true);

    const consoleError = vi.spyOn(console, "error").mockImplementation(() => {});
    requests[0].fail();
    consoleError.mockRestore();

    expect(save.disabled).toBe(false);
  });
});
