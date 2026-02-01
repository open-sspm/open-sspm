(() => {
  const sidebarToggle = document.getElementById("sidebar-toggle");

  if (sidebarToggle) {
    sidebarToggle.addEventListener("click", () => {
      document.dispatchEvent(new CustomEvent("basecoat:sidebar", { detail: { id: "app-sidebar" } }));
    });
  }

  const showFlashToast = () => {
    const toastEl = document.getElementById("flash-toast");
    if (!(toastEl instanceof HTMLElement)) return;

    const category = (toastEl.dataset.category || "info").trim() || "info";
    const title = (toastEl.dataset.title || "").trim();
    const description = (toastEl.dataset.description || "").trim();
    if (!title && !description) {
      toastEl.remove();
      return;
    }

    document.dispatchEvent(
      new CustomEvent("basecoat:toast", {
        detail: {
          config: {
            category,
            title,
            description,
          },
        },
      }),
    );

    toastEl.remove();
  };

  const openServerDialogs = () => {
    document.querySelectorAll("dialog[data-open]").forEach((dialog) => {
      if (!(dialog instanceof HTMLDialogElement)) return;
      if (dialog.open) return;
      try {
        dialog.showModal();
      } catch {
        dialog.setAttribute("open", "");
      }
    });
  };

  const wireDialogCloseNavigation = () => {
    document.querySelectorAll("dialog[data-close-href]").forEach((dialog) => {
      if (!(dialog instanceof HTMLDialogElement)) return;

      const closeHref = dialog.getAttribute("data-close-href");
      if (!closeHref) return;

      const navigateToCloseHref = () => {
        const current = window.location.pathname + window.location.search;
        if (current === closeHref) return;
        window.location.href = closeHref;
      };

      dialog.addEventListener("cancel", (event) => {
        event.preventDefault();
        navigateToCloseHref();
      });

      dialog.addEventListener("close", () => {
        navigateToCloseHref();
      });
    });
  };

  const wireAutosubmit = (root = document) => {
    root.querySelectorAll("[data-autosubmit]").forEach((element) => {
      if (!(element instanceof HTMLElement)) return;
      if (element.dataset.autosubmitBound === "true") return;
      element.addEventListener("change", () => {
        const form = element.closest("form");
        if (form instanceof HTMLFormElement) {
          if (typeof form.requestSubmit === "function") {
            form.requestSubmit();
          } else {
            form.submit();
          }
        }
      });
      element.dataset.autosubmitBound = "true";
    });
  };

  const initIdpUserAccessGraph = () => {
    const container = document.getElementById("access-graph");
    if (!(container instanceof HTMLElement)) return;

    const userID = container.getAttribute("data-idp-user-id");
    if (!userID) {
      container.textContent = "Missing user id.";
      return;
    }

    const email = (container.getAttribute("data-idp-user-email") || "").trim();
    const displayName = (container.getAttribute("data-idp-user-display-name") || "").trim();

    const state = {
      userID,
      cache: new Map(),
      inflight: new Map(),
    };

    const fetchChildren = async (nodeID) => {
      if (state.cache.has(nodeID)) return state.cache.get(nodeID);
      if (state.inflight.has(nodeID)) return state.inflight.get(nodeID);

      const url = `/api/idp-users/${encodeURIComponent(state.userID)}/access-tree?node=${encodeURIComponent(nodeID)}`;
      const promise = fetch(url, { headers: { Accept: "application/json" } }).then(async (response) => {
        let body;
        try {
          body = await response.json();
        } catch {
          body = null;
        }
        if (!response.ok) {
          const message = body && typeof body.error === "string" && body.error.trim() ? body.error.trim() : `Request failed (${response.status})`;
          throw new Error(message);
        }
        if (!body || !Array.isArray(body.nodes)) {
          throw new Error("Invalid response.");
        }
        return body.nodes;
      });

      state.inflight.set(nodeID, promise);
      try {
        const nodes = await promise;
        state.cache.set(nodeID, nodes);
        return nodes;
      } finally {
        state.inflight.delete(nodeID);
      }
    };

    const setToggleExpanded = (nodeEl, expanded) => {
      const toggle = nodeEl.querySelector("button[data-action='toggle']");
      if (!(toggle instanceof HTMLButtonElement)) return;
      toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
      toggle.setAttribute("aria-label", expanded ? "Collapse" : "Expand");
      toggle.textContent = expanded ? "▾" : "▸";
    };

    const createNodeElement = (node, depth) => {
      const nodeEl = document.createElement("div");
      nodeEl.className = "access-tree-node";
      nodeEl.dataset.nodeId = String(node.id || "");
      nodeEl.dataset.depth = String(depth);
      nodeEl.dataset.hasChildren = node.hasChildren ? "true" : "false";
      nodeEl.dataset.loaded = "false";

      const row = document.createElement("div");
      row.className = "flex items-start gap-2 py-1";

      if (node.hasChildren) {
        const toggle = document.createElement("button");
        toggle.type = "button";
        toggle.className = "btn-icon-ghost";
        toggle.dataset.action = "toggle";
        toggle.textContent = "▸";
        toggle.setAttribute("aria-expanded", "false");
        toggle.setAttribute("aria-label", "Expand");
        row.append(toggle);
      } else {
        const spacer = document.createElement("span");
        spacer.className = "inline-block h-9 w-9";
        row.append(spacer);
      }

      const body = document.createElement("div");
      body.className = "min-w-0 flex-1";

      const top = document.createElement("div");
      top.className = "flex flex-wrap items-center gap-2";

      const labelText = String(node.label || "");
      const href = typeof node.href === "string" ? node.href.trim() : "";
      if (href) {
        const link = document.createElement("a");
        link.href = href;
        link.className = "btn-sm-link px-0 font-medium";
        link.textContent = labelText;
        top.append(link);
      } else {
        const label = document.createElement("span");
        label.className = "font-medium";
        label.textContent = labelText;
        top.append(label);
      }

      if (Array.isArray(node.badges) && node.badges.length > 0) {
        node.badges.forEach((badge) => {
          if (typeof badge !== "string") return;
          const text = badge.trim();
          if (!text) return;
          const el = document.createElement("span");
          el.className = "badge-outline";
          el.textContent = text;
          top.append(el);
        });
      }

      body.append(top);

      const subLabelText = typeof node.subLabel === "string" ? node.subLabel.trim() : "";
      if (subLabelText) {
        const subLabel = document.createElement("div");
        subLabel.className = "text-xs text-muted-foreground break-all";
        subLabel.textContent = subLabelText;
        body.append(subLabel);
      }

      row.append(body);

      const children = document.createElement("div");
      children.dataset.role = "children";
      children.className = "ml-4 border-l border-border pl-4";
      children.hidden = true;

      nodeEl.append(row, children);
      return nodeEl;
    };

    const renderChildren = (childrenEl, nodes, depth) => {
      childrenEl.textContent = "";
      nodes.forEach((child) => {
        if (!child || typeof child !== "object") return;
        if (typeof child.id !== "string" || !child.id.trim()) return;
        childrenEl.append(createNodeElement(child, depth));
      });
    };

    const toggleNode = async (nodeEl) => {
      if (!(nodeEl instanceof HTMLElement)) return;

      const hasChildren = nodeEl.dataset.hasChildren === "true";
      if (!hasChildren) return;

      const children = nodeEl.querySelector("[data-role='children']");
      if (!(children instanceof HTMLElement)) return;

      const isExpanded = !children.hidden;
      if (isExpanded) {
        children.hidden = true;
        setToggleExpanded(nodeEl, false);
        return;
      }

      children.hidden = false;
      setToggleExpanded(nodeEl, true);

      if (nodeEl.dataset.loaded === "true") return;

      const nodeID = nodeEl.dataset.nodeId;
      const depth = Number.parseInt(nodeEl.dataset.depth || "0", 10) + 1;

      children.textContent = "";
      const loading = document.createElement("div");
      loading.className = "text-sm text-muted-foreground py-1";
      loading.textContent = "Loading…";
      children.append(loading);

      try {
        const nodes = await fetchChildren(nodeID);
        renderChildren(children, nodes, depth);
        nodeEl.dataset.loaded = "true";
      } catch (error) {
        nodeEl.dataset.loaded = "false";
        children.textContent = "";
        const message = error instanceof Error ? error.message : "Failed to load.";
        const errEl = document.createElement("div");
        errEl.className = "text-sm text-destructive py-1";
        errEl.textContent = message;
        children.append(errEl);
      }
    };

    container.textContent = "";
    container.className = "space-y-1";

    const rootLabel = displayName || email || `User ${userID}`;
    const rootSubLabel = displayName && email ? email : "";

    const root = createNodeElement({ id: "root", label: rootLabel, subLabel: rootSubLabel, hasChildren: true }, 0);
    container.append(root);
    void toggleNode(root);

    container.addEventListener("click", (event) => {
      const target = event.target;
      if (!(target instanceof Element)) return;
      const toggle = target.closest("button[data-action='toggle']");
      if (!(toggle instanceof HTMLButtonElement)) return;
      const nodeEl = toggle.closest(".access-tree-node");
      if (!(nodeEl instanceof HTMLElement)) return;
      event.preventDefault();
      void toggleNode(nodeEl);
    });
  };

  document.addEventListener("DOMContentLoaded", () => {
    showFlashToast();
    openServerDialogs();
    wireDialogCloseNavigation();
    wireAutosubmit();
    document.addEventListener("htmx:load", (event) => {
      if (event.target instanceof HTMLElement) {
        wireAutosubmit(event.target);
      } else {
        wireAutosubmit();
      }
    });
    initIdpUserAccessGraph();

    document.addEventListener("keydown", (e) => {
      if (e.key === "/" && document.activeElement.tagName !== "INPUT" && document.activeElement.tagName !== "TEXTAREA") {
        e.preventDefault();
        const searchInput = document.getElementById("command-search-input");
        if (searchInput) {
          searchInput.focus();
        }
      }
    });
  });
})();
