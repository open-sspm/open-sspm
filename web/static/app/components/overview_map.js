/**
 * Overview map component
 *
 * Keeps connector-card hover/focus state aligned with the matching SVG edge.
 */
import { register } from "./registry.js";

const nodeSelector = "[data-overview-map-node]";
const edgeSelector = "[data-overview-map-edge]";

const setActive = (surface, index) => {
  const nodes = Array.from(surface.querySelectorAll(nodeSelector));
  const edges = Array.from(surface.querySelectorAll(edgeSelector));
  const activeNode = nodes.find((node) => node.dataset.overviewMapNode === index);

  if (!activeNode) return;

  const accent = getComputedStyle(activeNode)
    .getPropertyValue("--overview-map-accent")
    .trim();

  surface.dataset.overviewMapActive = index;
  if (accent) surface.style.setProperty("--overview-map-active-accent", accent);

  nodes.forEach((node) => {
    node.toggleAttribute("data-overview-map-active", node.dataset.overviewMapNode === index);
  });
  edges.forEach((edge) => {
    edge.toggleAttribute("data-overview-map-active", edge.dataset.overviewMapEdge === index);
  });
};

const clearActive = (surface) => {
  delete surface.dataset.overviewMapActive;
  surface.style.removeProperty("--overview-map-active-accent");
  surface.querySelectorAll(`${nodeSelector}, ${edgeSelector}`).forEach((el) => {
    el.removeAttribute("data-overview-map-active");
  });
};

const eventTargetNode = (event) => {
  if (!(event.target instanceof Element)) return null;
  return event.target.closest(nodeSelector);
};

const containsRelatedTarget = (node, relatedTarget) =>
  relatedTarget instanceof Node && node.contains(relatedTarget);

const init = (surface) => {
  const onPointerOver = (event) => {
    const node = eventTargetNode(event);
    if (!node) return;
    setActive(surface, node.dataset.overviewMapNode);
  };

  const onPointerOut = (event) => {
    const node = eventTargetNode(event);
    if (!node || containsRelatedTarget(node, event.relatedTarget)) return;
    clearActive(surface);
  };

  const onPointerLeave = () => {
    clearActive(surface);
  };

  const onFocusIn = (event) => {
    const node = eventTargetNode(event);
    if (!node) return;
    setActive(surface, node.dataset.overviewMapNode);
  };

  const onFocusOut = (event) => {
    const node = eventTargetNode(event);
    if (!node || containsRelatedTarget(node, event.relatedTarget)) return;
    clearActive(surface);
  };

  surface.addEventListener("pointerover", onPointerOver);
  surface.addEventListener("pointerout", onPointerOut);
  surface.addEventListener("pointerleave", onPointerLeave);
  surface.addEventListener("focusin", onFocusIn);
  surface.addEventListener("focusout", onFocusOut);

  return () => {
    clearActive(surface);
    surface.removeEventListener("pointerover", onPointerOver);
    surface.removeEventListener("pointerout", onPointerOut);
    surface.removeEventListener("pointerleave", onPointerLeave);
    surface.removeEventListener("focusin", onFocusIn);
    surface.removeEventListener("focusout", onFocusOut);
  };
};

register(
  "overview-map",
  "[data-overview-map-surface]:not([data-overview-map-initialized])",
  init,
);
