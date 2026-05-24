# Architecture

This section explains the data and control structures that hold Open-SSPM
together. The intent is to keep them small and stable so connectors, the
evaluation engine, and the UI can evolve independently.

## Sections

- [Identity Graph](/architecture/identity-graph) — How source accounts roll up
  into normalized identities, what link reasons mean, and which invariants
  must not be relaxed.

## Reading Order

Start with the [Identity Graph](/architecture/identity-graph) page. Most other
Open-SSPM behavior — access rollups, authoritative anchors, connector
discovery, governance review — sits on top of that model.
