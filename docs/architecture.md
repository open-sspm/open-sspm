# Architecture

Open-SSPM is structured around a small set of commands and worker lanes rather than a large distributed service mesh.

## Runtime components

- `open-sspm serve`: HTTP server and UI
- `open-sspm worker`: background full sync loop
- `open-sspm worker-discovery`: background SaaS discovery loop
- `open-sspm sync`: one-off full sync
- `open-sspm sync-discovery`: one-off discovery sync

## Key concepts to document next

- full sync versus discovery sync
- configured connectors versus unconfigured sources
- identity matching and manual linking
- findings and rule seeding
- programmatic access and discovered SaaS assets

## Documentation TODO

This page should later include:

- a component diagram
- request and data flow notes
- sync lifecycle details
- the relationship between UI actions and background jobs
