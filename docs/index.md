---
layout: home

hero:
  name: Open-SSPM
  text: Small IAM and SaaS governance documentation
  tagline: Deployment, connector setup, sync model, and operational guidance for Open-SSPM.
  actions:
    - theme: brand
      text: Get Started
      link: /getting-started
    - theme: alt
      text: Deployment
      link: /deployment

features:
  - title: In-repo docs
    details: This site lives with the application code so it can evolve with the product and release process.
  - title: Operator-focused
    details: The first pass emphasizes installation, connector setup, sync behavior, and operational troubleshooting.
  - title: Static hosting
    details: The site builds with VitePress and deploys to GitHub Pages without depending on the Go application runtime.
---

## What belongs here

This docs site should answer four questions quickly:

1. What does Open-SSPM do?
2. How do I run and deploy it?
3. How do I configure connectors and sync data?
4. How do I troubleshoot and operate it safely?

## Initial scope

The current skeleton creates the starting structure for:

- quickstart and local setup
- architecture and product concepts
- deployment and hosting
- connectors and configuration
- operations and troubleshooting

## Source material

The initial content should be expanded from the existing repo sources:

- `README.md`
- `helm/README.md`
- `demo/infra/DEPLOYMENT.md`
- `.env.example`
- `justfile`
