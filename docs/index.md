---
layout: home

hero:
  name: Open-SSPM
  text: IAM and SaaS Governance
  tagline: Know who has access to what across your identity providers and SaaS applications.
  actions:
    - theme: brand
      text: Get Started
      link: /install/
    - theme: alt
      text: View on GitHub
      link: https://github.com/open-sspm/open-sspm

features:
  - title: Identity Provider Sync
    details: Sync users, groups, and app assignments from Okta and Microsoft Entra ID.
  - title: SaaS Application Discovery
    details: Discover OAuth apps, token grants, and SSO activity across your IdPs.
  - title: Connected Apps
    details: Sync permissions from GitHub, Google Workspace, Datadog, and AWS Identity Center.
  - title: Account Linking
    details: Automatically link accounts by email or manually connect identities without matching addresses.
  - title: Security Findings
    details: Evaluate your Okta configuration against CIS benchmarks and security rules.
  - title: Self-Hosted
    details: Run locally with Docker-backed Postgres or deploy the container image to Kubernetes.
---

## Quick Start

```bash
git clone https://github.com/open-sspm/open-sspm.git
cd open-sspm
cp .env.example .env
docker compose up -d
# Open http://localhost:8080
```

See the [Installation Guide](/install/) for production deployment options.

**Live demo** — try Open-SSPM without installing at [demo.opensspm.com](https://demo.opensspm.com) (admin@admin.com / admin, resets daily).
