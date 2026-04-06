# Getting Started

This page is the entry point for first-time operators and evaluators.

## Local quickstart

The current application quickstart already exists in the repo README and should be migrated here in a more task-oriented format:

1. Copy `.env.example` to `.env`.
2. Start Postgres with `just dev-up`.
3. Run migrations with `just migrate`.
4. Install Node dependencies and build CSS with `npm install && just ui`.
5. Start the app with `just run`.
6. Start background workers with `just worker` and `just worker-discovery`.

## What this page should grow into

- prerequisites and local tooling
- first login and admin bootstrap
- connector secret key setup
- first sync and expected UI flows
- evaluation path for demo users
