# notify

Monorepo root for the notify project.

## Apps

- `apps/server` — Rust WebSocket notification server for trusted internal and development environments

## Root tooling

- `package.json` — Bun-based Husky + Commitlint setup
- `.husky/` — Git hooks
- `commitlint.config.js` — Conventional Commit rules

## Common commands

### Run the backend

```bash
cd apps/server
cargo run
```

### Test the backend

```bash
cd apps/server
cargo test
```

See [`apps/server/README.md`](apps/server/README.md) for server details and
[`apps/server/docs/frontend-integration.md`](apps/server/docs/frontend-integration.md)
for client integration examples.
