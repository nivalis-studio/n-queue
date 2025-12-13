# Contributing

Thanks for your interest in contributing!

## Development Setup

Prerequisites:
- Node.js 18+
- pnpm (via Corepack)
- Docker (for integration tests)

Install dependencies:

```bash
corepack enable
pnpm install
```

Run checks:

```bash
pnpm lint
pnpm ts
pnpm test
```

Run integration tests (Redis in Docker):

```bash
pnpm test:docker
```

## Project Conventions

- Formatting/linting: Biome (`pnpm lint`)
- Type checking: TypeScript (`pnpm ts`)
- Tests: Bun (`pnpm test`)

## Pull Requests

- Keep PRs focused (one change category per PR).
- Include tests when adding or changing behavior.
- If you change public API, update `README.md` examples.
