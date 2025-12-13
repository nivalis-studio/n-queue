# syntax=docker/dockerfile:1

FROM node:22-bookworm-slim

# pnpm via corepack (matches package.json packageManager)
RUN corepack enable && corepack prepare pnpm@10.23.0 --activate

WORKDIR /repo

# Copy only what we need for install/build + the demo script.
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml tsconfig.json ./
COPY src ./src
COPY examples/quickstart/run.mjs ./examples/quickstart/run.mjs

# Avoid running repo hooks in container.
RUN pnpm install --frozen-lockfile --ignore-scripts

RUN pnpm build

ENV NODE_ENV=production

CMD ["node", "examples/quickstart/run.mjs"]
