# Match the Bun lockfile version committed by this repository.
FROM oven/bun:1.4.4

WORKDIR /app

# Install from the lockfile before copying source to keep dependency layers cacheable.
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# Build production output; the build also type-checks the no-emit integration project.
COPY . .
RUN bun run build

EXPOSE 3000
# Apply pending reversible migrations before starting the API and background workers.
CMD ["sh", "-c", "bun run db:migrate && bun run start"]