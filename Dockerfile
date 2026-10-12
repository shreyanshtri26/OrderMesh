# One image for all four services. Choose the service at runtime:
#   SERVICE=order | inventory | payment | notification   one service per container
#   SERVICE=all                                          all four in one container
#                                                        (Render's free tier: one web service)
#
# Debian slim (glibc) rather than Alpine: the Kafka client ships prebuilt
# librdkafka binaries for glibc.

FROM node:22-bookworm-slim AS base
ENV PNPM_HOME=/pnpm PATH=/pnpm:$PATH
RUN corepack enable
WORKDIR /app

FROM base AS deps
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY packages/shared/package.json packages/shared/
COPY services/order/package.json services/order/
COPY services/inventory/package.json services/inventory/
COPY services/payment/package.json services/payment/
COPY services/notification/package.json services/notification/
RUN pnpm install --frozen-lockfile --prod

FROM base AS runtime
ENV NODE_ENV=production
COPY --from=deps /app /app
COPY packages packages
COPY services services
COPY scripts scripts
USER node
ENV SERVICE=all
CMD ["sh", "-c", "if [ \"$SERVICE\" = all ]; then exec node scripts/start-all.ts; else cd services/$SERVICE && exec node --import @opentelemetry/auto-instrumentations-node/register src/index.ts; fi"]
