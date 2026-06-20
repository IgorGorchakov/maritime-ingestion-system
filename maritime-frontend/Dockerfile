# ── Stage 1: build ────────────────────────────────────────────────────────────
FROM node:20-alpine AS builder
WORKDIR /app

# Install dependencies first (layer cached unless package.json changes)
COPY package*.json ./
RUN npm ci

# Copy source and build
COPY . .
RUN npm run build

# ── Stage 2: serve ────────────────────────────────────────────────────────────
FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
