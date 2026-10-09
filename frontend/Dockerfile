# Bibliagraphia frontend: build the React/Vite app, then serve it with nginx,
# which proxies the JSON API routes to the FastAPI backend container.
#
#   docker compose build
#   docker compose up
#   open http://localhost:8080

# ---- Stage 1: build the Vite app -------------------------------------------
FROM node:20-alpine AS build

WORKDIR /app

# Install deps from the lockfile for reproducible builds.
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci

# Copy the source and build. Output lands in /app/dist.
COPY frontend/ ./
# apiBase (VITE_API_BASE_URL) defaults to "" so the SPA talks same-origin,
# and nginx proxies those API paths to the backend below.
RUN npm run build

# ---- Stage 2: serve with nginx ----------------------------------------------
FROM nginx:alpine

# nginx config proxies the API paths to the `api` service and hands every
# other route to the single-page app.
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf

# The built SPA.
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
