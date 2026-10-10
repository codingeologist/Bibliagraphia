# Bibliagraphia frontend: build the React/Vite app, then serve it with nginx,
# which proxies the JSON API routes to the FastAPI backend.
#
# SELF-CONTAINED: the build context is the `frontend/` directory itself
# (paths are relative to `./`). This makes it work identically with:
#   - local:    docker compose build               (context = ./frontend)
#   - Railway: service Root Directory = frontend   (context = frontend/)
#
# The backend upstream is injected at build time via the BACKEND_URL build arg:
#   - local:    BACKEND_URL=api:8000   (compose private network)
#   - Railway: BACKEND_URL=http://<api>.railway.internal:8000
#
#   docker compose up --build
#   open http://localhost:8080

# ---- Stage 1: build the Vite app -------------------------------------------
FROM node:20-alpine AS build

WORKDIR /app

# Install deps from the lockfile for reproducible builds.
COPY package.json package-lock.json ./
RUN npm ci

# Copy the source and build. Output lands in /app/dist.
COPY . ./
# apiBase (VITE_API_BASE_URL) defaults to "" so the SPA talks same-origin,
# and nginx proxies those API paths to the backend below.
RUN npm run build

# ---- Stage 2: serve with nginx ----------------------------------------------
FROM nginx:alpine

# The backend upstream target (host:port, NO scheme). Defaults to the compose
# service name for local use; override on Railway with the api service's
# private-networking host, e.g. BACKEND_URL=api.railway.internal:8000
# (set it in the frontend service's Variables tab — Railway passes service
# variables to matching Dockerfile ARGs at build time).
# A leading http:// is stripped defensively so both forms work.
ARG BACKEND_URL=api:8000

# nginx config proxies the API paths. Proxy headers are inlined because the
# alpine image has no /etc/nginx/proxy_params.
COPY nginx.conf /etc/nginx/conf.d/default.conf

# The built SPA.
COPY --from=build /app/dist /usr/share/nginx/html

# Rewrite the upstream placeholder before serving. ${BACKEND_URL#http://}
# strips any accidental scheme so the result is always a valid absolute URL.
RUN sed -i "s|http://api:8000|http://${BACKEND_URL#http://}|g" /etc/nginx/conf.d/default.conf

EXPOSE 80
