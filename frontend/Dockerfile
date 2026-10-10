# Bibliagraphia frontend: build the React/Vite app, then serve it with nginx,
# which proxies the JSON API routes to the FastAPI backend.
#
# SELF-CONTAINED: the build context is the `frontend/` directory itself
# (paths are relative to `./`). This makes it work identically with:
#   - local:    docker compose build               (context = ./frontend)
#   - Railway:  service Root Directory = frontend  (context = frontend/)
#
# The backend upstream is set at RUNTIME via the BACKEND_URL env var.
# nginx resolves it lazily per request through the container's DNS resolver,
# so a briefly unavailable backend yields 502s that self-heal instead of a
# fatal startup crash. Accepts host:port (private network) or a full URL
# (public domain, https):
#   - local:    BACKEND_URL=api:8000   (compose service name, the default)
#   - Railway:  BACKEND_URL=api.railway.internal:8000 (private networking)
#               or https://<api-public-domain>       (public API)
#
#   docker compose up --build
#   open http://localhost:8080

# ---- Shared Node dependencies ---------------------------------------------
FROM node:20-alpine AS dependencies

WORKDIR /app

# Install deps from the lockfile for reproducible builds.
COPY package.json package-lock.json ./
RUN npm ci

# ---- Local development only (selected by docker-compose.dev.yml) -----------
FROM dependencies AS development
COPY . ./
EXPOSE 80
CMD ["npm", "run", "dev", "--", "--host", "0.0.0.0", "--port", "80", "--strictPort"]

# ---- Stage 1: build the Vite app -------------------------------------------
FROM dependencies AS build

# Copy the source and build. Output lands in /app/dist.
COPY . ./
# apiBase (VITE_API_BASE_URL) defaults to "" so the SPA talks same-origin,
# and nginx proxies those API paths to the backend below.
RUN npm run build

# ---- Stage 2: serve with nginx ----------------------------------------------
FROM nginx:alpine

# Backend upstream, substituted into the nginx template at container start.
# Two supported forms (start.sh normalises bare host:port to http://…):
#   - host:port            → private networking/compose, e.g. api:8000 or
#                            api.railway.internal:8000
#   - full URL with scheme → public domain, e.g.
#                            https://api-production-xxxx.up.railway.app
# This default matches the compose service name so local runs need no
# configuration.
ENV BACKEND_URL=api:8000

# nginx config template. The official image's entrypoint runs
# 20-envsubst-on-templates.sh, which substitutes ${BACKEND_URL} and
# ${DNS_RESOLVER} from the environment and writes
# /etc/nginx/conf.d/default.conf before nginx starts. Proxy headers stay
# inlined in the template because the alpine image has no
# /etc/nginx/proxy_params.
COPY nginx.conf /etc/nginx/templates/default.conf.template

# The built SPA.
COPY --from=build /app/dist /usr/share/nginx/html

# Entrypoint wrapper: exports DNS_RESOLVER (read from /etc/resolv.conf) so
# the official entrypoint's envsubst step can substitute it into the nginx
# template, then delegates to the original /docker-entrypoint.sh with the
# original CMD arguments. A wrapper is required because the envsubst step
# runs as part of the image ENTRYPOINT — an export inside CMD would run too
# late.
COPY start.sh /start.sh
RUN chmod +x /start.sh

ENTRYPOINT ["/start.sh"]
CMD ["nginx", "-g", "daemon off;"]

EXPOSE 80
