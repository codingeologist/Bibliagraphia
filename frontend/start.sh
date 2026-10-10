#!/bin/sh
# Bibliagraphia frontend container entrypoint wrapper.
#
# The official nginx image's entrypoint (/docker-entrypoint.sh) runs
# 20-envsubst-on-templates.sh BEFORE nginx starts, substituting ${BACKEND_URL}
# and ${DNS_RESOLVER} from the environment into the template at
# /etc/nginx/templates/default.conf.template. DNS_RESOLVER must therefore be
# exported BEFORE the original entrypoint runs — which is why this wrapper
# replaces the image ENTRYPOINT instead of doing the export inside CMD.
#
# The container's nameserver differs per platform (Docker: 127.0.0.11,
# Railway: its own resolver), so it is read from /etc/resolv.conf; the
# fallback matches Docker's embedded DNS.
set -e

DNS_RESOLVER="$(awk '/^nameserver/{print $2; exit}' /etc/resolv.conf)"
export DNS_RESOLVER="${DNS_RESOLVER:-127.0.0.11}"

exec /docker-entrypoint.sh "$@"
