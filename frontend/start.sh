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
# Railway: an IPv6 address such as fd12::10), so it is read from
# /etc/resolv.conf. nginx requires IPv6 resolver addresses in square
# brackets (resolver [fd12::10] valid=5s;), so colons trigger bracketing;
# the fallback matches Docker's embedded DNS.
set -e

DNS_RESOLVER="$(awk '/^nameserver/{print $2; exit}' /etc/resolv.conf)"
DNS_RESOLVER="${DNS_RESOLVER:-127.0.0.11}"

# Bracket IPv6 addresses (contain ":") for nginx's resolver directive.
case "$DNS_RESOLVER" in
  *:*) DNS_RESOLVER="[$DNS_RESOLVER]" ;;
esac
export DNS_RESOLVER

# Normalise the backend upstream scheme: BACKEND_URL may be given as
# host:port (private networking / compose, gets http://) or as a full URL
# with scheme (public Railway domain, keep https://).
case "${BACKEND_URL:-}" in
  http://*|https://*) ;;
  "") export BACKEND_URL="http://api:8000" ;;
  *) export BACKEND_URL="http://${BACKEND_URL}" ;;
esac

exec /docker-entrypoint.sh "$@"
