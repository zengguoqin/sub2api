#!/bin/sh
set -e

# Fix data directory permissions when running as root.
# Docker named volumes / host bind-mounts may be owned by root,
# preventing the non-root sub2api user from writing files.
if [ "$(id -u)" = "0" ]; then
    mkdir -p /app/data
    # Use || true to avoid failure on read-only mounted files (e.g. config.yaml:ro)
    chown -R sub2api:sub2api /app/data 2>/dev/null || true

    # Docker self-update cleanup: when this container is a replacement for an
    # older one, a background task (running as root so it can access the Docker
    # socket) waits until we are healthy, then removes the old container and
    # renames us to the original name so that Caddy/other services find us.
    if [ -n "$REPLACING_CONTAINER" ] && [ -e /var/run/docker.sock ]; then
        (
            port="${SERVER_PORT:-8080}"
            # Poll our own health endpoint until ready (up to 60 s).
            i=0
            while [ "$i" -lt 60 ]; do
                wget -q -O /dev/null "http://localhost:${port}/health" 2>/dev/null && break
                sleep 1
                i=$((i + 1))
            done
            # Extra buffer to let the app fully initialize.
            sleep 2

            # Stop the old container so its restart policy does not fire.
            curl -sf --unix-socket /var/run/docker.sock \
                -X POST "http://localhost/containers/${REPLACING_CONTAINER}/stop?t=30" \
                2>/dev/null || true
            sleep 1

            # Remove the stopped old container.
            curl -sf --unix-socket /var/run/docker.sock \
                -X DELETE "http://localhost/containers/${REPLACING_CONTAINER}" \
                2>/dev/null || true

            # Rename ourselves from the temp name to the target name.
            # hostname returns the short container ID inside Docker.
            MY_ID="$(hostname)"
            TARGET="${SELF_CONTAINER_NAME:-$REPLACING_CONTAINER}"
            curl -sf --unix-socket /var/run/docker.sock \
                -X POST "http://localhost/containers/${MY_ID}/rename?name=${TARGET}" \
                2>/dev/null || true
        ) &
    fi

    # Re-invoke this script as sub2api so the flag-detection below
    # also runs under the correct user.
    exec su-exec sub2api "$0" "$@"
fi

# Compatibility: if the first arg looks like a flag (e.g. --help),
# prepend the default binary so it behaves the same as the old
# ENTRYPOINT ["/app/sub2api"] style.
if [ "${1#-}" != "$1" ]; then
    set -- /app/sub2api "$@"
fi

exec "$@"
