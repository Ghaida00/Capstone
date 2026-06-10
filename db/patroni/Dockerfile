# ============================================================
# Patroni-managed Postgres 18 node image
# ============================================================
#
# Base: the official postgres:18.3-bookworm image. We stay on
# Debian (not Alpine) because the Python wheel for the etcd3
# client (`python-etcd3` / `etcd3`) builds cleanly against
# glibc but is a pain on musl.
#
# What we add:
#   - Python 3 + pip (for Patroni itself)
#   - patroni[etcd3]       — the leader-election daemon that
#                            also manages the PG lifecycle
#   - psycopg              — Patroni's DB driver for talking
#                            to its own node
#   - gettext-base         — provides `envsubst`, used by
#                            patroni-entrypoint.sh to render
#                            the shard-specific config from
#                            a template at first boot
#
# The image is shared between ALL six Postgres nodes across
# the three shards. Per-node variance (hostname, shard scope,
# container name) is injected through environment variables
# and rendered into the Patroni YAML config at start-up.
#
# ============================================================
# === Why this looks nothing like the old pg_auto_failover ===
# Patroni is not a separate daemon that shells out to pg_ctl
# — it IS the process manager for PostgreSQL. One container,
# one ENTRYPOINT, one process tree: Patroni → (fork) Postgres.
# No socat sidecar, no monitor service: Patroni's built-in
# HTTP API on :8008 serves `GET /primary` and `GET /replica`
# with the same 200/503 semantics our HAProxy config already
# expects.
# ============================================================

FROM postgres:18.3-bookworm

# Install Patroni + etcd client + envsubst + pgBackRest.
# `--break-system-packages` is required on Debian Bookworm's
# Python 3.11+ (PEP 668) because we are installing into the
# system site-packages inside a dedicated container where a
# venv would only add noise. Safe here; not safe on a host.
#
# pgbackrest comes from the PGDG apt repo the base image already
# carries (2.58+; PG 18 support needs >= 2.56 — the Debian-main
# 2.45 package does NOT know PG 18). It is dormant unless
# BACKUP_ENABLED=true renders a config and the archive_command
# engages — when the toggle is off this costs image bytes only.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-setuptools \
        gettext-base \
        pgbackrest \
        ca-certificates; \
    pip3 install --break-system-packages --no-cache-dir \
        "patroni[etcd3]==4.1.0" \
        "psycopg[binary]>=3.2"; \
    apt-get purge -y --auto-remove; \
    rm -rf /var/lib/apt/lists/*

# Patroni needs a writable config dir. The base image's
# `postgres` user (UID 999) owns /var/lib/postgresql by
# default, so we nest our Patroni files under that tree.
# pgBackRest needs its config/log/repo dirs writable by the
# same user (archive_command runs as postgres). /var/lib/
# pgbackrest doubles as the default posix repo target for
# local testing; the s3 repo type never touches it.
RUN mkdir -p /var/lib/patroni \
             /etc/pgbackrest \
             /var/lib/pgbackrest \
             /var/log/pgbackrest \
 && chown -R postgres:postgres /var/lib/patroni \
                               /etc/pgbackrest \
                               /var/lib/pgbackrest \
                               /var/log/pgbackrest

COPY templates/patroni.yml.tmpl /etc/patroni/patroni.yml.tmpl
COPY patroni-entrypoint.sh      /opt/patroni-entrypoint.sh
COPY post-bootstrap.sh          /var/lib/patroni/post-bootstrap.sh
RUN chmod +x /opt/patroni-entrypoint.sh /var/lib/patroni/post-bootstrap.sh \
 && chown postgres:postgres /var/lib/patroni/post-bootstrap.sh

# The base image's ENTRYPOINT runs docker-entrypoint.sh which
# expects to manage PG itself. We bypass it completely —
# Patroni owns the PG process end-to-end.
#
# Entrypoint runs as root so it can `chown postgres:postgres
# $PGDATA` before starting Patroni. On Docker Desktop / WSL2
# a freshly-created named volume mounts with root-owned dir
# perms regardless of the image's mountpoint owner, which
# made initdb fail with "could not change permissions of
# directory ... Operation not permitted". The entrypoint
# drops to UID 999 via `gosu postgres` before exec'ing
# Patroni, so the running daemon is unprivileged.
ENTRYPOINT ["/opt/patroni-entrypoint.sh"]
