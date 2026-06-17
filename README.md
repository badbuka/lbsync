# lbsync

A distributed load-balancer coordination agent. One instance runs on every load
balancer; all instances form a single embedded [Olric](https://github.com/olric-data/olric)
cluster that acts as a shared state plane. On top of that cluster, lbsync runs
**modules**.

The first-class capability is a generic **newest-wins replication engine**:

- the **certs** module keeps every certbot lineage at its newest certificate
  across the fleet (full bundle: cert + private key + chain), then writes the
  winner to a serving directory and runs a verify-then-reload hook;
- the **blob** module does the same for arbitrary files (IP block lists, WAF
  rules, haproxy maps, config fragments).

The cluster also exposes primitives (atomic counters, pub/sub, distributed
locks) that future coordination modules will use; those modules
(`acme-leader`, `ratelimit`, `ocsp`, `pubsub`, `health`) ship as documented,
disabled stubs.

> The cert/discovery parsing logic is adapted from
> [letsencrypt-exporter](https://github.com/badbuka/letsencrypt-exporter)
> (MIT), extended locally to also read private keys.

## How it works

Each reconcile tick, for every resource the engine:

1. reads the node's local copy (e.g. certbot `live/<lineage>`);
2. reads the cluster's current copy from the Olric DMap;
3. publishes the local copy if it is strictly newer (guarded by a per-key lock);
4. applies the newest of the two locally (atomic write to the serving dir);
5. if anything changed, runs the module's verify-then-reload hook once.

If the verify command fails, the tick's writes are **rolled back** to the
previous on-disk copy and the reload is skipped, so the load balancer keeps
serving the last-known-good config. Because the renewing node re-publishes its
on-disk copy every tick, the newest copy always wins and the cluster can never
permanently lose a value that exists on some node.

"Newest" for certs is decided by `NotBefore` (then `NotAfter`, then serial, then
SHA-256). For blobs it is the source file mtime (or content hash with
`strategy=sha`).

## Configuration

Flags override environment variables, which override defaults.

| Env | Flag | Default | Purpose |
| --- | --- | --- | --- |
| `MODULES` | `-modules` | `certs` | comma-separated enabled modules (`certs`, `blob`, ...) |
| `LETSENCRYPT_PATH` | `-letsencrypt-path` | `/etc/letsencrypt` | certbot input root |
| `SERVING_DIR` | `-serving-dir` | `/etc/lbsync/live` | output dir the LB reads |
| `BLOB_RESOURCES` | `-blob-resources` | `` | blob resources `name=src:dest[:strategy],...` |
| `CLUSTER_PEERS` | `-peers` | `` | comma-separated `host:3322` of the other nodes |
| `ADVERTISE_ADDR` | `-advertise-addr` | `` | address peers use to reach this node |
| `CLUSTER_ENV` | `-cluster-env` | `lan` | memberlist env: `local`/`lan`/`wan` |
| `BIND_ADDR` | `-bind-addr` | `0.0.0.0` | bind address |
| `OLRIC_PORT` | `-olric-port` | `3320` | Olric TCP port |
| `MEMBERLIST_PORT` | `-memberlist-port` | `3322` | gossip port |
| `REPLICA_COUNT` | `-replica-count` | `0` | replicas; `0` = `len(peers)+1` (full replication) |
| `CLUSTER_PASSWORD` | `-cluster-password` | `` | Olric auth password (mandatory) |
| `CLUSTER_GOSSIP_KEY` | `-cluster-gossip-key` | `` | base64 16/24/32-byte memberlist encryption key |
| `INSECURE` | `-insecure` | `false` | allow running without auth (dev only) |
| `RECONCILE_INTERVAL` | `-interval` | `30s` | reconcile cadence |
| `VERIFY_CMD` | `-verify-cmd` | `` | config check before reload (e.g. `nginx -t`) |
| `RELOAD_CMD` | `-reload-cmd` | `` | reload after verify passes (e.g. `nginx -s reload`) |
| `RELOAD_TIMEOUT` | `-reload-timeout` | `30s` | verify/reload exec timeout |
| `PORT` | `-port` | `8623` | metrics/health HTTP port |
| `HOSTNAME` | `-hostname` | `os.Hostname()` | node id stored as the record source |

## Security

Private keys and arbitrary blobs travel over the cluster network, so the agent
**refuses to start** without `CLUSTER_PASSWORD` unless `-insecure` is set
(single-host development only). Also set `CLUSTER_GOSSIP_KEY` to encrypt the
memberlist gossip:

```bash
head -c 32 /dev/urandom | base64   # use as CLUSTER_GOSSIP_KEY on every node
```

Serving-directory private keys are written `0600`. Run under a dedicated
`lbsync` user/group with read-only access to `/etc/letsencrypt` and read/write
to the serving dir (see `deploy/lbsync.service`).

## Endpoints

- `GET /metrics` — Prometheus exposition (namespace `lbsync_`)
- `GET /healthz` — liveness
- `GET /readyz` — ready once the cluster has been joined

Key metrics: `lbsync_cluster_members`, `lbsync_module_enabled`,
`lbsync_applied_timestamp_seconds`, `lbsync_not_after_seconds`,
`lbsync_publish_total`, `lbsync_apply_total`, `lbsync_verify_errors_total`,
`lbsync_reload_errors_total`, `lbsync_rollback_total`,
`lbsync_reconcile_duration_seconds`.

## Deployment

A local 3-node cluster for testing:

```bash
docker compose -f deploy/docker-compose.yml up --build
```

For systemd, install the binary to `/usr/local/bin/lbsync`, copy
`deploy/lbsync.env.example` to `/etc/lbsync/lbsync.env`, and enable
`deploy/lbsync.service`.

Point your load balancer's TLS config at `SERVING_DIR/<lineage>/` instead of
certbot's `live/` directory.

## Development

Requires Go 1.26+ and [golangci-lint](https://golangci-lint.run/) v2.x.

```bash
make lint    # golangci-lint run
make test    # go test -race -count=1 ./...
make build   # produces bin/lbsync
make all     # lint + test + build
```

## License

MIT
