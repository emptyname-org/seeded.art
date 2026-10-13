# Deployment

No host, hostname, certificate or service has been changed. These commands are for a separately approved Linux target. Standard Caddy 2.11.7 or newer, systemd and service accounts are target prerequisites. Caddy obtains and renews certificates; this package installs no host software.

## Contract and boundaries

| Setting | Deployed value |
|---|---|
| `SEEDED_BIND` | `127.0.0.1:3918` |
| `SEEDED_CONTENT_BIND` | `127.0.0.1:3919` |
| `SEEDED_APP_ORIGIN` | `https://APP_HOST_PLACEHOLDER` |
| `SEEDED_CONTENT_ORIGIN` | `https://CONTENT_HOST_PLACEHOLDER` |
| `SEEDED_DATABASE` | `/var/lib/seeded/archive.sqlite` |
| `SEEDED_TRADING_DATABASE` | `/var/lib/seeded/trading.sqlite` |
| `SEEDED_CACHE` | `/var/lib/seeded/cache` |
| `SEEDED_MIRROR` | `/var/lib/seeded/mirror` (empty disables mirror reads) |
| `SEEDED_MIRROR_BYTES` | `53687091200` bytes (50 GiB; maximum 1 PiB) |
| `SEEDED_CONTENT_CONCURRENCY` | `4` assemblies (range 1–16) |
| `SEEDED_ASSETS` | `/opt/seeded/current/assets` |
| `SEEDED_PLATFORM_FEE_BPS` | `250` (2.5%; range 0–1000) |
| `SEEDED_PLATFORM_FEE_ADDRESS` | unset; disables accepts |

Fee recipients are tz1/tz2/tz3 or KT1 contracts. Before preparing and checking an accept to a KT1, the app verifies its default entrypoint accepts `unit` at a pinned mainnet block, then simulates the full operation group. The default entrypoint is a convention, not automatic; see https://octez.tezos.com/docs/active/michelson.html. tz4 is rejected. Cancels carry no platform fee; network fees still apply. Accept-fee collection depends on the seller signing the reviewed plain transfer: the external marketplace contract does not enforce it, and a modified signer can omit it. Client-initiated confirmation checks canonical applied operations and records finalized fee outcomes, including `platform_fee_evasion` when a bound acceptance completed without its reviewed payment. This `409` is terminal because acceptance already applied; do not retry or broadcast again. Checked accept bindings are recorded best effort in the private trading journal before signing. After 30 minutes, a background worker pages TzKT and verifies matching actor/counter/accept operations against canonical RPC blocks; unique operation hashes prevent duplicate evidence. Retries and scan cursors survive restart. Conflicting reviewed fee terms are recorded as ambiguous, never evasion. Reauthored operations with changed counters and accepts that bypass check are outside this audit; payment remains voluntary. The journal contains no session credentials.

The audit retains up to 16,384 active pending bindings (32 per address, 32 KiB each) for seven days in a 128 MiB SQLite database. A new binding replaces that address's oldest pending record at its limit. Expired work and unreferenced evidence are pruned by the background worker at most once per minute, not during signing or confirmation. Each minute it leases eight bindings, scans up to 100 indexer rows per binding and allows 30 seconds per attempt. Finalized consumed counters retire after their scan completes; unsubmitted bindings retry hourly and failures after five minutes. Competing reviews settle together. Outcomes retain at most 16,384 records for 30 days; unreferenced terms are removed. Full or unwritable audit storage logs `platform_fee_audit_record_unavailable` without blocking signing; evidence can therefore be incomplete. An audit open failure at startup logs `platform_fee_audit_unavailable_accepts_disabled` and disables accepts with the explicit `FeeAuditUnavailable` response, while the archive and cancels remain available. Back up `trading.sqlite` and monitor audit logs. Schema v1 upgrades to v2 automatically.

In combined mode one unprivileged process owns both loopback listeners; split mode runs one process per listener under the same account. Caddy runs as a separate account; only its service receives `CAP_NET_BIND_SERVICE`. Binaries/assets are root-owned and read-only. Database, WAL, cache, mirror and staging files belong to `seeded`, under its private state directory. Keep `/var/lib/seeded/mirror` private and never mount it as web content. The app reads verified objects there before cache or upstream lookup; its writer stores complete verified packages and never evicts them. The byte cap accounts stored objects by rounded logical size or allocated disk size, whichever is larger, and reserves space for packages in flight. A full cap preserves existing objects and rejects or defers further packages; it is not a filesystem quota, so allow space for staging and other state.

Production requires two HTTPS hosts. Prefer different registrable domains for cookie isolation; never set shared `Domain` cookies on either host. The configuration requires distinct hosts; it does not guess public suffix boundaries. Hostname assignment remains a separate decision. Content requests discard Cookie/Authorization, and responses discard Set-Cookie. Local development deliberately uses distinct ports on the same loopback host. Artwork still requires the application's sandbox/CSP boundary; a proxy alone does not isolate scripts.

App HTML/errors use `no-store`; successful static/fixture responses retain `public, max-age=0, must-revalidate`, including 304. There is no proxy cache. Content receives upstream immutable caching only for 200/304 responses reporting verified `onchfs-keccak`, `ipfs-car` or `ipfs-kubo` content. A CID-shaped path or a gateway assertion is insufficient. Derived `/render` HTML remains `no-store`, including verified sources. Other responses use `no-store`; upstream 503/Retry-After pass through. Proxy connection/timeout failures become 503 with Retry-After.

Playback notices attest only the main HTML file, using the server's per-instance `/playback-status/` response rather than artwork messages. Subresources carry separate response verification; the notice does not attest the whole artwork. These responses remain `no-store`. Late artwork errors keep the frame open with a reset notice.

## Prepare a release

Run from the checkout. Assign approved hostnames and measured limits in a local copy of the settings file; placeholders deliberately fail validation. Keep actual settings and TLS keys out of commits.

```sh
make test
make lint
make build
cp deploy/settings.example.json tmp/deployment-settings.json
# Edit tmp/deployment-settings.json after measurements and hostname approval.
python3 deploy/configure.py tmp/deployment-settings.json tmp/deployment-ready
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s deploy -p 'test_*.py'
```

The distribution includes `seeded_archive`, `seeded-index`, `seeded-sales` and `seeded-articles`. Packaging stages the executables before replacing any, then atomically replaces each file; open executable inodes remain intact. Build into `dist/`, then install a new read-only release. Packaging is not a transaction across the whole directory. `release-manifest.json` records sorted SHA-256 hashes for every packaged file except itself, plus source revision and dirty state. Missing HEAD is explicitly `unknown`/`uncommitted`; unavailable Git checks remain `unknown`, never clean. It contains no machine paths or timestamps. Verify its hashes against the installed release; generated files and unmanaged output entries are excluded or rejected.

Resource placeholders are mandatory. `SEEDED_TRUST_LOOPBACK_PROXY=true` requires exactly one client IP supplied by Caddy; missing or repeated forwarding headers fail closed. Exact GET/HEAD health probes may omit it. Leave proxy trust false for direct access. IPv4 uses individual addresses; IPv6 uses /64 keys. Each host has independent limits: 20 requests/second, burst 40, and 16 active responses per client. Each registry holds at most 8,192 entries and evicts the least recently used idle entry; active and queued requests are protected. No /48 admission budget applies. Auth, edits and trading have additional limits.

Content GETs under `/content/ipfs/` and `/content/onchfs/` have a separate 256-request burst at the same refill rate. They wait up to ten seconds for admission and delivery capacity, before content allocation: at most 256 waiting requests per client and 2,048 globally. Active and byte limits stay unchanged. Cancellation and expiry release queue slots; queued clients cannot be evicted. Content-host 429/503 responses include `Access-Control-Allow-Origin: *` and `Retry-After`.

Caddy streams without full-response buffering or shared upstream request caps. Rust separates assembly from delivery. Verified memory, mirror and cache hits bypass upstream assembly slots. Local reads have 16 worker slots and a separate scratch budget of at least 512 MiB (increased when the configured object cap needs more decode space). Missing local objects reserve no body memory. Probes bound decode space before reading; reconstructed bodies reserve their actual size in a separate 512 MiB delivery budget. Readers of the same retained allocation share one charge until its final clone drops, even after eviction. Reservations are all-or-nothing; after 250 ms, new reservations leave headroom for the oldest waiter. Already charged shared bodies bypass this wait. Local reads have a two-second budget for admission waits only (reader, scratch, allocation leader and delivery); disk probing, reads and copies are outside it. Same-key followers share one successful lookup, including misses and bodies too large for the LRU. Their separate queue permits 256 waiters for up to five seconds without spending the two-second admission budget or holding reader/scratch slots. The existing cohort shares successful bodies, misses and transient busy errors. A cancelled, panicked or I/O-failed leader lets one follower at a time retry; later arrivals start a fresh generation. Completed outcomes are shared only with that generation’s existing followers. At most 272 generations exist, including completed results held by followers; shared bodies retain their delivery charge until the last clone drops. The independent 40-second content-handler deadline still applies. Reader, scratch and delivery wait queues each permit 256 waiters. Small mirror leaves use a direct bounded read, and disk-cache CAR probes inspect at most 1 MiB to size their scratch reservation. `/health/metrics` exposes `local_delivery`, `local_scratch`, and `artwork_admission` rejection counters; the monitor records and reports these groups and `local_reads`, `resolution`, `request_limits` and `database_admission`. A 256 MiB/8,192-entry LRU retains compact verified bytes, so repeated reads require neither disk I/O nor hashing. Its copies run on blocking workers outside the cache mutex. Upstream retention uses at most two copy workers and the shared scratch budget; if either is full, the resolved response succeeds without memory-cache retention. Detached copies keep their reservations until completion. Mirror reads trust write-time verification, including first reads and after restart; temporary-cache rereads verify again after LRU eviction or restart. Unverified responses never enter this cache. Local decompression/preparation has 16 slots, independent of upstream work. Artwork metadata uses one eight-slot pool with up to 512 waiters for ten seconds. Its read-only connections are reused, but each lookup starts a fresh transaction and checks that the database file still exists and has not been replaced. A worker keeps its slot after request cancellation or its five-second query timeout until blocking work ends. Known content roots are held in memory as 16-byte digests (about 60 MB for 3.9 million roots), so their artwork needs no database lookup; unknown roots still ask the database. After any commit to the archive (SQLite `data_version`, about 3 µs per lookup) the set is unused until a fingerprint check of the snapshot row and hidden-project list confirms it, so hiding a project applies on the next request; otherwise the first request ten seconds after the last check revalidates it. A commit during a reload leaves artwork lookups on the database until that reload ends (a few seconds, during each sync). A hidden-list change withdraws the set until it is reloaded; a snapshot-only change (syncs only add roots) keeps serving and reconfirms it during the reload. A set unconfirmed for 30 seconds is not used, and every lookup checks the archive file: a missing or replaced file stops the set at once and reloads it in full. To remove artwork, hide its project: deleting roots or projects directly is noticed only at the next full reload or restart. Page reads have their own eight-slot pool with up to 512 waiters for two seconds. Their read-only connections are reused until the archive or sales file is replaced; each read re-checks the sales attachment and retries a failed one.

Completed artwork reserves its byte length in 4 KiB units until the last response buffer drops. Unchanged local bodies already charged by the content pool reserve only per-client web capacity; upstream, decompressed and copied range bodies also reserve global web capacity. Fixed error bodies bypass artwork byte accounting and keep their original status. Default delivery bounds are 512 MiB globally and 64 MiB per client. For configured objects above 63 MiB, the client budget grows to the object cap plus 1 MiB, with a global budget eight times larger. Each client also has at most `min(4, SEEDED_CONTENT_CONCURRENCY)` artwork deliveries; no /48 shares these slots. Up to 16 assembled responses can wait two seconds, sharing a pending-byte budget of one configured object plus 1 MiB. Pending capacity transfers to active capacity without an uncharged wait. Upstream assembly slots are released once completed bytes are charged. After 250 ms, queued global-byte requests use FIFO admission to preserve headroom for older large bodies. Pending retention remains first-come and can temporarily hold one full-size body; refusal also triggers a throttled reclaim attempt. Include this extra envelope in memory sizing.

Under byte pressure, proxy connections can be cancelled after one second without 64 KiB progress, or when their remaining body bytes cannot finish at their rate over the last five seconds (or since the response started, if shorter) before the earlier of the connection deadline and acceptance plus Caddy’s 180-second write limit. Time already spent at the edge can make the real limit earlier. Selection favours the heaviest /48, then /64, without banning neighbouring addresses. Shared allocations are eligible only when every registered owner is stalled or projected late. All scan attempts, including those finding no victim, share a 250 ms throttle. Each admission allows four cancellation batches over two seconds; each batch has at most eight owners. Cancellation wakes blocked transport I/O; charges remain until actual buffers, including final Hyper frames, drop. Direct or multiplexed connections retain their ordinary bounds. A rare allocator-address reuse during teardown can conservatively return 503; retry is safe. Arbitrary distributed exhaustion remains possible. Allow memory for the local LRU, read/decoding workers and retained delivery bytes in addition to upstream assemblies.

Both HTTPS and explicit HTTP redirect servers bound headers to 128 KiB/five seconds, request bodies to ten seconds, writes to 180 seconds absolute and 30 seconds idle, and idle connections to 15 seconds. The absolute write budget includes upstream waiting. Declared oversized app/content bodies fail before proxying: 32 KiB for API calls, 4 KiB for POST `/oauth/token`, 1 KiB otherwise. Chunked bodies have the same read caps, but Caddy may forward a prefix before returning 413. Rust parses complete bodies before mutation, under 16 KiB auth / 24 KiB trading / 4 KiB OAuth limits; native tests exercise proxy boundaries.

Upstream keep-alive is disabled. Each Rust listener allows 1,024 connections with a 240-second lifetime. On these single-request proxy connections, delivery must advance by 64 KiB within each 15-second window once writing starts; stalled and trickling writes terminate and release buffers. Direct development connections retain only the lifetime bound. Proxy dialing takes at most two seconds; headers/reads 65 seconds; writes ten seconds. Content handling has a 40-second deadline after admission. Caddy drains for 45 seconds on SIGTERM; systemd allows 90 seconds.

The proxy has 128 tasks. `PROXY_OPEN_FILES` defaults to 16,384 (maximum 65,536). Memory must cover 64 KiB per descriptor plus 256 MiB: at least 1,280 MiB by default or 4,352 MiB at the maximum. This is a sizing guard, not a measured attack ceiling; measure a larger profile before deployment. App and proxy failures retry every five seconds without permanent start-rate lockout. Target unit validation remains required; local cgroup evidence is in [hardware estimates](hardware.md).

Access logs are disabled. A separate DEBUG logger includes only `http.log.error`, including handled upstream failures; its encoder deletes the complete request object. Other operational logs retain WARN and above. The proxy conflicts with stock `caddy.service` and denies writable executable memory. These settings require startup validation on the approved target.

Optional `MIRROR` and `MIRROR_BYTES` settings override the mirror path and byte cap in the rendered environment. Optional `MAX_OBJECT_BYTES` (default 33554432, at most 268435456) sets the largest single artwork file the app serves and the mirror stores.

Optional `CONTENT_CONCURRENCY` sets the artwork assembly limit from 1 to 16; it defaults to 4.

Optional `CONTENT_HOST_REDIRECTS` takes space-separated hostnames, distinct from the other hosts. Each answers every request with a 302 to the same path and query on `CONTENT_HOST` (plain HTTP first upgrades on the same host) and needs DNS and a certificate like the other hosts.

Optional `FORUM_HOST`, `FORUM_ROOT` and `FORUM_PHP_SOCKET` must be set together. They add a distinct forum vhost and HTTP redirect, PHP-FPM forwarding, and denies for `/vendor`, `/storage`, `/composer.*`, `/config.php`, `/flarum`, `/site.php` and `/extend.php`. Paths must be absolute and outside the service's hidden home/tmp mounts. The proxy gains `www-data` socket access and an explicit read-only forum root; its existing hardening stays enabled. Ensure directory traversal/read permissions for `seeded-proxy`/`www-data` and an accessible PHP-FPM socket before starting. The forum has its own PHP routes; app/content methods and limits stay separate. PHP-FPM reference: https://caddyserver.com/docs/caddyfile/directives/php_fastcgi

Optional `OAUTH_CLIENT_ID`, `OAUTH_CLIENT_SECRET` and `OAUTH_REDIRECT_URI` must be set together. They render as `SEEDED_OAUTH_*` only in the mode-0600 `seeded.env`; absent settings disable OAuth. Keep the input settings file mode 0600 too. The redirect must exactly match the forum callback; use HTTPS. The forum side is the Flarum extension in `deploy/forum/`: copy `extend.php` and `seeded/` into the Flarum root (owner root, mode 0644), then run `sudo -u www-data php flarum cache:clear` there. Its callback answers JSON to `Accept: application/json` for agents (`docs/agents.md`). The extension also emits the description, Open Graph tags and discussion structured data. Forum settings for agents: Members start and reply without approval; `allow_post_editing` is `-1` (own posts stay editable). Secrets are quoted literally and never printed by the renderer. POST `/oauth/token` alone accepts the OAuth exchange; other OAuth routes remain GET/HEAD, and the artwork host accepts no POSTs.

Optional `JOB_MEMORY_MAX`, `JOB_CPU_QUOTA` and `JOB_TASKS_MAX` settings control maintenance jobs; otherwise they inherit the app limits. Each job has a separate cgroup: app, job and proxy memory budgets are additive. [Hardware estimates](hardware.md) include an allowance for indexing and backups.

## Automatic HTTPS

Both approved hostnames need public DNS A/AAAA records pointing to the target; published IPv6 must work too. Permit inbound TCP 80 and 443, including any NAT forwarding, and outbound DNS plus HTTPS to the ACME issuer. The issuer is pinned to Let’s Encrypt; CAA records must permit `letsencrypt.org`. Optional `ACME_EMAIL` must be a validated contact address; blank omits contact email. The stock build uses HTTP/TLS validation; wildcard/DNS challenges would require a separate provider module and credentials. HTTP/3 is disabled, so UDP 443 is not required.

### Crawlers and IndexNow

Caddy serves explicit search, training/dataset and requested-fetch groups in
`robots.txt`. Access is unchanged: app exclusions are repeated in every group;
the forum allows crawling; artwork-host exclusions stay separate. These are
crawler preferences, not access controls. Google and Bing search-console
integrations are excluded; no account setup is required.

Optional deployment settings (empty disables the verification route):

| Setting | Public response |
|---|---|
| `INDEXNOW_KEY` | App `/<key>.txt`; 8–128 letters, digits or hyphens |
| `FORUM_INDEXNOW_KEY` | Forum `/<key>.txt` |

IndexNow requires no search-console account. Generate a separate random key per
host, render it, and verify its public route before submitting. Put reviewed,
canonical public page URLs in a newline-separated file, at most 100 per batch:

```sh
python3 deploy/indexnow.py --site https://APP_HOST --key-file KEY_FILE --urls-file URLS_FILE
# The first command previews the exact outgoing JSON; add --submit to send it.
```

Use `--forum` for forum URLs. Submission checks the hosted key and anonymous page
responses; it refuses redirects, foreign hosts, queries, private routes and
`noindex` responses. It sends only URLs and the public verification key to
https://api.indexnow.org/indexnow. It neither exports database contents nor
crawls the catalogue. Run for published changes; there is no automatic bulk
resubmission or new timer. Exit 75 means retry later with the same file; inputs
are never deleted. HTTP 200/202 acknowledges receipt, not indexing.

Protocol: https://www.indexnow.org/documentation. Bot definitions:
https://developers.openai.com/api/docs/bots,
https://support.claude.com/en/articles/8896518-does-anthropic-crawl-data-from-the-web-and-how-can-site-owners-block-the-crawler,
https://docs.perplexity.ai/docs/resources/perplexity-crawlers,
https://commoncrawl.org/ccbot.

Caddy handles HTTP redirects, certificate issuance and renewal. Its account keys and certificates live under `/var/lib/seeded-proxy`, mode 0700, independently of the archive. Keep this directory persistent and include it in the target's private recovery policy; it is never web content. ACME operations disclose the approved hostnames to the issuer and certificate transparency logs. Test issuer staging before repeated public issuance. Reference: https://caddyserver.com/docs/automatic-https

The admin API and configuration persistence are disabled. Port 2019 is not opened; configuration changes require a validated service restart. No `caddy reload` or `--resume` workflow is provided. References: https://caddyserver.com/docs/caddyfile/options#admin and https://caddyserver.com/docs/running

## Clean target installation

Replace `RELEASE_ID` with the reviewed release revision after confirming the target is clean, stock Caddy is inactive, TCP ports 80/443 and 3918/3919 plus UDP 443 are free, and destination filesystems have room for the install and backup reserve. Do not replace an existing proxy without a separate integration decision. These administrative commands have not been run.

```sh
sudo useradd --system --user-group --home-dir /var/lib/seeded --shell /usr/sbin/nologin seeded
sudo useradd --system --user-group --home-dir /var/lib/seeded-proxy --shell /usr/sbin/nologin seeded-proxy
sudo useradd --system --user-group --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin seeded-monitor
sudo install -d -o root -g root -m 0755 /opt/seeded /opt/seeded/releases /opt/seeded/releases/RELEASE_ID
sudo cp -a dist/. /opt/seeded/releases/RELEASE_ID/
sudo chown -R root:root /opt/seeded/releases/RELEASE_ID
sudo chmod -R u=rwX,go=rX /opt/seeded/releases/RELEASE_ID
sudo chmod -R a-w /opt/seeded/releases/RELEASE_ID
sudo ln -s releases/RELEASE_ID /opt/seeded/current
sudo install -d -o seeded -g seeded -m 0700 /var/lib/seeded /var/lib/seeded/backups /var/lib/seeded/cache /var/lib/seeded/mirror /var/lib/seeded/tmp
sudo install -d -o root -g root -m 0755 /etc/seeded
sudo install -d -o seeded-proxy -g seeded-proxy -m 0700 /var/lib/seeded-proxy
sudo install -o root -g seeded -m 0640 tmp/deployment-ready/seeded.env /etc/seeded/seeded.env
sudo install -o root -g seeded-proxy -m 0640 tmp/deployment-ready/Caddyfile /etc/seeded/Caddyfile
sudo install -o root -g root -m 0644 tmp/deployment-ready/seeded.service tmp/deployment-ready/seeded-proxy.service /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/seeded.service /etc/systemd/system/seeded-proxy.service
sudo systemctl daemon-reload
```

After rollback, keep the `seeded` accounts and `/var/lib/seeded*` state. To reinstall, repeat release, config and unit installation/validation; skip account creation, state-directory creation and database installation so existing databases and backups remain intact.

Populate `/var/lib/seeded/archive.sqlite` using the indexing or checked-backup procedure below before starting the app. Production `serve` refuses a missing or empty database; only explicit development startup imports demo fixtures.

To install a completed mirror, stop its writer, copy its `ipfs/`, `onchfs/` and `status.sqlite` into `/var/lib/seeded/mirror`, and set ownership to `seeded:seeded`, directories to mode 0700 and files to 0600. Do not copy `tmp/`; it contains in-flight staging. The `seeded` process needs directory traversal and read access to every object.

```sh
sudo systemctl start seeded.service
curl --fail http://127.0.0.1:3918/health/live
curl --fail http://127.0.0.1:3918/health/ready
sudo systemctl start seeded-proxy.service
```

`seeded-proxy.service` runs `caddy validate` before opening listeners. It runs independently of any distribution `caddy.service`; do not enable both on ports 80/443. Do not enable either unit until the acceptance gates below pass. Enablement is a separate explicit host action: `sudo systemctl enable seeded.service seeded-proxy.service`.

## Split app and artwork processes

`seeded-app.service` (`serve --role app`) and `seeded-content.service` (`serve --role content`) run the two listeners as separate processes, so artwork load or failure cannot stop the app. Budgets: `APP_*` for the app; optional `CONTENT_MEMORY_HIGH`, `CONTENT_MEMORY_MAX`, `CONTENT_CPU_QUOTA`, `CONTENT_TASKS_MAX`, `CONTENT_OPEN_FILES` for artwork, each defaulting to its `APP_*` value. Ports, proxy upstreams and `seeded.env` are unchanged. Both conflict with `seeded.service`, which remains for the combined model. Only the artwork process opens the cache; only the app migrates the archive and writes `trading.sqlite`. The app's `/health/ready` then no longer checks the cache. Use both units wherever this guide names `seeded.service`.

Migrate:

```sh
sudo install -o root -g root -m 0644 tmp/deployment-ready/seeded-app.service tmp/deployment-ready/seeded-content.service tmp/deployment-ready/seeded-proxy.service /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/seeded-app.service /etc/systemd/system/seeded-content.service /etc/systemd/system/seeded-proxy.service
sudo systemctl daemon-reload
sudo systemctl stop seeded.service
sudo systemctl start seeded-app.service seeded-content.service
curl --fail http://127.0.0.1:3918/health/ready
curl --fail http://127.0.0.1:3919/health/metrics
curl --fail -o /dev/null -H 'Host: <artwork host>' http://127.0.0.1:3919/content/ipfs/<mirrored CID>/
```

Then move enablement: `sudo systemctl disable seeded.service` and `sudo systemctl enable seeded-app.service seeded-content.service`. Roll back with `sudo systemctl start seeded.service`; it stops both split units.

## Populate and synchronize

Use an approved local fxhashArchive checkout readable by `seeded`; its catalogue data is not bundled in the release. These explicit commands bootstrap metadata, fetch chain records, verify a sample and report coverage. The indexer requires `--db`; it does not take its database path from `SEEDED_DATABASE`.

```sh
sudo -u seeded /opt/seeded/current/seeded-index bootstrap --frank /PATH/TO/fxhashArchive --db /var/lib/seeded/archive.sqlite
sudo -u seeded /opt/seeded/current/seeded-index sync --db /var/lib/seeded/archive.sqlite
sudo -u seeded /opt/seeded/current/seeded-index escrow --db /var/lib/seeded/archive.sqlite
sudo -u seeded /opt/seeded/current/seeded-index verify --db /var/lib/seeded/archive.sqlite --sample 100
sudo -u seeded /opt/seeded/current/seeded-index stats --db /var/lib/seeded/archive.sqlite
```

Alternatively, create the deployed database from a checked online backup using the restore procedure. Preserve schema compatibility. Never run `import-demo` against the indexed archive.

For ongoing updates, rerun `sync`, `escrow`, then `verify` and `stats`. Upgrade a v2 database with the app stopped: retain a checked backup, run `seeded-index upgrade --db PATH`, then `escrow`, and start the new release. Upgrade rebuilds local derived records without a token rescan. The first sync performs a resumable full pass; later runs resume pending work or run incremental passes. Run one indexer writer at a time. WAL permits concurrent web readers. Repeated `--contract KT1…` arguments select contracts. `SEEDED_SYNC_BATCH=500`, `SEEDED_TZKT_CONCURRENCY=4` and `SEEDED_TZKT_TIMEOUT_SECS=30` are CLI defaults; manual invocations do not inherit the web service's environment file. Provide approved overrides explicitly with `env`.

`0` means completion or a requested limit; `75` means retryable failure, `130` interruption, `64` bad usage and `1` another failure. Inspect pending contracts, issues, verification mismatches and stale rows before accepting coverage. First SIGINT/SIGTERM requests cooperative cancellation; sync keeps committed batch cursors. Bootstrap and local verification can continue their current database work before observing cancellation. A second signal exits immediately; open SQLite transactions roll back.

Schema-v10 upgrade is required before starting the new release. For an existing v9 database, stop the app, retain a checked backup, run the new binary's `seeded-index upgrade --db PATH`, then start the release. Upgrade, import and sync assign display names automatically; `seeded-index names` is manual only. For an existing schema-v3 database, also run `seeded-index reconcile --db PATH` after upgrade to queue omitted catalogue and boundary records without a full chain rescan. The next `sync` fetches queued records. Cursor pagination reaches the full catalogue. Metadata verification does not prove every artifact is mirrored or renderable.

## Optional scheduled maintenance

The rendered `seeded-{sync,verify,backup,sales,articles,profiles}.{service,timer}` units and `seeded-sync.path` are disabled until explicitly installed and enabled on the approved target. They use `/usr/bin/python3 /opt/seeded/current/deploy/maintain.py JOB`, the same environment file, and the private `/var/lib/seeded/archive.sqlite` database.

| Job | Schedule | Work / bound |
|---|---|---|
| `sync` | Every 10 minutes, at :05, :15, …, and after each confirmed trade (`seeded-sync.path` watches `/var/lib/seeded/sync.request`, which the app writes) | Sync, escrow and offers; eight-minute total budget with reserved time for each. Exit 75 (writer busy or budget reached) is not a failure; the next run resumes. |
| `sales` | Daily, 00:30 | Incremental sales sync followed by volume refresh; ten-minute total budget. |
| `articles` | Mondays, 00:50 | Articles, project references and displayed names; five-minute budget. |
| `profiles` | Before verification | Refresh objkt profiles (fxhash profiles are frozen); forty-minute budget. Required by verify. |
| `verify` | Mondays, 02:00 target time | Verify 500 samples; thirty-minute budget. Requires successful profiles. |
| `backup` | Daily, 03:00 target time | Back up each existing database, verify integrity, retain two copies per database; ten minutes per database. Independent of profiles/verify success. |

Enable `seeded-verify.timer` and `seeded-backup.timer` independently. Verify starts profiles first; backup never starts or requires either job. `After=` orders jobs only when queued together; randomized catch-up timers can start backup first. Its maintenance lock can exhaust that day's profiles/verify retries. The profiles timer remains available for standalone refreshes. One inherited `maintenance.lock` serializes archive jobs. Lock contention and transient failures return 75. Sync retries next tick; daily jobs retry after five minutes, at most four starts in six hours. Profiles and verify use `RestartMode=direct` (systemd 254+) so transient retries do not fail queued dependents. Interruption returns 130. Check failed units and journal output; exhausted retries require intervention. Runners terminate and reap timed-out children. Stop all enabled timers and their services before manual bootstrap, migration, reconciliation or restoration: bare CLI commands do not acquire the maintenance lock.

Backups publish only after validation. Each database gets an independent deadline, and a failed source does not skip the others. An unfinished run records exact successful filenames in a private retry journal. Same-day retries revalidate those copies; invalid copies are quarantined and replaced. A completed run clears the journal, so another run always takes fresh copies. Manual CLI backups remain unrestricted. Retention touches `scheduled-*.sqlite` files matching the runner's exact format; manual backups remain untouched. A crash between linking a completed backup and removing its pending name is recovered by matching inode and revalidating SQLite. Other pending outputs are preserved for inspection; eight staged outputs stop further scheduled backups. Never remove `maintenance.lock` while a job is active.

Before enabling backup timers, reserve space for three copies of every existing database and its WAL (two retained plus one being published) and 1 GiB headroom. Include `trading.sqlite` after it is created, and repeat the check after database growth. Timer enablement is separate from local-only acceptance.

Install the twelve generated archive maintenance unit files with owner root, mode 0644, validate them with `systemd-analyze verify`, then reload systemd. After manual runs pass, enable independent sync/sales/articles timers as needed, plus verify and backup timers. Existing installations that enabled only backup must also enable verify to retain weekly profile refreshes. No workstation timers are installed by this project or its tests.

### Forum database backups

With `FORUM_*` configured, rendering adds `seeded-forum-backup.{service,timer}`. It dumps `flarum` daily at 03:30, retaining seven complete SQL files in `/var/lib/seeded-forum-backup`, outside the archive account's state directory. It does not depend on archive jobs. Failed, interrupted or oversized dumps do not replace or prune completed copies. The runner allows ten minutes and 1 GiB per dump; reserve eight times that bound before enabling it. Files are private (0600, directory 0700); SQL and database errors are not logged. This backs up the database only: application accounts/grants, `config.php`, extensions and uploaded avatars need separate copies.

Use a dedicated OS/MariaDB account with Unix-socket authentication, not the forum's application password. On MariaDB 11.3+, these schema-scoped grants include table data, views, triggers, events and routine definitions. `TRIGGER` and `EVENT` also permit changing those objects; the account has no shell, global grants or network login. Confirm that all forum tables use InnoDB and avoid schema migrations during dumps. Sources: https://mariadb.com/docs/server/reference/plugins/authentication-plugins/authentication-plugin-unix-socket and https://mariadb.com/docs/server/clients-and-utilities/backup-restore-and-import-clients/mariadb-dump

On the approved target, create the account once:

```sh
sudo useradd --system --user-group --home-dir /nonexistent --no-create-home --shell /usr/sbin/nologin seeded-forum-backup
sudo mariadb --execute="CREATE USER 'seeded-forum-backup'@'localhost' IDENTIFIED VIA unix_socket;"
sudo mariadb --execute="GRANT SELECT, SHOW VIEW, TRIGGER, EVENT, SHOW CREATE ROUTINE ON flarum.* TO 'seeded-forum-backup'@'localhost';"
```

Systemd creates the separate state directory, owned by `seeded-forum-backup`, mode 0700. The account joins neither `seeded` nor `www-data`. It reads only the local MariaDB socket and writes its backup directory; no app environment or forum configuration is loaded. Include this directory in off-host backup sync.

Install the two rendered units as root-owned 0644 files, validate them, reload systemd, then run `seeded-forum-backup.service` manually. Before enabling its timer, restore the first dump on a disposable MariaDB instance with `--skip-networking --event-scheduler=OFF`; stored SQL objects may reference the original schema. Substitute its socket and the actual dump filename below. Compare table and key row counts with the source.

```sh
sudo mariadb --no-defaults --socket=/path/to/isolated.sock --execute="CREATE DATABASE flarum CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
sudo mariadb --no-defaults --socket=/path/to/isolated.sock flarum --execute="source /var/lib/seeded-forum-backup/FILE"
sudo mariadb --no-defaults --socket=/path/to/isolated.sock --execute="SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA='flarum';"
```

If the generated dump has no triggers, events or routines, a scratch database on the existing server is sufficient. The following check must print `0` (grep exits 1); otherwise use the disposable instance above:

```sh
sudo grep -c -E '^(/\*!50003 CREATE\*/|/\*!50106 CREATE\*/|CREATE DEFINER=)' /var/lib/seeded-forum-backup/FILE
sudo mariadb --execute="CREATE DATABASE flarum_restore_check CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
sudo mariadb flarum_restore_check --execute="source /var/lib/seeded-forum-backup/FILE"
sudo mariadb --execute="SELECT TABLE_SCHEMA,COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('flarum','flarum_restore_check') GROUP BY TABLE_SCHEMA;"
```

Compare key row counts too, then remove the scratch database, including after a failed check:

```sh
sudo mariadb --execute="DROP DATABASE flarum_restore_check;"
```

For a real restore, select a verified dump and a new `RESTORE_ID` directory; stop on any failed command. Preserve the selected file outside retention before taking a fresh pre-restore dump. Confirm the original database charset/collation and existing application grants:

```sh
sudo systemctl stop php8.4-fpm.service seeded-forum-backup.timer seeded-forum-backup.service
sudo mkdir -m 0700 /var/lib/seeded-forum-restore-RESTORE_ID
sudo cp /var/lib/seeded-forum-backup/FILE /var/lib/seeded-forum-restore-RESTORE_ID/selected.sql
sudo mariadb-dump --no-defaults --protocol=socket --single-transaction --quick --skip-lock-tables --no-tablespaces --routines --events --triggers --result-file=/var/lib/seeded-forum-restore-RESTORE_ID/current.sql flarum
```

Check that the fresh dump completed successfully before replacing the database:

```sh
sudo mariadb --execute="DROP DATABASE flarum; CREATE DATABASE flarum CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
sudo mariadb flarum --execute="source /var/lib/seeded-forum-restore-RESTORE_ID/selected.sql"
```

After checking tables, rows and application grants, restart PHP and the timer. Stop this timer during forum migrations; reset its failed state between repeated acceptance runs. Undo scheduling by disabling the timer; retain completed dumps. These same-disk copies do not replace off-host backups.

## Resource monitor

`seeded-monitor.service` runs `deploy/monitor.py` (stdlib only, user `seeded-monitor`, not `DynamicUser`, which cannot query systemd over D-Bus without NSS `systemd`; state in `/var/lib/seeded-monitor`). Every 15 s it samples `127.0.0.1:3918` and `:3919` `/health/metrics` and the `systemctl show` properties of the seeded, PHP and MariaDB units. It writes one aggregate JSON line per minute to `metrics-YYYYMMDD.jsonl` (0600, 14 days, 256 MiB cap) and logs `seeded-monitor alert:` warnings to the journal (5xx/503 share, p95, endpoint down, memory less inactive page cache near `MemoryHigh`, disk, inactive enabled unit, failed maintenance or forum-backup job; once per kind per 15 min). It stores aggregates only. Enable it like the other units. Summarize a load test:

```sh
python3 deploy/monitor.py report --state /var/lib/seeded-monitor --since 2026-01-01T12:00:00Z --until 2026-01-01T12:30:00Z
```

## Usage statistics

GoatCounter (pinned release binary, checksum-verified) runs as `seeded-stats` on 127.0.0.1:3920 with `-base-path /stats`; Caddy proxies `/stats` and `/stats/*` on the app host to it. Pages load `/stats/count.js`, which reports page views to `/stats/count`; crawlers that run no script are not counted. It stores no IP addresses and sets no cookies on the site. Dashboard: `https://APP_HOST/stats/` (login created with `goatcounter db create site -vhost APP_HOST`). Install: static user `seeded-stats`, binary in `/opt/seeded-stats/` (`goatcounter` symlink to the versioned file), the rendered `seeded-stats.service`, then `systemctl enable --now seeded-stats`.

## Restart, backup and restore

For a reviewed configuration or release, restart and require readiness. Validate proxy changes before restart:

```sh
sudo systemctl restart seeded.service
curl --fail http://127.0.0.1:3918/health/ready
sudo -u seeded-proxy env XDG_DATA_HOME=/var/lib/seeded-proxy XDG_CONFIG_HOME=/var/lib/seeded-proxy/config /usr/bin/caddy validate --config /etc/seeded/Caddyfile --adapter caddyfile
sudo systemctl restart seeded-proxy.service
```

For a new release, first install it read-only as above and retain the previous release. Replace `NEXT_RELEASE_ID`; the temporary symlink must not already exist. Switch atomically, restart and check readiness:

```sh
sudo ln -s releases/NEXT_RELEASE_ID /opt/seeded/next
sudo mv -T /opt/seeded/next /opt/seeded/current
sudo systemctl restart seeded.service
curl --fail http://127.0.0.1:3918/health/ready
```

Repeat with the previous release ID to roll back code. Schema rollback requires a compatible backup, not merely an older executable.

Back up online with a new, unique destination. The CLI uses SQLite's online backup API with a stable source read transaction, so concurrent WAL commits cannot restart its copy. It checks the result with `quick_check`, syncs it, and publishes without replacing an existing file or symlink. Files are mode 0600. The default deadline is 300 seconds; `SEEDED_BACKUP_TIMEOUT_SECS` accepts 1–3600 seconds. Cancellation/deadline cleans unpublished staging. An operating-system filesystem stall cannot be preempted by the synchronous API. A failure explicitly reporting an already-published backup requires retaining and checking that destination.

```sh
sudo -u seeded env SEEDED_DATABASE=/var/lib/seeded/archive.sqlite /opt/seeded/current/seeded_archive backup /var/lib/seeded/backups/BACKUP_ID.sqlite
```

Backups sharing a directory serialize through its persistent `.seeded-backup.lock`; never remove that file while the directory is in use. The next backup removes orphan `.seeded-backup-v2-PID-N.sqlite` files under this lock, scanning at most 4,096 entries. Published backups, links, directories and legacy staging remain intact. Source and destination names cannot use the reserved staging/lock prefixes. Legacy `.seeded-backup-PID-N.sqlite` files still require manual inspection with every backup process stopped.

Never copy a live database with `cp`; WAL/shm files are not independent backups. Retain a tested backup before replacing code or importing a snapshot. Validate a restore by creating a fresh candidate through the same API:

```sh
sudo -u seeded env SEEDED_DATABASE=/var/lib/seeded/backups/BACKUP_ID.sqlite /opt/seeded/current/seeded_archive backup /var/lib/seeded/restore-RESTORE_ID.sqlite
```

Stop every importer/indexer writer before these restore commands. Unique `RESTORE_ID` paths must not exist. Move the complete old database/sidecar set into private rollback storage, then install the checked candidate. Run each line only after the previous succeeds; a missing optional sidecar is harmless.

```sh
sudo systemctl stop seeded.service
sudo -u seeded mkdir -m 0700 /var/lib/seeded/rollback-RESTORE_ID
sudo -u seeded mv /var/lib/seeded/archive.sqlite /var/lib/seeded/rollback-RESTORE_ID/
if sudo -u seeded test -e /var/lib/seeded/archive.sqlite-wal; then
    sudo -u seeded mv /var/lib/seeded/archive.sqlite-wal /var/lib/seeded/rollback-RESTORE_ID/
fi
if sudo -u seeded test -e /var/lib/seeded/archive.sqlite-shm; then
    sudo -u seeded mv /var/lib/seeded/archive.sqlite-shm /var/lib/seeded/rollback-RESTORE_ID/
fi
sudo -u seeded mv -T --no-clobber /var/lib/seeded/restore-RESTORE_ID.sqlite /var/lib/seeded/archive.sqlite
sudo -u seeded test ! -e /var/lib/seeded/restore-RESTORE_ID.sqlite
sudo systemctl start seeded.service
curl --fail http://127.0.0.1:3918/health/ready
curl --fail --output /dev/null http://127.0.0.1:3918/project/29437
curl --fail --output /dev/null http://127.0.0.1:3918/token/KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr/133459
```

Keep rollback files until restored reads and subsequent writes pass. Trial restoration must first pass against a separate state directory. No backup leaves the private target without separate authorization.

## Acceptance gates

- Native `caddy validate` succeeds, the admin API is absent, and the approved target obtains valid certificates for both hosts. Local loopback proxy tests do not validate public DNS, ACME reachability or production certificates.
- Public `/health` and `/health/` requests return 404 without reaching the app. Probe live/readiness through loopback; live remains healthy during an upstream outage and readiness checks local state.
- `/projects`, the Arboreal comparison, a real NFT, artist Projects/Collected and `/about` work through the app origin. Source, database and fixture JSON URLs fail.
- Artwork loads only from the content origin. App endpoints fail on that origin, and content execution routes fail on the app origin. Inspect iframe sandbox/CSP, relative assets, redirects and actual verification headers.
- App HTML and every error have `Cache-Control: no-store`. Only verified immutable 200/304 content can cache; static app files retain revalidation. A stopped upstream produces 503/no-store/Retry-After; restarting it recovers without clearing negative caches.
- SIGTERM during a render and during resolution drains or cancels within the configured deadline. No partial object becomes a cache hit; restart preserves database and verified cache.
- Online backup remains consistent during WAL writes; restore succeeds in an isolated state directory before an operational restore is accepted.
- `seeded-index stats` and verification reports identify pending/mismatched/unsigned records; a successful CLI exit alone is insufficient. Escrow attribution and full-catalogue deep pagination must pass their integration checks before claiming alpha readiness.

Use TLS-valid requests with `curl --resolve HOST:443:127.0.0.1 https://HOST/projects` for the app and a known content URL for the artwork host after ACME issuance and before opening the site to visitors. HTTP/TLS ACME challenges require working public DNS first. Check public `/health/ready` returns 404. TLS hosts send one-year HSTS without `includeSubDomains` or preload; HTTP redirect HSTS headers have no browser effect. Do not use `--insecure` as an acceptance result.

## Sizing

Measured workload and configuration estimates: [hardware.md](hardware.md). Recheck storage with:

```sh
python3 deploy/measure_state.py --database var/seeded.sqlite --cache var/cache --previews assets/fixtures/previews > tmp/alpha-storage.json
```

The report measures only local logical/allocated bytes, WAL/shm, row counts and largest stored files. Missing data is labelled missing. It does not infer full-chain totals. Capture cold and warm results, test inputs, concurrency, completion/failure counts and peak RSS from the launched alpha process; compare identical requests. Separately measure backup time/size and temporary peak disk use.

| Configuration | Evidence needed before sizing |
|---|---|
| Metadata + gateways | Measured database/WAL, previews, backup reserve, concurrent resolution memory and upstream traffic |
| Warm cache | Above, plus measured working-set reuse, bounded cache quota/eviction, staging peak and free-space alert threshold |
| Full mirror | Enumerated unique verified artifacts/chunks with actual sizes, missing/unverified inventory, recovery copy and measured indexing rate |

Cache quota charges each file `max(4 KiB, logical bytes rounded to 4 KiB, allocated bytes) + 4 KiB` for metadata. It caps stored files at 65,536; absence markers at 4,096 and one eighth of the byte budget. Replacement staging reserves its charge, expired markers are reaped opportunistically, and the least recently read temporary files are evicted when full (mirror files never are). This is conservative accounting, not an exact filesystem quota. Startup scanning is bounded. An inaccessible or oversized tree disables the whole cache safely with a warning; readiness reports `X-Seeded-Cache: bypassed`. A fresh readiness failure while the cache is enabled still returns 503. Cache read/write failures are misses rather than permanent content absence; content resolution has a 35-second budget inside the 40-second HTTP deadline. `measure_state.py` reports actual file allocation separately.

A full mirror cannot be sized by multiplying an unverified token count by one sample artifact. `SEEDED_MIRROR_BYTES` defaults to 50 GiB and accepts at most 1 PiB; size it from enumerated verified objects and leave filesystem headroom for staged packages, databases, cache and backups.

Proxy behavior: https://caddyserver.com/docs/caddyfile/directives/reverse_proxy and https://caddyserver.com/docs/caddyfile/options#timeouts. Load shedding and buffer lifetime were checked against https://github.com/caddyserver/caddy/blob/v2.11.7/modules/caddyhttp/reverseproxy/reverseproxy.go and https://github.com/caddyserver/caddy/blob/v2.11.7/modules/caddyhttp/reverseproxy/hosts.go. Systemd syntax is checked against an isolated validation root; tests install no host units.

Validation tooling decision: Ruff and mypy were absent from PATH. Use binary wheels in disposable `tmp/deployment_tools` instead of changing system Python; pip cache and staging also stay in project `tmp/`. Removing those project-local directories reverses the tool installation. No application dependency is added.

Local Python gates use Ruff 0.16.10 and mypy 2.4.0:

```sh
PYTHONPATH=tmp/deployment_tools PYTHONDONTWRITEBYTECODE=1 python3 -m ruff check --cache-dir tmp/ruff_deployment deploy
PYTHONPATH=tmp/deployment_tools PYTHONDONTWRITEBYTECODE=1 python3 -m ruff format --check --cache-dir tmp/ruff_deployment deploy
PYTHONPATH=tmp/deployment_tools PYTHONDONTWRITEBYTECODE=1 python3 -m mypy --strict --cache-dir tmp/mypy_deployment deploy/configure.py deploy/measure_state.py deploy/package_release.py deploy/maintain.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s deploy -p 'test_*.py'
```

Native proxy validation uses the standard Caddy 2.11.7 Linux amd64 release under `tmp/caddy_tool`. The official archive SHA-256 is `727b91701a392de6ebc5027509f548bf39979e5216340d0faed8fa5e69c84f8b`, checked against https://github.com/caddyserver/caddy/releases/tag/v2.11.7. No installer or hooks run. Tests adapt the production Caddyfile, then replace listeners/upstreams and disable public ACME in that test copy. Local TLS checks use an isolated temporary authority, never the workstation trust store. Removing `tmp/caddy_tool` reverses the tool extraction. `make test` requires Caddy and OpenSSL; missing native tools fail the gate. Put Caddy on PATH or set `SEEDED_TEST_CADDY` to its absolute path. Standalone Python runs may report explicit skips.

## Feature data and sign-in

Before the first feature release, stop the old app, retain a checked backup, and run:

```sh
seeded-index upgrade --db var/seeded.sqlite
seeded-index escrow --db var/seeded.sqlite
seeded-index offers --db var/seeded.sqlite
seeded-index profiles --db var/seeded.sqlite --source all
seeded-sales sync --sales-db var/sales.sqlite --archive-db var/seeded.sqlite
seeded-index volumes --db var/seeded.sqlite --sales-db var/sales.sqlite
seeded-articles sync --db var/articles.sqlite --archive-db var/seeded.sqlite
```

Use deployment paths on the approved target. Complete initial backfills before enabling their timers. Sales backfill can exceed the timer deadline; progress is resumable. The app opens the two secondary databases read-only. Their absence leaves catalogue browsing available and reports history/articles unavailable.

To upgrade an existing articles database offline, run `seeded-articles refs --db var/articles.sqlite --archive-db var/seeded.sqlite` before starting the new release.

`SEEDED_SALES_DATABASE`, `SEEDED_ARTICLES_DATABASE` and `SEEDED_TRADING_DATABASE` select the secondary files. Scheduled jobs require configured files directly inside the private state directory. Daily backups retain each existing database separately in `backups`, `backups-sales`, `backups-articles` and `backups-trading`; the trading backup directory is created only when `trading.sqlite` exists. Restore each with its corresponding filename. These are individually consistent backups, not one atomic snapshot across the databases. Local profile edits live in the main database and are included in its backup.

Sign-in sessions are process-local, expire after eight hours, and end on restart. At 4,096 sessions, eviction takes the least recently used session of the IPv6 /48, then /64, holding the most sessions; IPv4 uses individual addresses. This replaces existing sessions without a shared sign-in refusal. Production cookies require HTTPS; development accepts loopback HTTP. Signature verification binds the public key to its tz1/tz2/tz3 address. Profile edits require the authenticated address, exact Origin and CSRF token. The saved proof records the sign-in only, never a signature over later edits.

Transaction preparation uses `SEEDED_TRADING_RPC` and `SEEDED_TRADING_TZKT`. The platform fee defaults to 2.5%, is configurable from 0–1000 basis points, has no minimum and rounds down per token in mutez. Configure a supported recipient before accepting offers; an unset address disables accepts. Mainnet identity, current contract code, ownership and active terms are checked again before signing. Preparation and local simulation do not broadcast transactions; the connected signing application does.

Current-release reads share a 64-request upstream burst, refilling once per second; cached results consume no allowance. New name claims share a synced mainnet head for two seconds, then compare changed fxhash registry names locally. The chosen name, address and signature never go upstream. Delta pages select only required fields and halve from 100 keys on response overflow (64 KiB). Refreshes allow 10,000 keys and eight seconds; waiters allow ten seconds. Head advances merge subsequent changes; reorgs restart the scan within the same bounds. Malformed responses and upstream failures remain retryable. Delta rows are reused only at the same shared head and import level; failures are not cached. Existing claims and bio/links edits do not need this network check. Authenticated addresses receive six edits per minute. Read/write opens cannot create or migrate the archive and occur before the single write permit; a transaction guard rechecks the import level. This is a freshness check, not an on-chain reservation. Objkt aliases still use the latest import; the fxhash check does not cover live objkt changes.

Challenges are authenticated, process-bound cookies with a five-minute monotonic deadline; issuance allocates no pending records or address/network rate entries. A successful sign-in consumes earlier challenges for that address. Invalid signatures remain retryable. Verification has two workers and 16 waiters; successful sign-ins are limited to 16 per minute per address, with no shared network bucket. The 8,192-record address table evicts idle entries, preserving replay watermarks in a fixed 512 KiB keyed-hash table. Hash collisions can expire unused challenges; obtaining a fresh challenge recovers. Used challenges never become valid again. Network changes preserve challenges.

Trading admits four jobs, at most two per IPv4 address or IPv6 /48, through a 128-place queue. Turns rotate /48 families, then their /64 prefixes. One address can hold one job and make 120 requests per minute. New families may replace the newest queued request of an oversized family; singleton families retain their place. Waits expire after 250 ms and refund admission before work starts. Running chain calls keep their existing deadline.

Trading receipts last 30 minutes and follow the signed-in address across sessions. Limits are eight receipts per address, four unconfirmed, and 256 overall; confirmed entries evict first; unconfirmed receipts become evictable after the 120-second signing window, except while confirming or for 60 seconds after a confirmation poll (never beyond receipt expiry). Capacity is reserved before chain work, and exhaustion returns `ReceiptCapacity` with a retry delay. Missing/expired receipts release the browser lock while retaining the operation link, without rebroadcasting. Checks refund admission on invalid tickets and cancelled or timed-out waits before chain work begins; confirmations validate their receipt before admission.

Successful objkt raster avatars share a 5-minute memory cache (16 MiB, 128 entries). Four cold sources and 64 callers bound concurrent work; failures never enter the cache. HTTP responses remain private/no-store.
