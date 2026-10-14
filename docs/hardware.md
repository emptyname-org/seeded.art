# Hardware estimates

Budget **6 GiB RAM provisionally and 96 GiB SSD** for the archive, profiles, sales, articles, gateway-backed artwork and the default 2 GiB cache. The combined app/proxy fixture passed under a 3 GiB cgroup; full-catalogue sustained capacity remains unmeasured. These are estimates, not measured capacity. No server-side browser is needed for normal playback; artwork runs in the visitor's browser.

| Configuration | RAM | Disk | Tradeoff |
|---|---:|---:|---|
| Default archive with feature data | 6 GiB provisional | 96 GiB | 32 MiB objects, four concurrent assemblies, 2 GiB cache. |
| Metadata without sales/articles | 6 GiB provisional | 64 GiB | History and article pages unavailable; retains catalogue, profiles and playback. |
| Larger working cache | 6 GiB provisional | 128 GiB | Example 32 GiB cache; fewer repeated upstream fetches. |
| Constrained archive | 4 GiB provisional | 96 GiB | Set `SEEDED_MAX_OBJECT_BYTES=8388608` and `SEEDED_CONTENT_CONCURRENCY=2`; larger works fail the cap. Caddy streams at either size. |
| Preservation mirror | 12 GiB provisional | Inventory-dependent | Extra RAM is an unmeasured allowance for mirror software; storage formula below. |

GiB means 2³⁰ bytes: 64 GiB is about 68.7 decimal GB. Allow separate app, maintenance-job, proxy and operating-system budgets. For the default profile, initially allocate 2 GiB for the app, 1.5 GiB for Caddy, 512 MiB for maintenance, and the remainder for the OS, filesystem cache and headroom. These are initial budgets, not proven worst-case bounds. Test large-object mixtures before enforcing hard limits. `JOB_MEMORY_MAX` otherwise inherits the app limit, in a separate cgroup.

## Measured locally

On 2026-10-05, the schema-v8 main database measures **3.029 GiB**, sales **0.710 GiB** (2,343,325 rows), and articles **0.020 GiB** (1,716 articles): **3.758 GiB combined**. Measure all three with `python3 deploy/measure_state.py`; current evidence is `tmp/review/r1-storage.json`. Profile ingestion finished in **1,030 seconds / 29.4 MiB peak RSS** (`tmp/feature_profiles.log`). Tezos Profiles was unavailable; fxhash and objkt completed.

A pre-v8 feature candidate passed **72/72 metadata requests** across 1, 4 and 8 clients, with **11.59 MiB idle / 29.60 MiB sampled peak RSS** and p95 **38.12 ms** at eight clients (`tmp/feature_measure.json`, binary `b13ba520b56c`). This subsecond burst used warm OS pages, 16 logical CPUs and no CPU cap, TLS or proxy. It included article and sign-in pages but no signature verification, transaction simulation or artwork bodies; it is not a capacity test.

The longer v5 measurements below predate sign-in, trading and articles. Additional bounded work includes two signature workers, four transaction preparations and four current-release fetches and four bounded avatar fetches. Neither benchmark validates peak feature-round memory. The current proxy no longer buffers complete artwork responses. Sales backfill measured 191 MiB RSS at default settings, or 45 MiB with `SEEDED_SALES_PAGE=2000` and `SEEDED_TZKT_CONCURRENCY=2` (`tmp/claude-sales/MARKETS.md`); article sync measured 25 MiB (`tmp/claude-articles/ARTICLES.md`).

The native fixture uses real Rust app/artwork routers, SQLite, verified ONCHFS content, TLS and Caddy 2.11.7. Five clients hold **80 stalled app responses** or **10 stalled 31 MiB artwork responses**. A sixth completes the full 32 MiB app file and 31 MiB artwork; disconnecting attackers restores their capacity. A separate phase holds **4,000 idle HTTP/2 connections from one address** while another address still downloads a complete file. Body-cap checks POST through Caddy into the real Rust extractors.

The expanded test passed under a temporary **3 GiB cgroup with swap disabled**, with **1286.1 MiB memory.peak and zero OOM kills**. Sampled maxima: combined app/proxy RSS **897.0 MiB**, app **828.8 MiB**, proxy **223.8 MiB**; individual peaks need not coincide. Evidence: `tmp/caddy-review/http2-isolation-r2-cgroup.json`. Command: `systemd-run --user --scope -p MemoryMax=3G -p MemorySwapMax=0 env SEEDED_REQUIRE_CGROUP=1 python3 -m unittest test_proxy_isolation`, with project tools and `deploy/` on the search paths. The scope also contains the Python and Node drivers. This excludes full-catalogue sustained load and each production unit's sandbox.

At the observed roughly 50–60 KiB per idle connection, 16,384 file descriptors warrant approximately 1 GiB for connection state plus startup and active-request headroom. The initial proxy allowance is 1.5 GiB, replacing the former 256 MiB. This estimate is not a proven ceiling at 16,384 active connections.

Earlier buffering measurements reached 806.8–1102.8 MiB for Caddy alone (`tmp/caddy-review/slow-readers-{8,16}x32MiB.json`). They describe the removed buffering design, not current requirements.

The final v5 release served a fresh database copy on 2026-10-03, using four database workers per origin (eight total). The primary archive remained running. The workstation exposed 16 logical CPUs; no CPU cap, proxy, TLS or Kubo daemon was included. Disk pages were already warm.

- Database: **2.806 GiB**, 27,430 stored projects and 1,802,387 tokens. Moderation reduces the public catalogue.
- Process: **9.73 MiB idle**, **84.71 MiB peak RSS**, up to 26 threads.
- **602 HTTP requests succeeded**: 144 metadata, 48 About, 38 cold-content, 228 warm-content and 144 mixed requests. The isolated process exited cleanly; primary listeners were untouched.
- At 16 concurrent clients, metadata p95 was **34.09 ms**; warm content **11.79 ms**; mixed broad searches, About, metadata and artwork **78.62 ms**.
- About p95 was **46.77 ms** at one client and **279.47 ms** at sixteen. All 48 requests returned 200; the archived pre-fix single request took 4.49 seconds.
- Largest tested body: **1.67 MiB**. These short bursts do not establish maximum-size capacity, sustained throughput or behavior under a CPU cap.
- Full chain sync: **29 minutes / 48.7 MiB peak RSS**; initial catalogue bootstrap **10.7 seconds / 85.5 MiB**. A no-change incremental sync took **1.6 seconds / 10.1 MiB**.

Evidence: `tmp/hardware/{MEASUREMENTS.md,measurements.json,summary.json,sizing.json}`. Final release SHA-256 prefix: `4bda90c11cc96`. Prior measurements are preserved under `tmp/hardware/pre-v5/`; sync/bootstrap figures come from the earlier `tmp/claude-ingest/MEASUREMENTS.md`, not a new v5 indexing run. The coordinator ran separate primary acceptance work during this benchmark. These observations exclude browser rendering and cannot predict WAN latency or visitors per second.

Default bounds explain the headroom above observed RSS: four 32 MiB content assemblies can hold 128 MiB of payload; independent delivery reservations retain at most 512 MiB by default, with 64 MiB per client. ONCHFS hex JSON, decoded chunks, CAR verification, gzip, HTML preparation and allocator retention require additional simultaneous buffers. These overlapping components are not an exact additive memory ceiling. Caddy streams artwork; Rust emits at most 64 KiB per delivery frame and retains the remaining payload with its permit. Socket and TLS buffers still consume memory.

## Disk calculation

Let `B` be the combined size of the main, sales and articles databases, and `C` the cache quota, both in GiB. Reserve active databases, two scheduled backups of each, one staging copy, one database-sized WAL allowance, 8 GiB for the OS, 2 GiB for logs, 0.5 GiB for releases and 20% free space:

`disk ≥ (5B + 10.5 + C) / 0.8`

At planning `B=4`, this gives **40.63 GiB** with a 2 GiB cache, or **78.13 GiB** with a 32 GiB cache. Round up to the table's capacities. Every extra retained full database set adds `B/0.8`. WAL space is an allowance, not an enforced cap; long readers during indexing can require more. Manual rollback backups are additional to the two scheduled copies.

Measure current files with `python3 deploy/measure_state.py`. Configure cache bytes through `SEEDED_CACHE_BYTES`; fetched IPFS objects larger than `SEEDED_CACHE_OBJECT_BYTES` (default 384 KiB, above every sampled thumbnail) are served but kept neither on disk nor in memory, so large NFT display images don't evict card thumbnails; increasing its quota does not remove its 65,536-file and 8,192-chunk entry caps. The temporary cache evicts least-recently-read objects when full, releasing accounting between batches of 128 deletions. Admission allows two seconds of eviction; partial progress survives a bypass. Startup trims a reduced quota for up to ten seconds; later writes continue if needed. Memory hits refresh backing-entry recency at most once per second. Recency resets on restart; byte accounting survives it. The mirror (`SEEDED_MIRROR`, `seeded-index mirror`) stores verified IPFS blocks and ONCHFS records once each, within `--max-bytes`, never evicted; `mirror-status` reports its bytes.

For a mirror, inventory unique retained artifact/preview bytes `U`, mirror/index overhead `I`, and the number of retained content copies `R`:

`disk ≥ (10B + 10.5 + C + R×U + I) / 0.8`

There is no complete verified byte inventory yet, so a fixed TB estimate would be unsupported. Shared artwork code must be deduplicated; multiplying token count by one sample size is misleading. A second copy on the same machine does not provide independent recovery.

## Preview storage sample — 2026-10-08

| Images | Distinct visible URIs | Estimated payload | Sample uncertainty (95% bootstrap) |
|---|---:|---:|---:|
| Project thumbnails | 24,691 | 2.67 GiB (2.86 GB) | 2.45–2.89 GiB |
| Edition thumbnails | 1,735,283 | 178.4 GiB (191.6 GB) | 163.5–193.7 GiB |
| Edition display images | 1,735,417 | 2,463.8 GiB (2.65 TB) | 1,897.0–3,093.5 GiB |

Project and edition thumbnails together are approximately **181 GiB (194 GB)** before cross-population deduplication and storage overhead. With the reported 97 GiB free on the VPS, project thumbnails fit; all original edition thumbnails do not. Edition cards currently prefer display images, so mirroring thumbnails alone would not remove their upstream requests.

Method: count distinct URIs on visible projects and their editions in `var/names-v10-main.sqlite`; uniformly sample 256 distinct URIs per population; read object sizes through bounded public IPFS HEAD/range requests. All 768 samples returned sizes. These are gateway-reported lengths, not CID-verified downloads; the estimates exclude CAR/filesystem overhead and may miss rare large files. They cover previews, not generator packages.

Evidence: `tmp/thumbnail-sizing/{inventory,measurements,sizes}.json`; sampler: `python3 tmp/thumbnail-sizing/probe_sizes.py`. Counts also match `var/seeded.sqlite`. The VPS free-space figure is Claude's read-only check reported on 2026-10-08, not a reservation.

## Loopback load results — 2026-10-09, release 781617a

| Visitors | Requests/s | Errors | Page p95 / p99 | Artwork p50 / p95 |
|---:|---:|---:|---:|---:|
| 100 | 665 | 0.85% | 57 / 155 ms | 11 / 213 ms |
| 250 | 1,482 | 1.04% | 251 / 387 ms | 192 / 1,666 ms |
| 500 | 1,641 | 3.69% | 428 / 1,259 ms | 1,378 / 4,431 ms |
| 1,000 | 1,798 | 8.02% | 292 / 507 ms | 1,610 / 7,236 ms |

500 visitors stayed below the 5% stop line; 1,000 exceeded it. These are 180-second loopback replays, not WAN capacity guarantees. The generator shared the six-core VPS and used 246% CPU at 500 visitors and 259% at 1,000, competing with the services.

At a 96 MiB object cap, response budgets allow 512 MiB local delivery plus 776 MiB active and 97 MiB pending artwork delivery, before scratch, LRU, assembly buffers and other process memory. This is a reservation envelope, not a process ceiling.

Evidence: `tmp/load/results-781617a/s{100,250,500,1000}.json`; previous run: `tmp/load/results-9449b9f/`.
