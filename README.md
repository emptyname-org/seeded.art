# seeded.art

https://seeded.art

Seeded.art is an archive of NFTs rescued from fxhash after its shutdown. It preserves the platform’s projects, NFTs, artist and collector profiles, and articles. Each NFT runs the artist’s original code in a browser, using the seed recorded when it was minted.

Rust (Axum, Askama), SQLite (FTS5), plain JavaScript.

## Functionality

- Browsing and searching projects, NFTs, seeds, artists, collectors, sales history and articles from fxhash.
- Running NFTs from their original code. A verified copy of each project’s code is stored.
- Signing in with a Tezos wallet to edit a profile: name, bio and links.
- Accepting or cancelling offers, and cancelling listings, made on fxhash before it shut down.

## Quick start

```sh
make run
```

Serves http://127.0.0.1:3918 with sample data from `assets/fixtures/`. Requires Rust 1.85+, a C compiler and Make.

## Development

```sh
make test
make lint
make build
```

`make test` and `make lint` also need Python 3.11+, Node 20+, Chromium, Caddy 2.11.7+ and cargo-audit. Run `make browser-tools trading-tools` once first.

## Indexing

Requires a local copy of [fxhashArchive](https://github.com/KilledByAPixel/fxhashArchive).

```sh
make build
mkdir -p var
dist/seeded-index bootstrap --frank /path/to/fxhashArchive --db var/archive.sqlite
dist/seeded-index sync --db var/archive.sqlite
dist/seeded-index escrow --db var/archive.sqlite
dist/seeded-index offers --db var/archive.sqlite
SEEDED_DATABASE=var/archive.sqlite dist/seeded_archive serve
```

Settings are `SEEDED_*` environment variables, listed in `src/config.rs`.

## Docs

- [Deployment](docs/deployment.md)
- [Agents and API](docs/agents.md)
- [Hardware](docs/hardware.md)

## Credits

Developed by [Emptyname](https://emptyname.org). Based on Frank Force's [fxhashArchive](https://github.com/KilledByAPixel/fxhashArchive) (MIT). Chain data from [TzKT](https://tzkt.io).

Not affiliated with fxhash or objkt. Artwork rights remain with the creators.
