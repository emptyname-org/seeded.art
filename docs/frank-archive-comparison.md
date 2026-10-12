# Frank Force's fxhashArchive vs seeded.art (planned)

Comparison of the partial parts of `KilledByAPixel/fxhashArchive` with seeded.art as planned, not as it is now. Recorded 2026-10-05 for later discussion.

| | Frank Force's fxhashArchive | seeded.art (planned) |
|---|---|---|
| **Generator code** | 420 projects stored in the repo (1.5% of the catalogue, 71.9% of volume); the rest depends on IPFS | Nothing stored by default; every project is run live from IPFS or ONCHFS and checked against its content address. Cached on our server after first view, within a size budget. A full IPFS mirror is optional and not yet sized |
| **Previews and thumbnails** | Local images for the same ~420 projects; the rest are IPFS addresses | IPFS addresses for all, served through our artwork server and cached when viewed |
| **Chains** | Tezos only | Tezos only (decided); a different chain is a stage-2 topic |
| **On-chain (ONCHFS) projects** | ~370 still need fxhash's resolver | Our own resolver, checked block by block; no fxhash servers |
| **Seeds** | 1,802,387 (43,246 tokens never revealed have none) | The same 1.8M, checked against the chain (0 mismatches in samples) and stored with iteration, minter and params |
| **Descriptions** | 27,422 of 27,430, from fxhash's API | The same text (imported from Frank), shown on project and token pages with safe rendering |
| **Names** | 18,855 from fxhash's registry, plus 41 collaboration members with none | fxhash registry plus objkt plus curated identities plus names claimed by signing in. Unique ignoring case; shared names hidden, so those profiles show the address |
| **Hidden projects** | 2,692 flagged by fxhash are hidden, with no code stored | The same 2,692 hidden, plus their tokens, artists, articles and sales everywhere |
| **Freshness** | A frozen snapshot of fxhash's API, refreshed by hand | Started from Frank's snapshot, then kept current from the chain: owners, escrowed sellers and offers every 10 minutes; sales and articles on their own timers |
