# Known bugs

## Generator mirror

All 24,738 projects are stored (2026-10-09). Nine (8875, 12147, 13951, 15632, 20068, 26207, 27962, 29106, 29898) came back incomplete from gateway CAR exports; their blocks were retrieved one by one, verified against their hashes and added to the mirror. The six projects with a 37–71 MB file (1769, 2141, 3483, 17119, 23591, 29853) are stored since `MAX_OBJECT_BYTES` was raised to 96 MiB.

## Playback limitations (top-100 check, 2026-10-08)

- `<iframes>` (5345) needs nested opaque frames. Content documents omit `frame-ancestors`: Chromium rejects opaque ancestors even with `*` (https://bugzilla.mozilla.org/show_bug.cgi?id=1891477). The script-only sandbox and content-only `frame-src` remain.
- Emotional Shell (17949) and 3D City (23591) load fonts from an external site; blocked by design, they render with fallback fonts.
- Charcoal Seeds (26659) reports `editionId is not defined` from its own `index.js:124` (SVG export filename, never defined in its package); the drawing completes.
