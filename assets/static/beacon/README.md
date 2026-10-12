# Beacon

Source: https://github.com/ecadlabs/beacon-sdk-taquito-patches

`@ecadlabs/beacon-sdk` 4.8.4-ecad supplies Tezos address pairing, message signing and
explicit operation requests. The maintained fork disables its legacy metrics endpoint.

Build with `make browser-tools`, then `node assets/static/beacon/build.mjs`.
The pinned esbuild 0.28.2 bundles local modules; no CDN or `eval` is used. Load the
bundle only after Connect or an explicit signing action. Runtime notices are
`client.js.LEGAL.txt`, `LICENSES.txt` and `UPSTREAM_LICENSE`.

`buffer` 6.0.3 supplies the Buffer API referenced by Beacon's ESM cryptography and
transport modules. esbuild injects it within the bundle; no browser global changes.
Source: https://github.com/feross/buffer

Beacon 4.8.4-ecad takes `network` in the DAppClient constructor, not
`requestPermissions`. Browser checks open and cancel the actual provider chooser.

WalletConnect packages are overridden to 2.25.0 to remove the vulnerable
elliptic/query-string dependency tree. `npm audit` covers the complete locked tree.

Signature format: https://docs.walletbeacon.io/guides/sign-payload/
and https://taquito.io/docs/25.0.0/signing/

Server verification uses BLAKE2b-256 of the complete packed Micheline bytes,
strict Ed25519 or ECDSA prehash verification, and BLAKE2b-160 public-key addresses.
Reference: https://github.com/ecadlabs/taquito/blob/master/packages/taquito-utils/src/verify-signature.ts
