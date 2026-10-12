Public mainnet Michelson, collected 2026-10-05 via TzKT and Tezos RPC. `hashes.json`
pins recorded inputs; `../pins.json` pins mutable entrypoints used at runtime.

https://api.tzkt.io/v1/contracts/KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u/code
https://api.tzkt.io/v1/contracts/KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9/code
https://rpc.tzkt.io/mainnet
https://pytezos.org/high_level.html

Run locally, without keys or network access during interpretation:

```
python3 -m venv tmp/trading/venv
TMPDIR="$PWD/tmp" PIP_CACHE_DIR="$PWD/tmp/trading/pip-cache" tmp/trading/venv/bin/pip install -r src/trading/fixtures/requirements.txt
tmp/trading/venv/bin/python tests/trading_contracts.py
```

Tests execute the recorded market and FA2 contracts on synthetic local storage.
External read-only views use deterministic results. Runtime RPC simulation remains
mandatory: the Python interpreter is an independent regression check, not consensus.
