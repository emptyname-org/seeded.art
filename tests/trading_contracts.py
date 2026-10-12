"""Execute recorded Michelson locally. No keys, node, or network calls."""

from __future__ import annotations

import copy
import hashlib
import json
import logging
import unittest
from pathlib import Path
from typing import Any

from pytezos import ContractInterface  # type: ignore[import-not-found]
from pytezos.michelson.format import (  # type: ignore[import-not-found]
    micheline_to_michelson,
)
from pytezos.michelson.micheline import (  # type: ignore[import-not-found]
    MichelsonRuntimeError,
)

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / "src/trading/fixtures"
MARKET = "KT1GbyoDi7H1sfXmimXpptZJuCdHMh66WS9u"
LEGACY = "KT1Xo5B7PNBAeynZPmca4bRh6LQow4og1Zb9"
BUYER = "tz1P719ymGCtNwavZKh2LmhMs53dGsEMzjnm"
SELLER = "tz1ewF2P3erZJSP8m35QiNEKGtwj7qsBhF8z"
TOKENS = [
    "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE",
    "KT1U6EHmNxJTkvaWJ4ThczG4FSDaHC21ssvi",
    "KT1EfsNuqwLAWDd3o4pvfUx1CAh5GMdTrRvr",
]
VIEWS = ["KT1WRpPyXHSCbTfDQFWVXbMpxxvf7Y7KChxc", *TOKENS[1:]]
logging.getLogger().setLevel(logging.ERROR)


def market_storage(contract: Any, version: int = 2) -> dict[str, Any]:
    storage: dict[str, Any] = contract.storage.dummy()
    storage.update(
        admin=SELLER,
        fees=25,
        treasury=SELLER,
        offers={
            7: {
                "buyer": BUYER,
                "gentk": {"id": 1, "version": version},
                "price": 1_000_000,
            }
        },
        collection_offers={
            8: {"buyer": BUYER, "amount": 2, "collection": 42, "price": 1_000_000}
        },
        listings={
            9: {
                "gentk": {"id": 1, "version": version},
                "seller": SELLER,
                "price": 1_000_000,
            }
        },
        gentk_contracts={
            i: {"fa2": token, "views": VIEWS[i]} for i, token in enumerate(TOKENS)
        },
        entrypoints_enabled={
            key: True
            for key in (
                "listing_cancel",
                "offer_cancel",
                "collection_offer_cancel",
                "offer_accept",
                "collection_offer_accept",
            )
        },
    )
    storage["big_map_18"] = {
        int(row["key"]["int"]): micheline_to_michelson(row["value"])
        for row in json.loads((FIXTURES / "lambdas.json").read_text())
    }
    return storage


def interpret(
    contract: Any,
    entrypoint: str,
    argument: Any,
    storage: dict[str, Any],
    sender: str,
    address: str = MARKET,
    project: int = 42,
) -> Any:
    views = {
        f"{view}%{name}": value
        for view in VIEWS
        for name, value in {
            "get_token_data": {
                "issuer_id": project,
                "iteration": 1,
                "royalties": 100,
                "royalties_split": [{"address": BUYER, "pct": 1000}],
            },
            "royalty_splits": {
                "royalty": 100,
                "splits": [{"address": BUYER, "pct": 1000}],
            },
        }.items()
    }
    return getattr(contract, entrypoint)(argument).interpret(
        storage=storage,
        sender=sender,
        source=sender,
        self_address=address,
        balance=10_000_000,
        view_results=views,
    )


class RecordedContracts(unittest.TestCase):
    market: Any
    legacy: Any

    @classmethod
    def setUpClass(cls) -> None:
        cls.market = ContractInterface.from_file(str(FIXTURES / "code_v2.michelson"))
        cls.legacy = ContractInterface.from_file(str(FIXTURES / "code_v1.michelson"))

    def test_builder_fixtures_decode_and_execute_recorded_contracts(self) -> None:
        for case in json.loads((FIXTURES / "builder_cases.json").read_text()):
            action = case["action"]
            if action["action"] == "cancel":
                call = case["calls"][0]
                contract = self.legacy if call["destination"] == LEGACY else self.market
                if call["destination"] == LEGACY:
                    storage: dict[str, Any] = contract.storage.dummy()
                    storage.update(
                        enabled=True,
                        objkts=TOKENS[0],
                        offers={
                            3: {
                                "creator": BUYER,
                                "issuer": SELLER,
                                "objkt_amount": 1,
                                "objkt_id": 1,
                                "price": 1_000_000,
                                "royalties": 100,
                            }
                        },
                    )
                else:
                    storage = market_storage(contract)
                entrypoint = call["parameters"]["entrypoint"]
                argument = getattr(contract, entrypoint).decode(
                    call["parameters"]["value"]
                )[entrypoint]
                interpret(
                    contract,
                    entrypoint,
                    argument,
                    storage,
                    case["actor"],
                    call["destination"],
                )
                continue
            address = action["token"]["contract"]
            version = TOKENS.index(address)
            token = ContractInterface.from_file(
                str(FIXTURES / f"code_{address}.michelson")
            )
            token_storage = token.storage.dummy()
            token_storage.update(
                ledger={(SELLER, 1): 1},
                token_metadata={1: {"token_id": 1, "token_info": {}}},
            )
            market = market_storage(self.market, version)
            for call in case["calls"]:
                contract = self.market if call["destination"] == MARKET else token
                entrypoint = call["parameters"]["entrypoint"]
                argument = getattr(contract, entrypoint).decode(
                    call["parameters"]["value"]
                )[entrypoint]
                result = interpret(
                    contract,
                    entrypoint,
                    argument,
                    market if contract is self.market else token_storage,
                    SELLER,
                    call["destination"],
                )
                if contract is self.market:
                    market = result.storage
                    transfer = result.operations[0]
                    argument = token.transfer.decode(transfer["parameters"]["value"])[
                        "transfer"
                    ]
                    token_storage = interpret(
                        token, "transfer", argument, token_storage, MARKET, address
                    ).storage
                else:
                    token_storage = result.storage
            self.assertEqual(token_storage["ledger"][(BUYER, 1)], 1)
            self.assertTrue(
                all(value is None for value in token_storage["operators"].values())
            )

    def test_recorded_inputs_match_hashes_and_runtime_lambda_pins(self) -> None:
        for name, expected in json.loads(
            (FIXTURES / "hashes.json").read_text()
        ).items():
            self.assertEqual(
                hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest(), expected
            )
        pins = json.loads((FIXTURES.parent / "pins.json").read_text())
        for row in json.loads((FIXTURES / "lambdas.json").read_text()):
            key = row["key"]["int"]
            self.assertEqual(pins[key]["level"], row["lastLevel"])
            encoded = json.dumps(
                row["value"], sort_keys=True, separators=(",", ":")
            ).encode()
            self.assertEqual(hashlib.sha256(encoded).hexdigest(), pins[key]["sha256"])

    def test_cancels_refund_exact_escrow_and_reject_other_sender(self) -> None:
        for entrypoint, key, refund in [
            ("offer_cancel", 7, 1_000_000),
            ("collection_offer_cancel", 8, 2_000_000),
        ]:
            with self.subTest(entrypoint=entrypoint):
                storage = market_storage(self.market)
                result = interpret(self.market, entrypoint, key, storage, BUYER)
                self.assertEqual(
                    [
                        (op["destination"], int(op["amount"]))
                        for op in result.operations
                    ],
                    [(BUYER, refund)],
                )
                with self.assertRaises(MichelsonRuntimeError):
                    interpret(self.market, entrypoint, key, storage, SELLER)
                with self.assertRaises(MichelsonRuntimeError):
                    interpret(self.market, entrypoint, key, result.storage, BUYER)

    def test_listing_cancel_returns_token_and_rejects_other_sender(self) -> None:
        storage = market_storage(self.market)
        result = interpret(self.market, "listing_cancel", 9, storage, SELLER)
        self.assertEqual(result.operations[0]["destination"], TOKENS[2])
        self.assertIsNone(result.storage["listings"].get(9))
        with self.assertRaises(MichelsonRuntimeError):
            interpret(self.market, "listing_cancel", 9, storage, BUYER)

    def test_v1_cancel_checks_owner_and_live_amount(self) -> None:
        storage = self.legacy.storage.dummy()
        storage.update(
            enabled=True,
            objkts=TOKENS[0],
            offers={
                3: {
                    "creator": BUYER,
                    "issuer": SELLER,
                    "objkt_amount": 1,
                    "objkt_id": 1,
                    "price": 1_000_000,
                    "royalties": 100,
                }
            },
        )
        result = interpret(self.legacy, "cancel_offer", 3, storage, SELLER, LEGACY)
        self.assertIsNone(result.storage["offers"].get(3))
        with self.assertRaises(MichelsonRuntimeError):
            interpret(self.legacy, "cancel_offer", 3, storage, BUYER, LEGACY)
        with self.assertRaises(MichelsonRuntimeError):
            interpret(self.legacy, "cancel_offer", 3, result.storage, SELLER, LEGACY)

    def test_paused_disabled_missing_and_exhausted_states_reject(self) -> None:
        for change in ("pause", "disable", "missing", "exhausted"):
            with self.subTest(change=change):
                storage = market_storage(self.market)
                if change == "pause":
                    storage["paused"] = True
                elif change == "disable":
                    storage["entrypoints_enabled"]["collection_offer_accept"] = False
                elif change == "missing":
                    storage["collection_offers"].clear()
                else:
                    storage["collection_offers"][8]["amount"] = 0
                with self.assertRaises(MichelsonRuntimeError):
                    interpret(
                        self.market,
                        "collection_offer_accept",
                        {"gentk": {"id": 1, "version": 2}, "offer_id": 8},
                        storage,
                        SELLER,
                    )

    def test_collection_rejects_wrong_project_and_decrements_remaining(self) -> None:
        storage = market_storage(self.market)
        argument = {"gentk": {"id": 1, "version": 2}, "offer_id": 8}
        with self.assertRaises(MichelsonRuntimeError):
            interpret(
                self.market,
                "collection_offer_accept",
                argument,
                storage,
                SELLER,
                project=99,
            )
        result = interpret(
            self.market, "collection_offer_accept", argument, storage, SELLER
        )
        self.assertEqual(result.storage["collection_offers"][8]["amount"], 1)
        self.assertEqual(sum(int(op["amount"]) for op in result.operations), 1_000_000)

    def test_atomic_operator_accept_remove_against_all_three_fa2_contracts(
        self,
    ) -> None:
        for version, address in enumerate(TOKENS):
            with self.subTest(version=version):
                token = ContractInterface.from_file(
                    str(FIXTURES / f"code_{address}.michelson")
                )
                original = token.storage.dummy()
                original.update(
                    ledger={(SELLER, 1): 1},
                    token_metadata={1: {"token_id": 1, "token_info": {}}},
                )
                storage = copy.deepcopy(original)
                operator = {"owner": SELLER, "operator": MARKET, "token_id": 1}
                storage = interpret(
                    token,
                    "update_operators",
                    [{"add_operator": operator}],
                    storage,
                    SELLER,
                    address,
                ).storage
                accepted = interpret(
                    self.market,
                    "offer_accept",
                    7,
                    market_storage(self.market, version),
                    SELLER,
                )
                transfer = accepted.operations[0]
                self.assertEqual(transfer["destination"], address)
                argument = token.transfer.decode(transfer["parameters"]["value"])[
                    "transfer"
                ]
                with self.assertRaises(MichelsonRuntimeError):
                    interpret(token, "transfer", argument, original, MARKET, address)
                storage = interpret(
                    token, "transfer", argument, storage, MARKET, address
                ).storage
                storage = interpret(
                    token,
                    "update_operators",
                    [{"remove_operator": operator}],
                    storage,
                    SELLER,
                    address,
                ).storage
                self.assertEqual(storage["ledger"][(BUYER, 1)], 1)
                self.assertEqual(storage["ledger"].get((SELLER, 1), 0), 0)
                self.assertTrue(
                    all(value is None for value in storage["operators"].values())
                )
                self.assertIsNone(accepted.storage["offers"].get(7))
                self.assertEqual(
                    sum(int(op["amount"]) for op in accepted.operations), 1_000_000
                )


if __name__ == "__main__":
    unittest.main()
