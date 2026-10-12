"""Optional platform fee recipient in the rendered app environment."""

import unittest

import configure
import test_deployment as deployment

ADDRESS = "tz1hCUrKAsam1ZBEVWyZnrzuQCwNhywUWrz6"


class FeeSettingsTests(unittest.TestCase):
    def setUp(self):
        self.fixture = deployment.DeploymentTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)

    def environment(self, values):
        rendered = self.fixture.directory / "rendered"
        configure.render(self.fixture.settings(values), rendered)
        return (rendered / "seeded.env").read_text().splitlines()

    def test_absent_fee_address_keeps_accepts_disabled(self):
        self.assertIn(
            "SEEDED_PLATFORM_FEE_ADDRESS=", self.environment(deployment.VALID)
        )

    def test_valid_kt1_and_tz_addresses_pass_the_checksum(self):
        for value in (
            ADDRESS,
            "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEL",
            "KT1P2BXYb894MekrCcSrnidzQYPVqitLoVLc",
        ):
            self.assertTrue(configure.fee_address(value), value)

    def test_fee_address_is_rendered_for_the_app(self):
        lines = self.environment({**deployment.VALID, "PLATFORM_FEE_ADDRESS": ADDRESS})
        self.assertIn(f"SEEDED_PLATFORM_FEE_ADDRESS={ADDRESS}", lines)

    def test_malformed_checksum_typo_or_tz4_fee_address_is_refused(self):
        for value in (
            "tz4" + ADDRESS[3:],
            ADDRESS[:-1],
            ADDRESS + "x",
            "KT1" + "0" * 33,
            "tz1gL9H53BzjDqmMi2JkKALeCzT7UAoopzEM",
            f"{ADDRESS}\nSEEDED_BIND=0.0.0.0:80",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.fixture.settings(
                    {**deployment.VALID, "PLATFORM_FEE_ADDRESS": value}
                )


if __name__ == "__main__":
    unittest.main()
