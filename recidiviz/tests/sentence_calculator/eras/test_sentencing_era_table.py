# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""Tests for SentencingEraTable and the Arizona era table."""

import os
import tempfile
import unittest
from datetime import date

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.eras.sentencing_era_table import (
    SentencingEra,
    SentencingEraTable,
    load_sentencing_eras,
)
from recidiviz.tests import sentence_calculator as sentence_calculator_tests

_FIXTURES_DIR = os.path.join(
    os.path.dirname(sentence_calculator_tests.__file__), "fixtures"
)


def _fixture_path(file_name: str) -> str:
    return os.path.join(_FIXTURES_DIR, file_name)


def _era(
    name: str, offenses_on_or_after: date | None, regime_name: str
) -> SentencingEra:
    return SentencingEra(
        name=name,
        description=f"Era {name}",
        offenses_on_or_after=offenses_on_or_after,
        regime_name=regime_name,
    )


class TestSentencingEraTable(unittest.TestCase):
    """Tests for the table's invariants and lookups."""

    def test_lookup_returns_the_last_era_started_on_or_before_the_date(
        self,
    ) -> None:
        table = SentencingEraTable(
            state_code=StateCode.US_XX,
            eras=[
                _era("FIRST", None, "REGIME_1"),
                _era("SECOND", date(2000, 1, 1), "REGIME_2"),
                _era("THIRD", date(2010, 6, 15), "REGIME_2"),
            ],
        )

        for offense_date, era_name in [
            (date(1900, 1, 1), "FIRST"),
            (date(1999, 12, 31), "FIRST"),
            (date(2000, 1, 1), "SECOND"),
            (date(2010, 6, 14), "SECOND"),
            (date(2010, 6, 15), "THIRD"),
            (date(2099, 1, 1), "THIRD"),
        ]:
            with self.subTest(offense_date=offense_date):
                self.assertEqual(
                    era_name, table.era_for_offense_date(offense_date).name
                )
        self.assertEqual(
            "REGIME_2", table.regime_name_for_offense_date(date(2010, 6, 15))
        )

    def test_first_era_with_a_start_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^The first era \[FIRST\] for state \[US_XX\] has a start"
        ):
            SentencingEraTable(
                state_code=StateCode.US_XX,
                eras=[
                    _era("FIRST", date(1990, 1, 1), "REGIME_1"),
                    _era("SECOND", date(2000, 1, 1), "REGIME_2"),
                ],
            )

    def test_later_era_without_a_start_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Era \[SECOND\] for state \[US_XX\] has no start date\."
        ):
            SentencingEraTable(
                state_code=StateCode.US_XX,
                eras=[_era("FIRST", None, "REGIME_1"), _era("SECOND", None, "R")],
            )

    def test_eras_out_of_date_order(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Era \[THIRD\] for state \[US_XX\] starts on \[2000-01-01\], which "
            r"is not after the start of the era before it, \[SECOND\] on "
            r"\[2000-01-01\]\.",
        ):
            SentencingEraTable(
                state_code=StateCode.US_XX,
                eras=[
                    _era("FIRST", None, "REGIME_1"),
                    _era("SECOND", date(2000, 1, 1), "REGIME_2"),
                    _era("THIRD", date(2000, 1, 1), "REGIME_3"),
                ],
            )

    def test_duplicate_era_names(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^The era table for state \[US_XX\] has duplicate era names: "
            r"\['FIRST'\]\.$",
        ):
            SentencingEraTable(
                state_code=StateCode.US_XX,
                eras=[
                    _era("FIRST", None, "REGIME_1"),
                    _era("FIRST", date(2000, 1, 1), "REGIME_2"),
                ],
            )

    def test_extra_key_in_an_era(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Found unexpected keys in era \[SECOND\] in \[.*\]: "
        ):
            SentencingEraTable.from_yaml(
                _fixture_path("us_xx_eras_extra_era_key.yaml"),
                state_code=StateCode.US_XX,
                regime_effective_dates={
                    "REGIME_1": date(1900, 1, 1),
                    "REGIME_2": date(2000, 1, 1),
                },
            )

    def test_repeated_key_in_an_era(self) -> None:
        # Written at test time because the repo's YAML hooks reject a checked-in
        # file with a repeated key.
        eras_yaml = """
eras:
  - name: FIRST
    description: The first era.
    regime: REGIME_1
  - name: SECOND
    description: The second era, with its start written twice.
    offenses_on_or_after: 2000-01-01
    offenses_on_or_after: 2001-01-01
    regime: REGIME_2
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = os.path.join(temp_dir, "us_xx_eras.yaml")
            with open(yaml_path, "w", encoding="utf-8") as yaml_file:
                yaml_file.write(eras_yaml)

            with self.assertRaisesRegex(
                ValueError,
                r"repeats the key \[offenses_on_or_after\] in one mapping",
            ):
                SentencingEraTable.from_yaml(
                    yaml_path,
                    state_code=StateCode.US_XX,
                    regime_effective_dates={
                        "REGIME_1": date(1900, 1, 1),
                        "REGIME_2": date(2000, 1, 1),
                    },
                )

    def test_era_start_differs_from_regime_effective_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Era \[SECOND\] in \[.*\] starts on \[2000-01-01\], but the law "
            r"regime it routes to, \[REGIME_2\], has effective_date \[2000-01-02\]",
        ):
            SentencingEraTable.from_yaml(
                _fixture_path("us_xx_eras_effective_date_mismatch.yaml"),
                state_code=StateCode.US_XX,
                regime_effective_dates={
                    "REGIME_1": date(1900, 1, 1),
                    "REGIME_2": date(2000, 1, 2),
                },
            )

    def test_load_state_without_a_table(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^No sentencing era table defined for state \[US_XX\]\."
        ):
            load_sentencing_eras(StateCode.US_XX)


class TestArizonaEras(unittest.TestCase):
    """Tests for us_az_eras.yaml."""

    def setUp(self) -> None:
        self.table = load_sentencing_eras(StateCode.US_AZ)

    def test_eras_in_order_with_their_regimes(self) -> None:
        self.assertEqual(
            [
                ("OLD_CODE", "OLD_CODE"),
                ("NEW_CODE_A", "NEW_CODE_A_B"),
                ("NEW_CODE_B", "NEW_CODE_A_B"),
                ("NEW_CODE_C", "NEW_CODE_C_D"),
                ("NEW_CODE_D", "NEW_CODE_C_D"),
                ("TIS_A", "TIS_A"),
                ("TIS_B", "TIS_B"),
                ("TIS_C", "TIS_C"),
            ],
            [(era.name, era.regime_name) for era in self.table.eras],
        )

    def test_regime_on_each_side_of_every_boundary(self) -> None:
        for offense_date, regime_name in [
            (date(1978, 9, 30), "OLD_CODE"),
            (date(1978, 10, 1), "NEW_CODE_A_B"),
            (date(1986, 8, 12), "NEW_CODE_A_B"),
            (date(1986, 8, 13), "NEW_CODE_C_D"),
            (date(1993, 12, 31), "NEW_CODE_C_D"),
            (date(1994, 1, 1), "TIS_A"),
            (date(1995, 7, 12), "TIS_A"),
            (date(1995, 7, 13), "TIS_B"),
            (date(2001, 8, 8), "TIS_B"),
            (date(2001, 8, 9), "TIS_C"),
            (date(2026, 11, 2), "TIS_C"),
        ]:
            with self.subTest(offense_date=offense_date):
                self.assertEqual(
                    regime_name, self.table.regime_name_for_offense_date(offense_date)
                )

    def test_eras_that_share_a_regime_still_split_at_their_boundary(self) -> None:
        self.assertEqual(
            "NEW_CODE_A", self.table.era_for_offense_date(date(1985, 8, 6)).name
        )
        self.assertEqual(
            "NEW_CODE_B", self.table.era_for_offense_date(date(1985, 8, 7)).name
        )
        self.assertEqual(
            "NEW_CODE_C", self.table.era_for_offense_date(date(1990, 9, 26)).name
        )
        self.assertEqual(
            "NEW_CODE_D", self.table.era_for_offense_date(date(1990, 9, 27)).name
        )
