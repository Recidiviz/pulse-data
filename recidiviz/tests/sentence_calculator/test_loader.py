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
"""Tests for recidiviz/sentence_calculator/rules/loader.py."""

import unittest
from unittest import mock

from recidiviz.common.constants.states import StateCode
from recidiviz.sentence_calculator.models.conditions import (
    ConditionOperator,
    FieldCondition,
)
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import NevadaSentence
from recidiviz.sentence_calculator.rules.loader import (
    law_regime_yaml_path,
    load_law_regime,
    load_state_conditions,
)
from recidiviz.tests.ingest import fixtures

_SENTENCE_CLASS_PATH = (
    "recidiviz.sentence_calculator.models.state_inputs.sentence_class"
)
_STATE_RULES_DIR_PATH = "recidiviz.sentence_calculator.rules.loader._state_rules_dir"


class TestLawRegimeYamlPath(unittest.TestCase):
    """Tests for law_regime_yaml_path."""

    def test_yaml_path_is_lowercased_under_state_directory(self) -> None:
        self.assertRegex(
            law_regime_yaml_path(state_code=StateCode.US_XX, regime_name="SB_413"),
            r"/recidiviz/sentence_calculator/rules/us_xx/sb_413\.yaml$",
        )

    def test_regime_name_with_trailing_newline(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid law regime name \[SB_413\n\]\. Regime names may only "
            r"contain letters, digits, and underscores\.$",
        ):
            law_regime_yaml_path(state_code=StateCode.US_XX, regime_name="SB_413\n")


class TestLoadLawRegime(unittest.TestCase):
    """Tests for load_law_regime."""

    def setUp(self) -> None:
        # The fixtures are for US_XX, which has no Sentence class of its own, so
        # their conditions compare Nevada sentence fields.
        self.sentence_class_patcher = mock.patch(
            _SENTENCE_CLASS_PATH, return_value=NevadaSentence
        )
        self.sentence_class_patcher.start()

    def tearDown(self) -> None:
        self.sentence_class_patcher.stop()

    def test_load_regime(self) -> None:
        with mock.patch(
            _STATE_RULES_DIR_PATH, return_value=fixtures.as_filepath("us_xx_rules")
        ):
            regime = load_law_regime(
                state_code=StateCode.US_XX, regime_name="TEST_REGIME"
            )

        self.assertEqual("TEST_REGIME", regime.name)
        self.assertEqual(StateCode.US_XX, regime.state_code)
        # The regime's first rule refers to a condition in the state's
        # conditions.yaml.
        self.assertEqual(
            FieldCondition(
                field="offense_id", operator=ConditionOperator.IN, value=[1001, 1002]
            ),
            regime.term_multipliers[0].when,
        )

    def test_load_regime_with_lowercase_name(self) -> None:
        with mock.patch(
            _STATE_RULES_DIR_PATH, return_value=fixtures.as_filepath("us_xx_rules")
        ):
            regime = load_law_regime(
                state_code=StateCode.US_XX, regime_name="test_regime"
            )

        self.assertEqual("TEST_REGIME", regime.name)

    def test_load_missing_regime(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^No law regime \[NOT_A_REGIME\] defined for state \[US_XX\]\. Expected "
            r"a file at \[.*/rules/us_xx/not_a_regime\.yaml\]\.$",
        ):
            load_law_regime(state_code=StateCode.US_XX, regime_name="NOT_A_REGIME")

    def test_load_regime_whose_state_does_not_match_path(self) -> None:
        with mock.patch(
            _STATE_RULES_DIR_PATH, return_value=fixtures.as_filepath("us_xx_rules")
        ), self.assertRaisesRegex(
            ValueError,
            r"^Law regime file \[.*us_xx_rules/test_regime\.yaml\] has state_code "
            r"\[US_XX\], but it is in the directory for state \[US_YY\]\. Make the "
            r"state_code in the file match its directory\.$",
        ):
            load_law_regime(state_code=StateCode.US_YY, regime_name="TEST_REGIME")

    def test_regime_name_that_could_escape_state_directory(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid law regime name \[\.\./us_yy/sb_413\]\. Regime names may "
            r"only contain letters, digits, and underscores\.$",
        ):
            load_law_regime(state_code=StateCode.US_XX, regime_name="../us_yy/sb_413")


class TestLoadStateConditions(unittest.TestCase):
    """Tests for load_state_conditions."""

    def test_state_without_conditions_file(self) -> None:
        self.assertEqual({}, load_state_conditions(StateCode.US_XX))


if __name__ == "__main__":
    unittest.main()
