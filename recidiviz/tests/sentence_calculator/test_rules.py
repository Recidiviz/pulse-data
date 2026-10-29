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
"""Tests for recidiviz/sentence_calculator/models/rules.py."""

import json
import os
import tempfile
import unittest
from datetime import date

from jsonschema.exceptions import ValidationError

from recidiviz.sentence_calculator.models.conditions import (
    AllOf,
    ConditionOperator,
    FieldCondition,
)
from recidiviz.sentence_calculator.models.rules import (
    LAW_REGIME_SCHEMA_PATH,
    DateFormulaRule,
    LawRegime,
    ReductionTarget,
    TermMultiplierRule,
    TermType,
    parse_state_conditions,
)
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)
from recidiviz.tests.ingest import fixtures

_CATEGORY_E = FieldCondition(
    field="offense_category",
    operator=ConditionOperator.EQUALS,
    value=NevadaOffenseCategory.CATEGORY_E,
)

_MAX_AND_MIN_TERMS = [
    ReductionTarget(term=TermType.MAX_TERM, when=None),
    ReductionTarget(term=TermType.MIN_TERM, when=None),
]


def _law_regime(
    *,
    term_multipliers: list[TermMultiplierRule],
    projected_dates: dict[str, DateFormulaRule],
) -> LawRegime:
    return LawRegime(
        name="TEST_REGIME",
        description="Test regime",
        effective_date=date(2020, 1, 1),
        use_calendar_term_conversion=False,
        days_per_month=30.0,
        days_per_year=365.0,
        term_multipliers=term_multipliers,
        projected_dates=projected_dates,
    )


def _date_rule(*, name: str, formula: str) -> DateFormulaRule:
    return DateFormulaRule(
        name=name,
        description=f"Computes {name}",
        when=None,
        formula=formula,
        statute=None,
    )


class TestLawRegimeFromYaml(unittest.TestCase):
    """Tests for LawRegime.from_yaml."""

    def test_parse_regime(self) -> None:
        shared_conditions = parse_state_conditions(
            fixtures.as_filepath("us_xx_rules/conditions.yaml"),
            sentence_class=NevadaSentence,
        )

        regime = LawRegime.from_yaml(
            fixtures.as_filepath("us_xx_law_regime.yaml"),
            sentence_class=NevadaSentence,
            shared_conditions=shared_conditions,
        )

        self.assertEqual(
            LawRegime(
                name="US_XX_LAW_REGIME",
                description=(
                    "Test regime that exercises every term multiplier regime section."
                ),
                effective_date=date(2020, 1, 1),
                use_calendar_term_conversion=True,
                days_per_month=30.44,
                # Not set in the file, so it gets the default.
                days_per_year=365.0,
                term_multipliers=[
                    TermMultiplierRule(
                        name="other_offense_term_reduction",
                        description="50% reduction for other offenses outside category A",
                        when=AllOf(
                            conditions=[
                                FieldCondition(
                                    field="offense_group",
                                    operator=ConditionOperator.EQUALS,
                                    value=NevadaOffenseGroup.OTHER,
                                ),
                                FieldCondition(
                                    field="offense_category",
                                    operator=ConditionOperator.NOT_IN,
                                    value=[NevadaOffenseCategory.CATEGORY_A],
                                ),
                            ]
                        ),
                        multiplier=0.5,
                        applies_to=[
                            ReductionTarget(term=TermType.MAX_TERM, when=None),
                            ReductionTarget(term=TermType.MIN_TERM, when=None),
                        ],
                        statute="Test Statute 1(a)",
                    ),
                    TermMultiplierRule(
                        name="standard_term_reduction",
                        description=(
                            "35% reduction of every other offense's max term, and of "
                            "its min term outside these offenses"
                        ),
                        when=None,
                        multiplier=0.65,
                        applies_to=[
                            ReductionTarget(term=TermType.MAX_TERM, when=None),
                            ReductionTarget(
                                term=TermType.MIN_TERM,
                                # The named conditions are filled in where they
                                # are used.
                                when=AllOf(
                                    conditions=[
                                        FieldCondition(
                                            field="offense_group",
                                            operator=ConditionOperator.NOT_IN,
                                            value=[
                                                NevadaOffenseGroup.VIOLENCE,
                                                NevadaOffenseGroup.SEX,
                                            ],
                                        ),
                                        FieldCondition(
                                            field="offense_id",
                                            operator=ConditionOperator.NOT_IN,
                                            value=[1001, 1002],
                                        ),
                                    ]
                                ),
                            ),
                        ],
                        statute="Test Statute 2",
                    ),
                ],
                projected_dates={
                    "parole_eligibility_date": DateFormulaRule(
                        name="parole_eligibility_date",
                        description="Eligible once the adjusted min term is served",
                        when=None,
                        formula="start_date + adjusted_min_days",
                        statute=None,
                    ),
                    "release_date": DateFormulaRule(
                        name="release_date",
                        description=(
                            "Release once the adjusted max term is served, for max "
                            "terms over three years"
                        ),
                        when=FieldCondition(
                            field="unadjusted_max_days",
                            operator=ConditionOperator.GREATER_THAN,
                            value=1094,
                        ),
                        formula="start_date + adjusted_max_days",
                        statute="Test Statute 3",
                    ),
                },
            ),
            regime,
        )

    def test_parse_term_multiplier_regime(self) -> None:
        regime = LawRegime.from_yaml(
            fixtures.as_filepath("us_xx_law_regime_term_multipliers.yaml"),
            sentence_class=NevadaSentence,
            shared_conditions={},
        )

        self.assertEqual(
            [
                TermMultiplierRule(
                    name="category_e_term_reduction",
                    description="50% reduction for category E offenses",
                    when=_CATEGORY_E,
                    multiplier=0.5,
                    applies_to=[
                        ReductionTarget(term=TermType.MAX_TERM, when=None),
                        ReductionTarget(term=TermType.MIN_TERM, when=None),
                    ],
                    statute=None,
                ),
                TermMultiplierRule(
                    name="standard_term_reduction",
                    description="35% reduction for every other offense",
                    when=None,
                    multiplier=0.65,
                    applies_to=[
                        ReductionTarget(term=TermType.MAX_TERM, when=None),
                        ReductionTarget(term=TermType.MIN_TERM, when=None),
                    ],
                    statute=None,
                ),
            ],
            regime.term_multipliers,
        )
        self.assertEqual({}, regime.projected_dates)
        # The file sets none of these, so they get their defaults.
        self.assertFalse(regime.use_calendar_term_conversion)
        self.assertEqual(30.0, regime.days_per_month)
        self.assertEqual(365.0, regime.days_per_year)

    def test_parse_rule_missing_name_fails_schema(self) -> None:
        with self.assertRaisesRegex(ValidationError, r"^'name' is a required property"):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_rule_missing_name.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_rule_missing_applies_to_fails_schema(self) -> None:
        with self.assertRaisesRegex(
            ValidationError, r"^'applies_to' is a required property"
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_rule_missing_applies_to.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_misspelled_condition_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[violent_term_reduction\]\. "
            r"The condition on field \[offense_group\] has value \['VIOLENT'\], which "
            r"the field can't take\. Values it can take: \['VIOLENCE', 'SEX', "
            r"'PROPERTY', 'DRUG', 'OTHER', 'UNKNOWN'\]\.$",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_unknown_condition_value.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_applies_to_with_unknown_value_fails_schema(self) -> None:
        with self.assertRaisesRegex(
            ValidationError, r"^'sometimes' is not valid under any of the given schemas"
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_applies_to_unknown_value.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_misspelled_condition_value_in_applies_to(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard_term_reduction\] "
            r"applies_to \[min_term\]\. The condition on field \[offense_group\] has "
            r"value \['VIOLENT'\], which the field can't take\. Values it can take: "
            r"\['VIOLENCE', 'SEX', 'PROPERTY', 'DRUG', 'OTHER', 'UNKNOWN'\]\.$",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath(
                    "us_xx_law_regime_applies_to_unknown_condition_value.yaml"
                ),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_projected_date_condition_on_sentence_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in projected date \[parole_eligibility_date\]\. "
            r"They use the field \[offense_group\], which conditions here can't "
            r"compare\.",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath(
                    "us_xx_law_regime_projected_date_sentence_field.yaml"
                ),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_projected_date_named_condition(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in projected date \[parole_eligibility_date\]\. "
            r"They refer to the named condition \[violent_or_sex\], but named "
            r"conditions can only be used in conditions on sentence fields\.$",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath(
                    "us_xx_law_regime_projected_date_named_condition.yaml"
                ),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_when_mapping_with_repeated_key(self) -> None:
        # Pre-commit rejects YAML files with repeated keys, so this file can't be
        # a checked-in fixture.
        regime_yaml = """
description: Test regime whose when mapping names two conditions under one key.
effective_date: "2020-01-01"
conditions:
  violent:
    offense_group: VIOLENCE
  category_a:
    offense_category: A
term_multipliers:
  - name: violent_category_a_term_reduction
    description: 15% reduction for violent category A offenses
    when:
      condition: violent
      condition: category_a
    multiplier: 0.85
  - name: standard_term_reduction
    description: 35% reduction for every other offense
    multiplier: 0.65
"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yaml_path = os.path.join(temp_dir, "test_regime.yaml")
            with open(yaml_path, "w", encoding="utf-8") as yaml_file:
                yaml_file.write(regime_yaml)

            with self.assertRaisesRegex(
                ValueError,
                r"^The YAML file \[.*test_regime\.yaml\] repeats the key "
                r"\[condition\] in one mapping, at line 14\. Remove one of them\. To "
                r"require several conditions on the same field, or several named "
                r"conditions, list them under all_of\.$",
            ):
                LawRegime.from_yaml(
                    yaml_path, sentence_class=NevadaSentence, shared_conditions={}
                )

    def test_parse_merge_key(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^The YAML file \[.*us_xx_law_regime_merge_key\.yaml\] uses the alias "
            r"\[\*category_e_term_reduction\] at line 14, which law regime files "
            r"don't support\. Write out the value in full\. To share a condition "
            r"between rules, define it as a named condition and refer to it with "
            r"condition: <name>\.$",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_merge_key.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_alias(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^The YAML file \[.*us_xx_law_regime_alias\.yaml\] uses the alias "
            r"\[\*long_sentence\] at line 12, which law regime files don't "
            r"support\.",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_alias.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_condition_redefining_shared_condition(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^The conditions \['not_violent_or_sex'\] are already defined in the "
            r"state's shared conditions\. Rename them or use the shared "
            r"definitions\.$",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={"not_violent_or_sex": _CATEGORY_E},
            )

    def test_parse_whole_numbers_as_floats(self) -> None:
        regime = LawRegime.from_yaml(
            fixtures.as_filepath("us_xx_law_regime_whole_numbers.yaml"),
            sentence_class=NevadaSentence,
            shared_conditions={},
        )

        self.assertEqual(30.0, regime.days_per_month)
        self.assertEqual(365.0, regime.days_per_year)
        self.assertEqual(1.0, regime.term_multipliers[0].multiplier)

    def test_parse_unquoted_dates(self) -> None:
        regime = LawRegime.from_yaml(
            fixtures.as_filepath("us_xx_law_regime_unquoted_dates.yaml"),
            sentence_class=NevadaSentence,
            shared_conditions={},
        )

        self.assertEqual(date(2020, 1, 1), regime.effective_date)
        self.assertEqual(
            [
                FieldCondition(
                    field="start_date",
                    operator=ConditionOperator.BEFORE,
                    value=date(2019, 1, 1),
                ),
                FieldCondition(
                    field="start_date",
                    operator=ConditionOperator.BETWEEN_INCLUSIVE,
                    value=[date(2019, 1, 1), date(2019, 12, 31)],
                ),
                None,
            ],
            [rule.when for rule in regime.term_multipliers],
        )

    def test_parse_date_with_time_fails_schema(self) -> None:
        with self.assertRaisesRegex(
            ValidationError,
            r"^datetime\.datetime\(2020, 1, 1, 10, 0\) is not of type 'string'",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_date_with_time.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={},
            )

    def test_parse_state_conditions_with_unquoted_date(self) -> None:
        self.assertEqual(
            {
                "started_after_2018": FieldCondition(
                    field="start_date",
                    operator=ConditionOperator.AFTER,
                    value=date(2018, 12, 31),
                )
            },
            parse_state_conditions(
                fixtures.as_filepath("us_xx_conditions_unquoted_dates.yaml"),
                sentence_class=NevadaSentence,
            ),
        )

    def test_schema_operators_match_python_enum(self) -> None:
        with open(LAW_REGIME_SCHEMA_PATH, encoding="utf-8") as f:
            schema = json.load(f)

        self.assertEqual(
            {e.value for e in ConditionOperator},
            set(schema["$defs"]["field_matcher"]["oneOf"][2]["properties"]),
        )

    def test_schema_terms_match_python_enum(self) -> None:
        with open(LAW_REGIME_SCHEMA_PATH, encoding="utf-8") as f:
            schema = json.load(f)

        self.assertEqual(
            [e.value for e in TermType],
            list(schema["$defs"]["applies_to"]["properties"]),
        )


class TestLawRegimeValidation(unittest.TestCase):
    """Tests for the cross-field validation in LawRegime.__attrs_post_init__."""

    def test_unconditional_rule_before_others(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has rule \[standard_term_reduction\] in "
            r"term_multipliers with no when conditions, so the rules after it never "
            r"apply\. Move it to the end of term_multipliers\.$",
        ):
            _law_regime(
                term_multipliers=[
                    TermMultiplierRule(
                        name="standard_term_reduction",
                        description="35% reduction for every other offense",
                        when=None,
                        multiplier=0.65,
                        applies_to=_MAX_AND_MIN_TERMS,
                        statute=None,
                    ),
                    TermMultiplierRule(
                        name="category_e_term_reduction",
                        description="50% reduction for category E offenses",
                        when=_CATEGORY_E,
                        multiplier=0.5,
                        applies_to=_MAX_AND_MIN_TERMS,
                        statute=None,
                    ),
                ],
                projected_dates={},
            )

    def test_term_multipliers_without_unconditional_last_rule(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has when conditions on "
            r"\[category_e_term_reduction\], the last rule in term_multipliers, so "
            r"sentences that match no rule get no reduction\. Add a last rule "
            r"without when conditions for every other sentence\.$",
        ):
            _law_regime(
                term_multipliers=[
                    TermMultiplierRule(
                        name="category_e_term_reduction",
                        description="50% reduction for category E offenses",
                        when=_CATEGORY_E,
                        multiplier=0.5,
                        applies_to=_MAX_AND_MIN_TERMS,
                        statute=None,
                    )
                ],
                projected_dates={},
            )

    def test_term_multiplier_with_repeated_term(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Term multiplier \[standard_term_reduction\] lists the terms "
            r"\['min_term'\] more than once in applies_to\. List each term once, "
            r"and combine its conditions with any_of or all_of\.$",
        ):
            TermMultiplierRule(
                name="standard_term_reduction",
                description="35% reduction for every offense",
                when=None,
                multiplier=0.65,
                applies_to=[
                    ReductionTarget(term=TermType.MAX_TERM, when=None),
                    ReductionTarget(term=TermType.MIN_TERM, when=_CATEGORY_E),
                    ReductionTarget(term=TermType.MIN_TERM, when=None),
                ],
                statute=None,
            )

    def test_duplicate_rule_names(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has duplicate rule names: "
            r"\['parole_eligibility_date'\]\.$",
        ):
            _law_regime(
                term_multipliers=[
                    TermMultiplierRule(
                        name="parole_eligibility_date",
                        description="Eligible once the adjusted min term is served",
                        when=None,
                        multiplier=0.65,
                        applies_to=_MAX_AND_MIN_TERMS,
                        statute=None,
                    )
                ],
                projected_dates={
                    "parole_eligibility_date": _date_rule(
                        name="parole_eligibility_date",
                        formula="start_date + adjusted_min_days",
                    )
                },
            )

    def test_formula_with_unknown_name(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Rule \[parole_eligibility_date\] uses \['adjusted_minimum_days'\] in "
            r"its formula, which are neither calculation values nor projected "
            r"dates\. Available names: \['adjusted_max_days', ",
        ):
            _law_regime(
                term_multipliers=[],
                projected_dates={
                    "parole_eligibility_date": _date_rule(
                        name="parole_eligibility_date",
                        formula="start_date + adjusted_minimum_days",
                    )
                },
            )

    def test_formula_with_duration_function(self) -> None:
        regime = _law_regime(
            term_multipliers=[],
            projected_dates={
                "mandatory_parole_release_date": _date_rule(
                    name="mandatory_parole_release_date",
                    formula="projected_expiration_date - months(12)",
                ),
                "projected_expiration_date": _date_rule(
                    name="projected_expiration_date",
                    formula="start_date + adjusted_max_days",
                ),
            },
        )

        self.assertEqual(
            ["projected_expiration_date", "mandatory_parole_release_date"],
            regime.projected_date_evaluation_order,
        )

    def test_malformed_formula(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[start_date \+\] in rule \[parole_eligibility_date\] is "
            r"not valid syntax\.$",
        ):
            _law_regime(
                term_multipliers=[],
                projected_dates={
                    "parole_eligibility_date": _date_rule(
                        name="parole_eligibility_date",
                        formula="start_date +",
                    )
                },
            )

    def test_formula_on_own_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has projected dates that depend on each "
            r"other in a cycle, where each date's formula uses the next: "
            r"\[release_date -> release_date\]\. Change the "
            r"formulas so no date depends on itself\.$",
        ):
            _law_regime(
                term_multipliers=[],
                projected_dates={
                    "release_date": _date_rule(
                        name="release_date",
                        formula="release_date + 1",
                    )
                },
            )

    def test_formulas_in_a_cycle(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has projected dates that depend on each "
            r"other in a cycle, where each date's formula uses the next: "
            r"\[release_date -> parole_eligibility_date -> release_date\]\. Change "
            r"the formulas so no date depends on itself\.$",
        ):
            _law_regime(
                term_multipliers=[],
                projected_dates={
                    "release_date": _date_rule(
                        name="release_date",
                        formula="parole_eligibility_date + 365",
                    ),
                    "parole_eligibility_date": _date_rule(
                        name="parole_eligibility_date",
                        formula="release_date - 365",
                    ),
                },
            )

    def test_projected_date_named_like_calculation_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has a projected date named \[start_date\], "
            r"which is already the name of a calculation value\. Rename the "
            r"projected date\.$",
        ):
            _law_regime(
                term_multipliers=[],
                projected_dates={
                    "start_date": _date_rule(
                        name="start_date",
                        formula="start_date + 1",
                    )
                },
            )


class TestProjectedDateEvaluationOrder(unittest.TestCase):
    """Tests for LawRegime.projected_date_evaluation_order."""

    def test_dates_come_after_the_dates_they_use(self) -> None:
        regime = _law_regime(
            term_multipliers=[],
            projected_dates={
                "mandatory_release_date": _date_rule(
                    name="mandatory_release_date",
                    formula="release_date - 365",
                ),
                "release_date": _date_rule(
                    name="release_date",
                    formula="parole_eligibility_date + 365",
                ),
                "parole_eligibility_date": _date_rule(
                    name="parole_eligibility_date",
                    formula="start_date + adjusted_min_days",
                ),
            },
        )

        self.assertEqual(
            ["parole_eligibility_date", "release_date", "mandatory_release_date"],
            regime.projected_date_evaluation_order,
        )


if __name__ == "__main__":
    unittest.main()
