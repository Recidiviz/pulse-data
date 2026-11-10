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

import attr
from jsonschema.exceptions import ValidationError

from recidiviz.sentence_calculator.models.conditions import (
    AllOf,
    ConditionOperator,
    FieldCondition,
)
from recidiviz.sentence_calculator.models.rules import (
    LAW_REGIME_SCHEMA_PATH,
    CreditCapRule,
    CreditRateRule,
    DateFloor,
    DateFormulaRule,
    LawRegime,
    ProjectedDateCondition,
    ReductionTarget,
    TermMultiplierRule,
    TermType,
    expression_variable_names,
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


@attr.define(frozen=True, kw_only=True)
class _SentenceWithOffenseDate(NevadaSentence):
    """A sentence with an offense date, for conditions that compare dates."""

    offense_date: date | None = attr.ib(default=None)


_MAX_AND_MIN_TERMS = [
    ReductionTarget(term=TermType.MAX_TERM, when=None),
    ReductionTarget(term=TermType.MIN_TERM, when=None),
]


def _law_regime(
    *,
    term_multipliers: list[TermMultiplierRule],
    credit_rates: list[CreditRateRule],
    credit_caps: list[CreditCapRule],
    projected_dates: dict[str, list[DateFormulaRule]],
) -> LawRegime:
    return LawRegime(
        name="TEST_REGIME",
        description="Test regime",
        effective_date=date(2020, 1, 1),
        use_calendar_term_conversion=False,
        days_per_month=30.0,
        days_per_year=365.0,
        term_multipliers=term_multipliers,
        credit_rates=credit_rates,
        credit_caps=credit_caps,
        projected_dates=projected_dates,
    )


def _date_rule(*, name: str, formula: str, floor: DateFloor | None) -> DateFormulaRule:
    return DateFormulaRule(
        name=name,
        description=f"Computes {name}",
        when=None,
        formula=formula,
        floor=floor,
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
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "parole_eligibility_date": [
                        DateFormulaRule(
                            name="parole_eligibility_date",
                            description="Eligible once the adjusted min term is served",
                            when=None,
                            formula="earliest_start_date + adjusted_min_days",
                            floor=None,
                            statute=None,
                        )
                    ],
                    "release_date": [
                        DateFormulaRule(
                            name="release_date",
                            description=(
                                "Release once the adjusted max term is served, for max "
                                "terms over three years"
                            ),
                            when=ProjectedDateCondition(
                                values_condition=FieldCondition(
                                    field="unadjusted_max_days",
                                    operator=ConditionOperator.GREATER_THAN,
                                    value=1094,
                                ),
                                sentence_condition=None,
                            ),
                            formula="earliest_start_date + adjusted_max_days",
                            floor=None,
                            statute="Test Statute 3",
                        )
                    ],
                },
            ),
            regime,
        )

    def test_parse_credit_regime(self) -> None:
        shared_conditions = parse_state_conditions(
            fixtures.as_filepath("us_xx_rules/conditions.yaml"),
            sentence_class=NevadaSentence,
        )

        regime = LawRegime.from_yaml(
            fixtures.as_filepath("us_xx_law_regime_credits.yaml"),
            sentence_class=NevadaSentence,
            shared_conditions=shared_conditions,
        )

        self.assertEqual(
            LawRegime(
                name="US_XX_LAW_REGIME_CREDITS",
                description="Test regime that exercises every credit regime section.",
                effective_date=date(2020, 1, 1),
                use_calendar_term_conversion=True,
                days_per_month=30.44,
                # Not set in the file, so it gets the default.
                days_per_year=365.0,
                term_multipliers=[],
                credit_rates=[
                    CreditRateRule(
                        name="enhanced_credit_rate",
                        description="Enhanced credits for qualified offenders outside category A",
                        when=AllOf(
                            conditions=[
                                FieldCondition(
                                    field="qualified_for_enhanced_good_time",
                                    operator=ConditionOperator.EQUALS,
                                    value=True,
                                ),
                                FieldCondition(
                                    field="offense_category",
                                    operator=ConditionOperator.NOT_IN,
                                    value=[NevadaOffenseCategory.CATEGORY_A],
                                ),
                            ]
                        ),
                        rate=30.0,
                        applies_to=[
                            ReductionTarget(term=TermType.MAX_TERM, when=None),
                            ReductionTarget(term=TermType.MIN_TERM, when=None),
                        ],
                        statute="Test Statute 1(a)",
                    ),
                    CreditRateRule(
                        name="default_credit_rate",
                        description=(
                            "Credits for every other offender, off the max term, and "
                            "off the min term outside these offenses"
                        ),
                        when=None,
                        rate=20.0,
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
                credit_caps=[
                    CreditCapRule(
                        name="credit_cap",
                        description="Credits against the min term are capped at half the term",
                        when=None,
                        max_credit_days="sentence_length_days * 0.5",
                        binds=frozenset({TermType.MIN_TERM}),
                        statute=None,
                    )
                ],
                projected_dates={
                    "parole_eligibility_date": [
                        DateFormulaRule(
                            name="parole_eligibility_date",
                            description="Eligible once the adjusted min term is served",
                            when=None,
                            formula="earliest_start_date + adjusted_min_days",
                            floor=None,
                            statute=None,
                        )
                    ],
                    "release_date": [
                        DateFormulaRule(
                            name="early_release_date",
                            description="Early release for long category B sentences",
                            when=ProjectedDateCondition(
                                values_condition=FieldCondition(
                                    field="unadjusted_max_days",
                                    operator=ConditionOperator.GREATER_THAN,
                                    value=1094,
                                ),
                                sentence_condition=FieldCondition(
                                    field="offense_category",
                                    operator=ConditionOperator.EQUALS,
                                    value=NevadaOffenseCategory.CATEGORY_B,
                                ),
                            ),
                            formula="earliest_start_date + adjusted_max_days - 30",
                            floor=DateFloor(
                                date="parole_eligibility_date",
                                when=ProjectedDateCondition(
                                    values_condition=FieldCondition(
                                        field="adjusted_min_days",
                                        operator=ConditionOperator.LESS_THAN,
                                        value=365,
                                    ),
                                    sentence_condition=None,
                                ),
                            ),
                            statute=None,
                        ),
                        DateFormulaRule(
                            name="release_date",
                            description="Release once the adjusted max term is served",
                            when=None,
                            formula="earliest_start_date + adjusted_max_days",
                            floor=None,
                            statute=None,
                        ),
                    ],
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
        self.assertEqual([], regime.credit_rates)
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

    def test_parse_listed_projected_date_rule_missing_name_fails_schema(
        self,
    ) -> None:
        # The schema allows either a single rule or a list of named rules, so
        # jsonschema reports that release_date matches neither.
        with self.assertRaisesRegex(
            ValidationError,
            r"^\[\{'name': 'long_release_date', .*\{'description': 'Release once "
            r"the adjusted min term is served', 'formula': 'start_date \+ "
            r"adjusted_min_days'\}\] is not valid under any of the given schemas",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath(
                    "us_xx_law_regime_projected_date_rule_missing_name.yaml"
                ),
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

    def test_parse_regime_with_both_reductions(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[US_XX_LAW_REGIME_BOTH_REDUCTIONS\] has both "
            r"term_multipliers and "
            r"credit_rates\. A regime shortens sentences one way; remove one\.$",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_both_reductions.yaml"),
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
            r"^Invalid conditions in projected date \[parole_eligibility_date\] "
            r"entry \[parole_eligibility_date\]\. "
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
            r"^Invalid conditions in projected date \[parole_eligibility_date\] "
            r"entry \[parole_eligibility_date\]\. "
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
            sentence_class=_SentenceWithOffenseDate,
            shared_conditions={},
        )

        self.assertEqual(date(2020, 1, 1), regime.effective_date)
        self.assertEqual(
            [
                FieldCondition(
                    field="offense_date",
                    operator=ConditionOperator.BEFORE,
                    value=date(2019, 1, 1),
                ),
                FieldCondition(
                    field="offense_date",
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
                "offense_after_2018": FieldCondition(
                    field="offense_date",
                    operator=ConditionOperator.AFTER,
                    value=date(2018, 12, 31),
                )
            },
            parse_state_conditions(
                fixtures.as_filepath("us_xx_conditions_unquoted_dates.yaml"),
                sentence_class=_SentenceWithOffenseDate,
            ),
        )

    def test_parse_condition_on_start_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry "
            r"\[early_start_term_reduction\]\. They use the field \[start_date\]\. "
            r"Start dates are sentence-specific and change over time\. They "
            r"shouldn't be used in rules\. Instead, compare against the offense date "
            r"or the court sentencing date\.$",
        ):
            LawRegime.from_yaml(
                fixtures.as_filepath("us_xx_law_regime_start_date_condition.yaml"),
                sentence_class=NevadaSentence,
                shared_conditions={},
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

    def test_schema_credit_cap_terms_match_python_enum(self) -> None:
        with open(LAW_REGIME_SCHEMA_PATH, encoding="utf-8") as f:
            schema = json.load(f)

        self.assertEqual(
            [e.value for e in TermType],
            schema["$defs"]["credit_cap"]["properties"]["binds"]["items"]["enum"],
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
                credit_rates=[],
                credit_caps=[],
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
                credit_rates=[],
                credit_caps=[],
                projected_dates={},
            )

    def test_credit_rates_without_unconditional_last_rule(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has when conditions on "
            r"\[category_e_credit_rate\], the last rule in credit_rates, so "
            r"sentences that match no rule get no reduction\. Add a last rule "
            r"without when conditions for every other sentence\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[
                    CreditRateRule(
                        name="category_e_credit_rate",
                        description="Credits for category E offenses",
                        when=_CATEGORY_E,
                        rate=20.0,
                        applies_to=[
                            ReductionTarget(term=TermType.MAX_TERM, when=None),
                            ReductionTarget(term=TermType.MIN_TERM, when=None),
                        ],
                        statute=None,
                    )
                ],
                credit_caps=[],
                projected_dates={},
            )

    def test_credit_caps_without_credit_rates(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has credit_caps but no credit_rates, so "
            r"there are no credits to cap\. Add credit_rates or remove "
            r"credit_caps\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[
                    CreditCapRule(
                        name="credit_cap",
                        description="Credits against the min term are capped at half the term",
                        when=None,
                        max_credit_days="sentence_length_days * 0.5",
                        binds=frozenset({TermType.MIN_TERM}),
                        statute=None,
                    )
                ],
                projected_dates={},
            )

    def test_credit_caps_without_unconditional_last_rule(self) -> None:
        # Unlike term_multipliers and credit_rates, credit_caps can leave
        # sentences that match no rule uncapped, so this regime loads.
        _law_regime(
            term_multipliers=[],
            credit_rates=[
                CreditRateRule(
                    name="stat_time",
                    description="Stat time for every offense",
                    when=None,
                    rate=20.0,
                    applies_to=_MAX_AND_MIN_TERMS,
                    statute=None,
                )
            ],
            credit_caps=[
                CreditCapRule(
                    name="category_e_credit_cap",
                    description="Credits against the min term are capped for category E",
                    when=_CATEGORY_E,
                    max_credit_days="sentence_length_days * 0.5",
                    binds=frozenset({TermType.MIN_TERM}),
                    statute=None,
                )
            ],
            projected_dates={},
        )

    def test_unconditional_credit_cap_before_others(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has rule \[credit_cap\] in credit_caps "
            r"with no when conditions, so the rules after it never apply\. Move it "
            r"to the end of credit_caps\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[
                    CreditRateRule(
                        name="stat_time",
                        description="Stat time for every offense",
                        when=None,
                        rate=20.0,
                        applies_to=_MAX_AND_MIN_TERMS,
                        statute=None,
                    )
                ],
                credit_caps=[
                    CreditCapRule(
                        name="credit_cap",
                        description="Credits against the min term are capped at half the term",
                        when=None,
                        max_credit_days="sentence_length_days * 0.5",
                        binds=frozenset({TermType.MIN_TERM}),
                        statute=None,
                    ),
                    CreditCapRule(
                        name="category_e_credit_cap",
                        description="Credits against the min term are capped for category E",
                        when=_CATEGORY_E,
                        max_credit_days="sentence_length_days * 0.25",
                        binds=frozenset({TermType.MIN_TERM}),
                        statute=None,
                    ),
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

    def test_credit_rate_with_repeated_term(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Credit rate \[stat_time\] lists the terms \['max_term'\] more than "
            r"once in applies_to\. List each term once, and combine its conditions "
            r"with any_of or all_of\.$",
        ):
            CreditRateRule(
                name="stat_time",
                description="Stat time for every offense",
                when=None,
                rate=20.0,
                applies_to=[
                    ReductionTarget(term=TermType.MAX_TERM, when=None),
                    ReductionTarget(term=TermType.MAX_TERM, when=_CATEGORY_E),
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
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "parole_eligibility_date": [
                        _date_rule(
                            name="parole_eligibility_date",
                            formula="earliest_start_date + adjusted_min_days",
                            floor=None,
                        )
                    ]
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
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "parole_eligibility_date": [
                        _date_rule(
                            name="parole_eligibility_date",
                            formula="earliest_start_date + adjusted_minimum_days",
                            floor=None,
                        )
                    ]
                },
            )

    def test_formula_with_duration_function(self) -> None:
        regime = _law_regime(
            term_multipliers=[],
            credit_rates=[],
            credit_caps=[],
            projected_dates={
                "mandatory_parole_release_date": [
                    _date_rule(
                        name="mandatory_parole_release_date",
                        formula="projected_expiration_date - months(12)",
                        floor=None,
                    )
                ],
                "projected_expiration_date": [
                    _date_rule(
                        name="projected_expiration_date",
                        formula="earliest_start_date + adjusted_max_days",
                        floor=None,
                    )
                ],
            },
        )

        self.assertEqual(
            ["projected_expiration_date", "mandatory_parole_release_date"],
            regime.projected_date_evaluation_order,
        )

    def test_malformed_formula(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[earliest_start_date \+\] in rule "
            r"\[parole_eligibility_date\] is not valid syntax\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "parole_eligibility_date": [
                        _date_rule(
                            name="parole_eligibility_date",
                            formula="earliest_start_date +",
                            floor=None,
                        )
                    ]
                },
            )

    def test_formula_with_unsupported_syntax(self) -> None:
        # start_date.year is the unsupported syntax that raises
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[earliest_start_date\.year \+ adjusted_min_days\] in rule "
            r"\[parole_eligibility_date\] uses unsupported syntax \[Attribute\]\. "
            r"Expressions can use numbers, variable names, parentheses, unary minus, "
            r"the operators \+, -, \*, /, and //, and the duration functions "
            r"years\(n\) and months\(n\)\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "parole_eligibility_date": [
                        _date_rule(
                            name="parole_eligibility_date",
                            formula="earliest_start_date.year + adjusted_min_days",
                            floor=None,
                        )
                    ]
                },
            )

    def test_floor_on_own_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Rule \[release_date\] has a floor on its own date \[release_date\]\. "
            r"A floor must be a different date\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "release_date": [
                        _date_rule(
                            name="release_date",
                            formula="earliest_start_date + adjusted_max_days",
                            floor=DateFloor(date="release_date", when=None),
                        )
                    ]
                },
            )

    def test_floor_on_value_that_is_not_a_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Rule \[release_date\] has a floor on \[adjusted_min_days\], which is "
            r"not a date\. Available dates: "
            r"\['earliest_start_date', 'max_ped', 'release_date'\]\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "release_date": [
                        _date_rule(
                            name="release_date",
                            formula="earliest_start_date + adjusted_max_days",
                            floor=DateFloor(date="adjusted_min_days", when=None),
                        )
                    ]
                },
            )

    def test_formula_on_own_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has projected dates that depend on each "
            r"other in a cycle, where each date's formula or floor uses the next: "
            r"\[release_date -> release_date\]\. Change the "
            r"formulas or floors so no date depends on itself\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "release_date": [
                        _date_rule(
                            name="release_date",
                            formula="release_date + 1",
                            floor=None,
                        )
                    ]
                },
            )

    def test_formulas_in_a_cycle(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has projected dates that depend on each "
            r"other in a cycle, where each date's formula or floor uses the next: "
            r"\[release_date -> parole_eligibility_date -> release_date\]\. Change "
            r"the formulas or floors so no date depends on itself\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "release_date": [
                        _date_rule(
                            name="release_date",
                            formula="parole_eligibility_date + 365",
                            floor=None,
                        )
                    ],
                    "parole_eligibility_date": [
                        _date_rule(
                            name="parole_eligibility_date",
                            formula="release_date - 365",
                            floor=None,
                        )
                    ],
                },
            )

    def test_floor_in_a_cycle(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has projected dates that depend on each "
            r"other in a cycle, where each date's formula or floor uses the next: "
            r"\[release_date -> parole_eligibility_date -> release_date\]\. Change "
            r"the formulas or floors so no date depends on itself\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "release_date": [
                        _date_rule(
                            name="release_date",
                            formula="parole_eligibility_date + 365",
                            floor=None,
                        )
                    ],
                    "parole_eligibility_date": [
                        _date_rule(
                            name="parole_eligibility_date",
                            formula="earliest_start_date + adjusted_min_days",
                            floor=DateFloor(date="release_date", when=None),
                        )
                    ],
                },
            )

    def test_projected_date_named_like_calculation_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Law regime \[TEST_REGIME\] has a projected date named "
            r"\[earliest_start_date\], "
            r"which is already the name of a calculation value\. Rename the "
            r"projected date\.$",
        ):
            _law_regime(
                term_multipliers=[],
                credit_rates=[],
                credit_caps=[],
                projected_dates={
                    "earliest_start_date": [
                        _date_rule(
                            name="earliest_start_date",
                            formula="earliest_start_date + 1",
                            floor=None,
                        )
                    ]
                },
            )


class TestProjectedDateEvaluationOrder(unittest.TestCase):
    """Tests for LawRegime.projected_date_evaluation_order."""

    def test_dates_come_after_the_dates_they_use(self) -> None:
        regime = _law_regime(
            term_multipliers=[],
            credit_rates=[],
            credit_caps=[],
            projected_dates={
                "mandatory_release_date": [
                    _date_rule(
                        name="mandatory_release_date",
                        formula="release_date - 365",
                        floor=None,
                    )
                ],
                "release_date": [
                    _date_rule(
                        name="release_date",
                        formula="earliest_start_date + adjusted_max_days",
                        floor=DateFloor(date="parole_eligibility_date", when=None),
                    )
                ],
                "parole_eligibility_date": [
                    _date_rule(
                        name="parole_eligibility_date",
                        formula="earliest_start_date + adjusted_min_days",
                        floor=None,
                    )
                ],
            },
        )

        self.assertEqual(
            ["parole_eligibility_date", "release_date", "mandatory_release_date"],
            regime.projected_date_evaluation_order,
        )


class TestRuleValidation(unittest.TestCase):
    """Tests for the validation on individual rules."""

    def test_cap_that_binds_no_terms(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Credit cap \[credit_cap\] binds no terms\. List one or more of "
            r"\['min_term', 'max_term'\] in binds\.$",
        ):
            CreditCapRule(
                name="credit_cap",
                description="Credits against the min term are capped at half the term",
                when=None,
                max_credit_days="sentence_length_days * 0.5",
                binds=frozenset(),
                statute=None,
            )

    def test_cap_with_unknown_variable(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Credit cap \[credit_cap\] uses \['term_days'\] in max_credit_days, "
            r"but it can only use \[sentence_length_days\]\.$",
        ):
            CreditCapRule(
                name="credit_cap",
                description="Credits against the min term are capped at half the term",
                when=None,
                max_credit_days="term_days * 0.5",
                binds=frozenset({TermType.MIN_TERM}),
                statute=None,
            )

    def test_cap_with_unsupported_syntax(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[min\(sentence_length_days, 365\)\] in rule "
            r"\[credit_cap\] calls \[min\], but the only functions are "
            r"\['years', 'months'\]\.$",
        ):
            CreditCapRule(
                name="credit_cap",
                description="Credits against the min term are capped at a year",
                when=None,
                max_credit_days="min(sentence_length_days, 365)",
                binds=frozenset({TermType.MIN_TERM}),
                statute=None,
            )


class TestExpressionVariableNames(unittest.TestCase):
    """Tests for expression_variable_names."""

    def test_supported_syntax(self) -> None:
        self.assertEqual(
            {"adjusted_max_days", "jail_credit_days"},
            expression_variable_names(
                expression="-(adjusted_max_days - jail_credit_days) * 0.58 / 2 // 1",
                rule_name="test_rule",
            ),
        )

    def test_duration_function(self) -> None:
        self.assertEqual(
            {"projected_expiration_date"},
            expression_variable_names(
                expression="projected_expiration_date - months(12)",
                rule_name="test_rule",
            ),
        )

    def test_duration_function_with_variable_argument(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[earliest_start_date \+ years\(unadjusted_max_years\)\] "
            r"in rule \[test_rule\] calls \[years\(unadjusted_max_years\)\], "
            r"but years\(\) takes one whole number, e\.g\. years\(12\)\.$",
        ):
            expression_variable_names(
                expression="earliest_start_date + years(unadjusted_max_years)",
                rule_name="test_rule",
            )

    def test_attribute(self) -> None:
        # start_date.year is the unsupported syntax that raises
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[earliest_start_date\.year \+ 1\] in rule \[test_rule\] "
            r"uses unsupported syntax \[Attribute\]\. "
            r"Expressions can use numbers, variable names, parentheses, unary minus, "
            r"the operators \+, -, \*, /, and //, and the duration functions "
            r"years\(n\) and months\(n\)\.$",
        ):
            expression_variable_names(
                expression="earliest_start_date.year + 1", rule_name="test_rule"
            )

    def test_unsupported_operator(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[adjusted_max_days \*\* 2\] in rule \[test_rule\] uses "
            r"unsupported syntax \[Pow\]\. "
            r"Expressions can use numbers, variable names, parentheses, unary minus, "
            r"the operators \+, -, \*, /, and //, and the duration functions "
            r"years\(n\) and months\(n\)\.$",
        ):
            expression_variable_names(
                expression="adjusted_max_days ** 2", rule_name="test_rule"
            )

    def test_comparison(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[adjusted_max_days > 1094\] in rule \[test_rule\] uses "
            r"unsupported syntax \[Compare\]\. "
            r"Expressions can use numbers, variable names, parentheses, unary minus, "
            r"the operators \+, -, \*, /, and //, and the duration functions "
            r"years\(n\) and months\(n\)\.$",
        ):
            expression_variable_names(
                expression="adjusted_max_days > 1094", rule_name="test_rule"
            )

    def test_string_constant(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \['2020-01-01'\] in rule \[test_rule\] has constant "
            r"\['2020-01-01'\]; only numeric constants are supported\.$",
        ):
            expression_variable_names(expression="'2020-01-01'", rule_name="test_rule")

    def test_boolean_constant(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[adjusted_max_days \* True\] in rule \[test_rule\] has "
            r"constant \[True\]; only numeric constants are supported\.$",
        ):
            expression_variable_names(
                expression="adjusted_max_days * True", rule_name="test_rule"
            )

    def test_infinite_constant(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Expression \[adjusted_max_days \* 1e999\] in rule \[test_rule\] has "
            r"constant \[inf\], but expressions only support finite numbers\.$",
        ):
            expression_variable_names(
                expression="adjusted_max_days * 1e999", rule_name="test_rule"
            )

    def test_huge_integer_constant(self) -> None:
        # An integer too large to convert to a float is a valid constant and must
        # not raise OverflowError at load.
        self.assertEqual(
            {"adjusted_max_days"},
            expression_variable_names(
                expression=f"adjusted_max_days * {'9' * 400}", rule_name="test_rule"
            ),
        )


if __name__ == "__main__":
    unittest.main()
