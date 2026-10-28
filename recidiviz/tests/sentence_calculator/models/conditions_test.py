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
"""Tests for recidiviz/sentence_calculator/models/conditions.py."""

import io
import unittest
from datetime import date, datetime

import attr

from recidiviz.sentence_calculator.models.conditions import (
    AllOf,
    AnyOf,
    ConditionOperator,
    ConditionParser,
    FieldCondition,
    parse_named_conditions,
    sentence_condition_field_types,
)
from recidiviz.sentence_calculator.models.inputs import SentenceType
from recidiviz.sentence_calculator.models.state_inputs.inputs_nv import (
    NevadaOffenseCategory,
    NevadaOffenseGroup,
    NevadaSentence,
)
from recidiviz.utils.yaml_dict import YAMLDict

_NEVADA_FIELD_TYPES = sentence_condition_field_types(NevadaSentence)
_PARSER = ConditionParser(field_types=_NEVADA_FIELD_TYPES, named_conditions={})
_LOCATION = "term_multipliers entry [standard]"


@attr.define(frozen=True, kw_only=True)
class _SentenceWithListField(NevadaSentence):
    """A sentence with a list field, which conditions can't compare."""

    charge_ids: list[str] | None = attr.ib(default=None)


@attr.define(frozen=True, kw_only=True)
class _SentenceWithDatetimeField(NevadaSentence):
    """A sentence with a datetime field, which conditions can't compare."""

    released_at: datetime | None = attr.ib(default=None)


@attr.define(frozen=True, kw_only=True)
class _SentenceWithUnionField(NevadaSentence):
    """A sentence with a field of several types, which conditions can't compare."""

    docket_number: int | str | None = attr.ib(default=None)


class TestSentenceConditionFieldTypes(unittest.TestCase):
    """Tests for sentence_condition_field_types."""

    def test_nevada_sentence_fields(self) -> None:
        self.assertEqual(
            {
                "sentence_id": str,
                "sentence_type": SentenceType,
                "start_date": date,
                "parent_sentence_id": str,
                "offense_category": NevadaOffenseCategory,
                "offense_group": NevadaOffenseGroup,
                "offense_id": int,
                "qualified_for_enhanced_good_time": bool,
            },
            _NEVADA_FIELD_TYPES,
        )

    def test_union_field_is_left_out(self) -> None:
        self.assertEqual(
            _NEVADA_FIELD_TYPES, sentence_condition_field_types(_SentenceWithUnionField)
        )

    def test_list_field_is_left_out(self) -> None:
        self.assertEqual(
            _NEVADA_FIELD_TYPES, sentence_condition_field_types(_SentenceWithListField)
        )

    def test_datetime_field_is_left_out(self) -> None:
        """A datetime is also a date, but a condition's date value would never
        equal it."""
        self.assertEqual(
            _NEVADA_FIELD_TYPES,
            sentence_condition_field_types(_SentenceWithDatetimeField),
        )


class TestConditionParser(unittest.TestCase):
    """Tests for ConditionParser.parse."""

    def test_parse_every_matcher_form(self) -> None:
        condition = _PARSER.parse(
            YAMLDict(
                {
                    "qualified_for_enhanced_good_time": True,
                    "offense_category": ["A", "B"],
                    "offense_id": {"greater_than": 1000},
                    "start_date": {"between_inclusive": ["2007-07-01", "2014-06-30"]},
                    "any_of": [
                        {"offense_group": "SEX"},
                        {"offense_group": {"not_equals": "OTHER"}},
                    ],
                }
            ),
            location=_LOCATION,
        )

        self.assertEqual(
            AllOf(
                conditions=[
                    FieldCondition(
                        field="qualified_for_enhanced_good_time",
                        operator=ConditionOperator.EQUALS,
                        value=True,
                    ),
                    FieldCondition(
                        field="offense_category",
                        operator=ConditionOperator.IN,
                        value=[
                            NevadaOffenseCategory.CATEGORY_A,
                            NevadaOffenseCategory.CATEGORY_B,
                        ],
                    ),
                    FieldCondition(
                        field="offense_id",
                        operator=ConditionOperator.GREATER_THAN,
                        value=1000,
                    ),
                    FieldCondition(
                        field="start_date",
                        operator=ConditionOperator.BETWEEN_INCLUSIVE,
                        value=[date(2007, 7, 1), date(2014, 6, 30)],
                    ),
                    AnyOf(
                        conditions=[
                            FieldCondition(
                                field="offense_group",
                                operator=ConditionOperator.EQUALS,
                                value=NevadaOffenseGroup.SEX,
                            ),
                            FieldCondition(
                                field="offense_group",
                                operator=ConditionOperator.NOT_EQUALS,
                                value=NevadaOffenseGroup.OTHER,
                            ),
                        ]
                    ),
                ]
            ),
            condition,
        )

    def test_parse_inclusive_operators(self) -> None:
        condition = _PARSER.parse(
            YAMLDict(
                {
                    "offense_id": {"greater_than_or_equal": 1000},
                    "start_date": {"on_or_after": "2014-07-01"},
                }
            ),
            location=_LOCATION,
        )

        self.assertEqual(
            AllOf(
                conditions=[
                    FieldCondition(
                        field="offense_id",
                        operator=ConditionOperator.GREATER_THAN_OR_EQUAL,
                        value=1000,
                    ),
                    FieldCondition(
                        field="start_date",
                        operator=ConditionOperator.ON_OR_AFTER,
                        value=date(2014, 7, 1),
                    ),
                ]
            ),
            condition,
        )

    def test_parse_all_of(self) -> None:
        sex_offense = FieldCondition(
            field="offense_group",
            operator=ConditionOperator.EQUALS,
            value=NevadaOffenseGroup.SEX,
        )
        category_a = FieldCondition(
            field="offense_category",
            operator=ConditionOperator.EQUALS,
            value=NevadaOffenseCategory.CATEGORY_A,
        )
        parser = ConditionParser(
            field_types=_NEVADA_FIELD_TYPES,
            named_conditions={"sex_offense": sex_offense, "category_a": category_a},
        )

        self.assertEqual(
            AllOf(conditions=[sex_offense, category_a]),
            parser.parse(
                YAMLDict(
                    {
                        "all_of": [
                            {"condition": "sex_offense"},
                            {"condition": "category_a"},
                        ]
                    }
                ),
                location=_LOCATION,
            ),
        )

    def test_parse_named_condition(self) -> None:
        sex_offense = FieldCondition(
            field="offense_group",
            operator=ConditionOperator.EQUALS,
            value=NevadaOffenseGroup.SEX,
        )
        parser = ConditionParser(
            field_types=_NEVADA_FIELD_TYPES,
            named_conditions={"sex_offense": sex_offense},
        )

        self.assertEqual(
            sex_offense,
            parser.parse(YAMLDict({"condition": "sex_offense"}), location=_LOCATION),
        )

    def test_parse_unknown_named_condition(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. They refer "
            r"to the named condition \[sex_offense\], which is not defined\. Defined "
            r"conditions: \[\]\.$",
        ):
            _PARSER.parse(YAMLDict({"condition": "sex_offense"}), location=_LOCATION)

    def test_parse_named_condition_where_not_allowed(self) -> None:
        parser = ConditionParser(
            field_types={"unadjusted_max_days": int}, named_conditions=None
        )
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. They refer "
            r"to the named condition \[sex_offense\], but named conditions can only "
            r"be used in conditions on sentence fields\.$",
        ):
            parser.parse(YAMLDict({"condition": "sex_offense"}), location=_LOCATION)

    def test_parse_unknown_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. They use "
            r"the field \[offense_type\], which conditions here can't compare\. "
            r"Fields they can compare: \['offense_category', 'offense_group', ",
        ):
            _PARSER.parse(YAMLDict({"offense_type": "SEX"}), location=_LOCATION)

    def test_parse_empty_conditions(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"conditions are empty\.$",
        ):
            _PARSER.parse(YAMLDict({}), location=_LOCATION)

    def test_parse_empty_all_of(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"\[all_of\] list is empty\.$",
        ):
            _PARSER.parse(YAMLDict({"all_of": []}), location=_LOCATION)

    def test_parse_empty_any_of(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"\[any_of\] list is empty\.$",
        ):
            _PARSER.parse(YAMLDict({"any_of": []}), location=_LOCATION)

    def test_parse_field_without_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[offense_id\] has no value\. Set it to a value, a "
            r"list of values, or a one-key mapping naming an operator\.$",
        ):
            _PARSER.parse(YAMLDict({"offense_id": None}), location=_LOCATION)

    def test_parse_unknown_operator(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[offense_id\] uses unknown operator \[at_least\]\. "
            r"Operators: ",
        ):
            _PARSER.parse(
                YAMLDict({"offense_id": {"at_least": 1000}}), location=_LOCATION
            )

    def test_parse_two_operators_for_one_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[offense_id\] must name exactly one operator\. "
            r"Found \['greater_than', 'less_than'\]\.$",
        ):
            _PARSER.parse(
                YAMLDict({"offense_id": {"greater_than": 1000, "less_than": 2000}}),
                location=_LOCATION,
            )

    def test_parse_value_of_wrong_type(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[qualified_for_enhanced_good_time\] has value "
            r"\['yes'\], but the field is a \[bool\]\.$",
        ):
            _PARSER.parse(
                YAMLDict({"qualified_for_enhanced_good_time": "yes"}),
                location=_LOCATION,
            )

    def test_parse_numeric_operator_on_enum_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[offense_category\] uses operator \[greater_than\], "
            r"which only compares numbers, but the field is a "
            r"\[NevadaOffenseCategory\]\.$",
        ):
            _PARSER.parse(
                YAMLDict({"offense_category": {"greater_than": "B"}}),
                location=_LOCATION,
            )

    def test_parse_date_operator_on_number_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[offense_id\] uses operator \[before\], which only "
            r"compares dates, but the field is a \[int\]\.$",
        ):
            _PARSER.parse(
                YAMLDict({"offense_id": {"before": "2007-07-01"}}), location=_LOCATION
            )

    def test_parse_date_that_is_not_iso(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[start_date\] has value \[July 1, 2007\], which is "
            r"not an ISO date\.$",
        ):
            _PARSER.parse(
                YAMLDict({"start_date": {"before": "July 1, 2007"}}),
                location=_LOCATION,
            )

    def test_parse_datetime_for_date_field(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[start_date\] has value \[2007-07-01 00:00:00\], "
            r"which includes a time, but the field is a date\. Write the date without "
            r"a time, e\.g\. \[2007-07-01\]\.$",
        ):
            _PARSER.parse(
                YAMLDict.from_io(
                    io.StringIO("start_date: {before: 2007-07-01 00:00:00}")
                ),
                location=_LOCATION,
            )

    def test_parse_between_inclusive_with_one_date(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[start_date\] uses operator \[between_inclusive\], "
            r"which requires a two-element list\. Found \[\['2007-07-01'\]\]\.$",
        ):
            _PARSER.parse(
                YAMLDict({"start_date": {"between_inclusive": ["2007-07-01"]}}),
                location=_LOCATION,
            )

    def test_parse_between_inclusive_with_start_after_end(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[start_date\] uses operator \[between_inclusive\] "
            r"with start \[2014-06-30\] after end \[2007-07-01\], so it can never "
            r"hold\. List the earlier date first\.$",
        ):
            _PARSER.parse(
                YAMLDict(
                    {"start_date": {"between_inclusive": ["2014-06-30", "2007-07-01"]}}
                ),
                location=_LOCATION,
            )

    def test_parse_not_in_without_list(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[offense_category\] uses operator \[not_in\], which "
            r"requires a non-empty list\. Found \['A'\]\.$",
        ):
            _PARSER.parse(
                YAMLDict({"offense_category": {"not_in": "A"}}), location=_LOCATION
            )

    def test_parse_empty_list(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[offense_category\] uses operator \[in\], which "
            r"requires a non-empty list\. Found \[\[\]\]\.$",
        ):
            _PARSER.parse(YAMLDict({"offense_category": []}), location=_LOCATION)

    def test_parse_not_in_with_empty_list(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in term_multipliers entry \[standard\]\. The "
            r"condition on field \[offense_category\] uses operator \[not_in\], which "
            r"requires a non-empty list\. Found \[\[\]\]\.$",
        ):
            _PARSER.parse(
                YAMLDict({"offense_category": {"not_in": []}}), location=_LOCATION
            )


class TestParseNamedConditions(unittest.TestCase):
    """Tests for parse_named_conditions."""

    def test_condition_referring_to_one_defined_after_it(self) -> None:
        conditions = parse_named_conditions(
            YAMLDict(
                {
                    "carve_outs": {"any_of": [{"condition": "sex_offense"}]},
                    "sex_offense": {"offense_group": "SEX"},
                }
            ),
            field_types=_NEVADA_FIELD_TYPES,
            inherited_conditions={},
        )

        sex_offense = FieldCondition(
            field="offense_group",
            operator=ConditionOperator.EQUALS,
            value=NevadaOffenseGroup.SEX,
        )
        self.assertEqual(
            {
                "carve_outs": AnyOf(conditions=[sex_offense]),
                "sex_offense": sex_offense,
            },
            conditions,
        )

    def test_conditions_in_a_cycle(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Invalid conditions in condition \[first\]\. Invalid conditions in "
            r"condition \[second\]\. Named conditions refer to each other in a cycle: "
            r"\[first -> second -> first\]\.$",
        ):
            parse_named_conditions(
                YAMLDict(
                    {
                        "first": {"condition": "second"},
                        "second": {"condition": "first"},
                    }
                ),
                field_types=_NEVADA_FIELD_TYPES,
                inherited_conditions={},
            )

    def test_condition_referring_to_inherited_condition(self) -> None:
        sex_offense = FieldCondition(
            field="offense_group",
            operator=ConditionOperator.EQUALS,
            value=NevadaOffenseGroup.SEX,
        )

        conditions = parse_named_conditions(
            YAMLDict(
                {
                    "category_a_sex_offense": {
                        "all_of": [
                            {"condition": "sex_offense"},
                            {"offense_category": "A"},
                        ]
                    },
                }
            ),
            field_types=_NEVADA_FIELD_TYPES,
            inherited_conditions={"sex_offense": sex_offense},
        )

        self.assertEqual(
            {
                "sex_offense": sex_offense,
                "category_a_sex_offense": AllOf(
                    conditions=[
                        sex_offense,
                        FieldCondition(
                            field="offense_category",
                            operator=ConditionOperator.EQUALS,
                            value=NevadaOffenseCategory.CATEGORY_A,
                        ),
                    ]
                ),
            },
            conditions,
        )

    def test_condition_redefining_inherited_condition(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^The conditions \['sex_offense'\] are already defined in the state's "
            r"shared conditions\. Rename them or use the shared definitions\.$",
        ):
            parse_named_conditions(
                YAMLDict({"sex_offense": {"offense_group": "VIOLENCE"}}),
                field_types=_NEVADA_FIELD_TYPES,
                inherited_conditions={
                    "sex_offense": FieldCondition(
                        field="offense_group",
                        operator=ConditionOperator.EQUALS,
                        value=NevadaOffenseGroup.SEX,
                    )
                },
            )


class TestConditionIsMet(unittest.TestCase):
    """Tests for is_met on FieldCondition, AllOf, and AnyOf."""

    def test_operators(self) -> None:
        values = {
            "offense_id": 1500,
            "start_date": date(2010, 1, 1),
            "offense_group": NevadaOffenseGroup.SEX,
        }
        for operator, field, condition_value, expected in [
            (ConditionOperator.EQUALS, "offense_id", 1500, True),
            (ConditionOperator.NOT_EQUALS, "offense_id", 1500, False),
            (ConditionOperator.GREATER_THAN, "offense_id", 1500, False),
            (ConditionOperator.GREATER_THAN_OR_EQUAL, "offense_id", 1500, True),
            (ConditionOperator.LESS_THAN, "offense_id", 1501, True),
            (ConditionOperator.LESS_THAN_OR_EQUAL, "offense_id", 1500, True),
            (ConditionOperator.IN, "offense_group", [NevadaOffenseGroup.SEX], True),
            (
                ConditionOperator.NOT_IN,
                "offense_group",
                [NevadaOffenseGroup.SEX],
                False,
            ),
            (ConditionOperator.BEFORE, "start_date", date(2010, 1, 1), False),
            (ConditionOperator.ON_OR_BEFORE, "start_date", date(2010, 1, 1), True),
            (ConditionOperator.AFTER, "start_date", date(2009, 12, 31), True),
            (ConditionOperator.ON_OR_AFTER, "start_date", date(2010, 1, 1), True),
            (
                ConditionOperator.BETWEEN_INCLUSIVE,
                "start_date",
                [date(2009, 1, 1), date(2010, 1, 1)],
                True,
            ),
        ]:
            with self.subTest(operator=operator):
                self.assertEqual(
                    expected,
                    FieldCondition(
                        field=field, operator=operator, value=condition_value
                    ).is_met(values.__getitem__),
                )

    def test_all_of_and_any_of(self) -> None:
        is_sex_offense = FieldCondition(
            field="offense_group",
            operator=ConditionOperator.EQUALS,
            value=NevadaOffenseGroup.SEX,
        )
        is_category_a = FieldCondition(
            field="offense_category",
            operator=ConditionOperator.EQUALS,
            value=NevadaOffenseCategory.CATEGORY_A,
        )
        values = {
            "offense_group": NevadaOffenseGroup.SEX,
            "offense_category": NevadaOffenseCategory.CATEGORY_C,
        }

        self.assertFalse(
            AllOf(conditions=[is_sex_offense, is_category_a]).is_met(values.__getitem__)
        )
        self.assertTrue(
            AnyOf(conditions=[is_sex_offense, is_category_a]).is_met(values.__getitem__)
        )

    def test_ordered_operator_on_missing_value(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"^Condition compares field \[offense_id\] with operator \[greater_than\], "
            r"but the field has no value\.$",
        ):
            FieldCondition(
                field="offense_id", operator=ConditionOperator.GREATER_THAN, value=1000
            ).is_met({"offense_id": None}.__getitem__)

    def test_equals_and_in_on_missing_value(self) -> None:
        self.assertFalse(
            FieldCondition(
                field="offense_id", operator=ConditionOperator.EQUALS, value=1000
            ).is_met({"offense_id": None}.__getitem__)
        )
        self.assertFalse(
            FieldCondition(
                field="offense_id", operator=ConditionOperator.IN, value=[1000]
            ).is_met({"offense_id": None}.__getitem__)
        )

    def test_not_equals_and_not_in_on_missing_value(self) -> None:
        self.assertTrue(
            FieldCondition(
                field="offense_id", operator=ConditionOperator.NOT_EQUALS, value=1000
            ).is_met({"offense_id": None}.__getitem__)
        )
        self.assertTrue(
            FieldCondition(
                field="offense_id", operator=ConditionOperator.NOT_IN, value=[1000]
            ).is_met({"offense_id": None}.__getitem__)
        )

    def test_any_of_checks_conditions_after_one_holds(self) -> None:
        values = {"offense_group": NevadaOffenseGroup.SEX, "offense_id": None}
        with self.assertRaisesRegex(
            ValueError,
            r"^Condition compares field \[offense_id\] with operator "
            r"\[greater_than\], but the field has no value\.$",
        ):
            AnyOf(
                conditions=[
                    FieldCondition(
                        field="offense_group",
                        operator=ConditionOperator.EQUALS,
                        value=NevadaOffenseGroup.SEX,
                    ),
                    FieldCondition(
                        field="offense_id",
                        operator=ConditionOperator.GREATER_THAN,
                        value=1000,
                    ),
                ]
            ).is_met(values.__getitem__)

    def test_all_of_checks_conditions_after_one_fails(self) -> None:
        values = {"offense_group": NevadaOffenseGroup.SEX, "offense_id": None}
        with self.assertRaisesRegex(
            ValueError,
            r"^Condition compares field \[offense_id\] with operator "
            r"\[greater_than\], but the field has no value\.$",
        ):
            AllOf(
                conditions=[
                    FieldCondition(
                        field="offense_group",
                        operator=ConditionOperator.EQUALS,
                        value=NevadaOffenseGroup.DRUG,
                    ),
                    FieldCondition(
                        field="offense_id",
                        operator=ConditionOperator.GREATER_THAN,
                        value=1000,
                    ),
                ]
            ).is_met(values.__getitem__)


class TestFieldCondition(unittest.TestCase):
    """Tests for FieldCondition's validation."""

    def test_value_of_unsupported_type(self) -> None:
        with self.assertRaisesRegex(
            TypeError,
            r"^Field \[value\] on \[FieldCondition\] must be a bool, int, float, "
            r"str, date, or enum, or a tuple of them\. Found \[\(1500, None\)\]\.$",
        ):
            FieldCondition(
                field="offense_id", operator=ConditionOperator.IN, value=[1500, None]
            )


class TestConditionHashing(unittest.TestCase):
    """Tests that conditions holding lists of values or conditions are hashable."""

    def test_parsed_condition_is_hashable(self) -> None:
        condition = _PARSER.parse(
            YAMLDict(
                {
                    "offense_category": ["A", "B"],
                    "start_date": {"between_inclusive": ["2007-07-01", "2014-06-30"]},
                }
            ),
            location=_LOCATION,
        )

        self.assertEqual(
            AllOf(
                conditions=(
                    FieldCondition(
                        field="offense_category",
                        operator=ConditionOperator.IN,
                        value=(
                            NevadaOffenseCategory.CATEGORY_A,
                            NevadaOffenseCategory.CATEGORY_B,
                        ),
                    ),
                    FieldCondition(
                        field="start_date",
                        operator=ConditionOperator.BETWEEN_INCLUSIVE,
                        value=(date(2007, 7, 1), date(2014, 6, 30)),
                    ),
                )
            ),
            condition,
        )
        self.assertEqual({condition}, {condition, condition})


if __name__ == "__main__":
    unittest.main()
