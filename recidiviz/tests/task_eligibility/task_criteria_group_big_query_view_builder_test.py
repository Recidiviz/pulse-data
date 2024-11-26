# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for TaskCriteriaGroupBigQueryViewBuilder classes."""
import unittest

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    AndTaskCriteriaGroup,
    InvertedTaskCriteriaBigQueryViewBuilder,
    OrTaskCriteriaGroup,
)

CRITERIA_1_STATE_AGNOSTIC = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name="MY_STATE_AGNOSTIC_CRITERIA",
    description="A state-agnostic criteria",
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.my_sessions_materialized`",
    meets_criteria_default=True,
    reasons_fields=[],
)

CRITERIA_2_STATE_AGNOSTIC = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name="ANOTHER_STATE_AGNOSTIC_CRITERIA",
    description="Another state-agnostic criteria",
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized`",
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="fees_owed",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="Amount of fees owed",
        ),
    ],
)

CRITERIA_3_STATE_SPECIFIC = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name="US_HI_CRITERIA_3",
    description="A criteria for HI residents",
    state_code=StateCode.US_HI,
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized` "
    "WHERE state_code = 'US_HI'",
    meets_criteria_default=True,
    reasons_fields=[],
)

CRITERIA_4_STATE_SPECIFIC = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name="US_KY_CRITERIA_4",
    description="A criteria for KY residents",
    state_code=StateCode.US_KY,
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized` "
    "WHERE state_code = 'US_KY'",
    meets_criteria_default=False,
    reasons_fields=[
        ReasonsField(
            name="violations",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="Number of violations",
        ),
        ReasonsField(
            name="latest_violation_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Latest violation date",
        ),
    ],
)

CRITERIA_5_STATE_SPECIFIC = StateSpecificTaskCriteriaBigQueryViewBuilder(
    criteria_name="US_KY_CRITERIA_5",
    description="Another criteria for KY residents",
    state_code=StateCode.US_KY,
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.drug_tests` "
    "WHERE state_code = 'US_KY'",
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="fees_owed",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="Amount of fees owed",
        ),
        ReasonsField(
            name="offense_types",
            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
            description="Offense types that person is serving",
        ),
    ],
)


class TestTaskCriteriaGroupBigQueryViewBuilder(unittest.TestCase):
    """Tests for the TaskCriteriaGroupBigQueryViewBuilder."""

    def test_and_criteria_group_state_agnostic(self) -> None:
        """Checks a standard AND group between two state-agnostic criteria"""
        criteria_group = AndTaskCriteriaGroup(
            criteria_name="CRITERIA_1_AND_CRITERIA_2",
            sub_criteria_list=[CRITERIA_1_STATE_AGNOSTIC, CRITERIA_2_STATE_AGNOSTIC],
            allowed_duplicate_reasons_keys=[],
        )
        # Check that a group with two state-agnostic criteria does not return a state_code
        self.assertIsNone(criteria_group.state_code)

        # Check that the state-agnostic criteria is stored in the general criteria dataset
        self.assertEqual(
            criteria_group.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_general",
                table_id="criteria_1_and_criteria_2_materialized",
            ),
        )

        # Check that meets_criteria_default is True if and only if all sub-criteria are True
        self.assertEqual(criteria_group.meets_criteria_default, False)

        # Check that description field is aligned
        expected_description = """
Combines the following criteria queries using AND logic:
 - MY_STATE_AGNOSTIC_CRITERIA: A state-agnostic criteria
 - ANOTHER_STATE_AGNOSTIC_CRITERIA: Another state-agnostic criteria"""
        self.assertEqual(criteria_group.description, expected_description)

        # Check that reasons fields are properly handled and combined
        expected_reasons_field_names = ["fees_owed"]
        actual_reasons_field_names = [
            field.name for field in criteria_group.reasons_fields
        ]
        self.assertEqual(actual_reasons_field_names, expected_reasons_field_names)

        # Check that query template is correct
        expected_query_template = f"""
WITH unioned_criteria AS (
    SELECT *, True AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.my_state_agnostic_criteria_materialized`
    UNION ALL
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.another_state_agnostic_criteria_materialized`
)
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT(MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed)) AS reason,
    MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed,
FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""
        self.assertEqual(expected_query_template, criteria_group.get_query_template())

    def test_criteria_group_state_specific_same_state(self) -> None:
        """Checks a standard AND group between two state-specific criteria from the same state"""
        criteria_group = AndTaskCriteriaGroup(
            criteria_name="US_KY_CRITERIA_4_AND_CRITERIA_5",
            sub_criteria_list=[CRITERIA_4_STATE_SPECIFIC, CRITERIA_5_STATE_SPECIFIC],
            allowed_duplicate_reasons_keys=[],
        )
        # Check that a group with two state-specific criteria from one state returns a state_code
        self.assertEqual(criteria_group.state_code, StateCode.US_KY)

        # Check that the state-specific criteria is stored in the right state criteria dataset
        self.assertEqual(
            criteria_group.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ky",
                table_id="criteria_4_and_criteria_5_materialized",
            ),
        )

        # Check that meets_criteria_default is True if and only if all sub-criteria are True
        self.assertEqual(criteria_group.meets_criteria_default, False)

        # Check that description field is aligned
        expected_description = """
Combines the following criteria queries using AND logic:
 - US_KY_CRITERIA_4: A criteria for KY residents
 - US_KY_CRITERIA_5: Another criteria for KY residents"""
        self.assertEqual(criteria_group.description, expected_description)

        # Check that reasons fields are properly handled and combined
        expected_reasons_field_names = [
            "fees_owed",
            "latest_violation_date",
            "offense_types",
            "violations",
        ]
        actual_reasons_field_names = [
            field.name for field in criteria_group.reasons_fields
        ]
        self.assertEqual(actual_reasons_field_names, expected_reasons_field_names)

        # Check that query template is correct
        expected_query_template = f"""
WITH unioned_criteria AS (
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_us_ky.criteria_4_materialized`
    UNION ALL
    SELECT *, True AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_us_ky.criteria_5_materialized`
)
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT(MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.latest_violation_date') AS DATE)) AS latest_violation_date, ANY_VALUE(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types, MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.violations') AS FLOAT64)) AS violations)) AS reason,
    MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.latest_violation_date') AS DATE)) AS latest_violation_date, ANY_VALUE(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types, MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.violations') AS FLOAT64)) AS violations,
FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""
        self.assertEqual(expected_query_template, criteria_group.get_query_template())

    def test_criteria_group_state_specific_and_state_agnostic_no_reasons(self) -> None:
        """Checks AND group between a state-specific and a state-agnostic criteria"""
        criteria_group = AndTaskCriteriaGroup(
            criteria_name="US_HI_CRITERIA_1_AND_CRITERIA_3",
            sub_criteria_list=[CRITERIA_1_STATE_AGNOSTIC, CRITERIA_3_STATE_SPECIFIC],
            allowed_duplicate_reasons_keys=[],
        )
        # Check that a group with one state-specific and one state-agnostic criteria returns a state_code
        self.assertEqual(criteria_group.state_code, StateCode.US_HI)

        # Check that the state-specific criteria is stored in the right state criteria dataset
        self.assertEqual(
            criteria_group.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_hi",
                table_id="criteria_1_and_criteria_3_materialized",
            ),
        )

        # Check that meets_criteria_default is True if and only if all sub-criteria are True
        self.assertEqual(criteria_group.meets_criteria_default, True)

        # Check that description field is aligned
        expected_description = """
Combines the following criteria queries using AND logic:
 - MY_STATE_AGNOSTIC_CRITERIA: A state-agnostic criteria
 - US_HI_CRITERIA_3: A criteria for HI residents"""
        self.assertEqual(criteria_group.description, expected_description)

        # Check that reasons fields are properly handled and combined
        actual_reasons_field_names = [
            field.name for field in criteria_group.reasons_fields
        ]
        self.assertEqual(actual_reasons_field_names, [])

        # Check that query template is correct
        expected_query_template = f"""
WITH unioned_criteria AS (
    SELECT *, True AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.my_state_agnostic_criteria_materialized`
    WHERE state_code = "US_HI"
    UNION ALL
    SELECT *, True AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_us_hi.criteria_3_materialized`
)
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT()) AS reason,

FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""
        self.assertEqual(expected_query_template, criteria_group.get_query_template())

    def test_criteria_group_state_specific_and_state_agnostic_with_duplicate_reasons(
        self,
    ) -> None:
        """Checks AND group between a state-specific and a state-agnostic criteria having overlapping reasons"""
        criteria_group = AndTaskCriteriaGroup(
            criteria_name="US_KY_CRITERIA_2_AND_CRITERIA_5",
            sub_criteria_list=[CRITERIA_2_STATE_AGNOSTIC, CRITERIA_5_STATE_SPECIFIC],
            allowed_duplicate_reasons_keys=["fees_owed"],
        )
        # Check that a group with one state-specific and one state-agnostic criteria returns a state_code
        self.assertEqual(criteria_group.state_code, StateCode.US_KY)

        # Check that the state-specific criteria is stored in the right state criteria dataset
        self.assertEqual(
            criteria_group.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ky",
                table_id="criteria_2_and_criteria_5_materialized",
            ),
        )

        # Check that meets_criteria_default is True if and only if all sub-criteria are True
        self.assertEqual(criteria_group.meets_criteria_default, False)

        # Check that description field is aligned
        expected_description = """
Combines the following criteria queries using AND logic:
 - ANOTHER_STATE_AGNOSTIC_CRITERIA: Another state-agnostic criteria
 - US_KY_CRITERIA_5: Another criteria for KY residents"""
        self.assertEqual(criteria_group.description, expected_description)

        # Check that reasons fields are properly handled and combined
        expected_reasons_field_names = ["fees_owed", "offense_types"]
        actual_reasons_field_names = [
            field.name for field in criteria_group.reasons_fields
        ]
        self.assertEqual(actual_reasons_field_names, expected_reasons_field_names)

        # Check that query template is correct
        expected_query_template = f"""
WITH unioned_criteria AS (
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.another_state_agnostic_criteria_materialized`
    WHERE state_code = "US_KY"
    UNION ALL
    SELECT *, True AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_us_ky.criteria_5_materialized`
)
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT(MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, ANY_VALUE(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types)) AS reason,
    MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, ANY_VALUE(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types,
FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""
        self.assertEqual(expected_query_template, criteria_group.get_query_template())

    def test_criteria_group_state_specific_and_state_agnostic_duplicate_reasons_missing_state(
        self,
    ) -> None:
        """Checks that OR group between a state-specific and a state-agnostic criteria having overlapping reasons
        with a missing exemption for duplicate keys throws an error"""

        with self.assertRaises(ValueError):
            criteria_query = OrTaskCriteriaGroup(
                criteria_name="CRITERIA_2_AND_CRITERIA_5",
                sub_criteria_list=[
                    CRITERIA_2_STATE_AGNOSTIC,
                    CRITERIA_5_STATE_SPECIFIC,
                ],
                allowed_duplicate_reasons_keys=[],
            )
            print(criteria_query.reasons_fields)

    def test_criteria_group_duplicate_array_reasons(
        self,
    ) -> None:
        """Checks that OR group with duplicate keys with type ARRAY throws an error when no aggregation function set"""

        with self.assertRaises(ValueError):
            criteria_query = OrTaskCriteriaGroup(
                criteria_name="CRITERIA_WITH_DUPLICATE_ARRAY_REASONS",
                sub_criteria_list=[
                    CRITERIA_5_STATE_SPECIFIC,
                    StateAgnosticTaskCriteriaBigQueryViewBuilder(
                        criteria_name="CRITERIA_WITH_ARRAY_2",
                        description="Another state-agnostic criteria with array reasons",
                        criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized`",
                        meets_criteria_default=False,
                        reasons_fields=[
                            ReasonsField(
                                name="offense_types",
                                type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                                description="Offense types that person is serving",
                            ),
                        ],
                    ),
                ],
                allowed_duplicate_reasons_keys=["offense_types"],
            )
            print(criteria_query.reasons_fields)

    def test_criteria_group_duplicate_array_reasons_with_aggregators(
        self,
    ) -> None:
        """Checks that groups with duplicate array keys are allowed if an override is set"""

        criteria_query = OrTaskCriteriaGroup(
            criteria_name="CRITERIA_WITH_DUPLICATE_ARRAY_REASONS",
            sub_criteria_list=[
                CRITERIA_5_STATE_SPECIFIC,
                StateAgnosticTaskCriteriaBigQueryViewBuilder(
                    criteria_name="CRITERIA_WITH_ARRAY_2",
                    description="Another state-agnostic criteria with array reasons",
                    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized`",
                    meets_criteria_default=False,
                    reasons_fields=[
                        ReasonsField(
                            name="offense_types",
                            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                            description="Offense types that person is serving",
                        ),
                    ],
                ),
            ],
            allowed_duplicate_reasons_keys=["offense_types"],
            reasons_aggregate_function_override={"offense_types": "ARRAY_CONCAT_AGG"},
        )
        expected_query_template = f"""
WITH unioned_criteria AS (
    SELECT *, True AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_us_ky.criteria_5_materialized`
    UNION ALL
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.criteria_with_array_2_materialized`
    WHERE state_code = "US_KY"
)
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_OR(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT(MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, ARRAY_CONCAT_AGG(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types)) AS reason,
    MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, ARRAY_CONCAT_AGG(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types,
FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""
        self.assertEqual(expected_query_template, criteria_query.get_query_template())

    def test_criteria_group_duplicate_array_reasons_with_aggregators_and_ordering_clause(
        self,
    ) -> None:
        """Checks that ordering clauses are inserted into aggregation queries when specified"""

        criteria_query = OrTaskCriteriaGroup(
            criteria_name="CRITERIA_WITH_DUPLICATE_ARRAY_REASONS",
            sub_criteria_list=[
                StateAgnosticTaskCriteriaBigQueryViewBuilder(
                    criteria_name="CRITERIA_WITH_ARRAY_1",
                    description="Some state-agnostic criteria with array reasons",
                    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized`",
                    meets_criteria_default=False,
                    reasons_fields=[
                        ReasonsField(
                            name="array_field_one",
                            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                            description="Some array",
                        ),
                        ReasonsField(
                            name="array_field_two",
                            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                            description="Some other array",
                        ),
                    ],
                ),
                StateAgnosticTaskCriteriaBigQueryViewBuilder(
                    criteria_name="CRITERIA_WITH_ARRAY_2",
                    description="Another state-agnostic criteria with array reasons",
                    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized`",
                    meets_criteria_default=False,
                    reasons_fields=[
                        ReasonsField(
                            name="array_field_one",
                            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                            description="Some array",
                        ),
                        ReasonsField(
                            name="array_field_two",
                            type=bigquery.enums.StandardSqlTypeNames.ARRAY,
                            description="Some other array",
                        ),
                    ],
                ),
            ],
            allowed_duplicate_reasons_keys=["array_field_one", "array_field_two"],
            reasons_aggregate_function_override={
                "array_field_one": "ARRAY_CONCAT_AGG",
                "array_field_two": "SOME_AGG_FUNC",
            },
            reasons_aggregate_function_use_ordering_clause={"array_field_one"},
        )
        expected_query_template = f"""
WITH unioned_criteria AS (
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.criteria_with_array_1_materialized`
    UNION ALL
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.criteria_with_array_2_materialized`
)
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_OR(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT(ARRAY_CONCAT_AGG(JSON_VALUE_ARRAY(reason_v2, '$.array_field_one') ORDER BY ARRAY_TO_STRING(JSON_VALUE_ARRAY(reason_v2, '$.array_field_one'), ',')) AS array_field_one, SOME_AGG_FUNC(JSON_VALUE_ARRAY(reason_v2, '$.array_field_two')) AS array_field_two)) AS reason,
    ARRAY_CONCAT_AGG(JSON_VALUE_ARRAY(reason_v2, '$.array_field_one') ORDER BY ARRAY_TO_STRING(JSON_VALUE_ARRAY(reason_v2, '$.array_field_one'), ',')) AS array_field_one, SOME_AGG_FUNC(JSON_VALUE_ARRAY(reason_v2, '$.array_field_two')) AS array_field_two,
FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""
        self.assertEqual(expected_query_template, criteria_query.get_query_template())

    def test_criteria_group_two_state_specific_criteria_error(self) -> None:
        """Checks that OR group between state-specific criteria from two different states throws an error"""
        with self.assertRaises(ValueError):
            criteria_query = OrTaskCriteriaGroup(
                criteria_name="CRITERIA_3_AND_CRITERIA_4",
                sub_criteria_list=[
                    CRITERIA_3_STATE_SPECIFIC,
                    CRITERIA_4_STATE_SPECIFIC,
                ],
                allowed_duplicate_reasons_keys=[],
            )
            print(criteria_query.state_code)

    def test_criteria_group_invalid_reason_aggregate_override(self) -> None:
        """Checks that an error is raised when an aggregate function override is set for a reason outside the group"""
        with self.assertRaises(ValueError):
            criteria_group = AndTaskCriteriaGroup(
                criteria_name="US_KY_CRITERIA_2_AND_CRITERIA_5",
                sub_criteria_list=[
                    CRITERIA_2_STATE_AGNOSTIC,
                    CRITERIA_5_STATE_SPECIFIC,
                ],
                allowed_duplicate_reasons_keys=["fees_owed"],
                reasons_aggregate_function_override={
                    "fees_owed": "MIN",
                    "other_reason": "ANY_VALUE",
                },
            )
            print(criteria_group.flatten_reasons_blob_clause())

    def test_inverted_criteria_state_specific_criteria_name(self) -> None:
        """Checks inverted state-specific criteria"""
        criteria = InvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=CRITERIA_5_STATE_SPECIFIC
        )
        self.assertEqual(criteria.criteria_name, "US_KY_NOT_CRITERIA_5")

        self.assertEqual(StateCode.US_KY, criteria.state_code)

        self.assertEqual(
            criteria.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ky",
                table_id="not_criteria_5_materialized",
            ),
        )

        self.assertEqual(criteria.meets_criteria_default, False)

        # Check that the reasons fields are the same between the inverted criteria view builder and the sub-criteria
        self.assertEqual(
            criteria.as_criteria_view_builder.reasons_fields,
            criteria.sub_criteria.reasons_fields,
        )

        # Check that query template is correct
        expected_query_template = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,
    SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64) AS fees_owed,
    JSON_VALUE_ARRAY(reason_v2, '$.offense_types') AS offense_types,
FROM
    `{project_id}.task_eligibility_criteria_us_ky.criteria_5_materialized`
"""
        self.assertEqual(expected_query_template, criteria.get_query_template())

    def test_inverted_criteria_state_agnostic_criteria_name(self) -> None:
        """Checks inverted state-agnostic criteria"""
        criteria = InvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=CRITERIA_2_STATE_AGNOSTIC
        )
        self.assertEqual(criteria.criteria_name, "NOT_ANOTHER_STATE_AGNOSTIC_CRITERIA")

        self.assertIsNone(criteria.state_code)

        self.assertEqual(
            criteria.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_general",
                table_id="not_another_state_agnostic_criteria_materialized",
            ),
        )

        self.assertEqual(criteria.meets_criteria_default, True)

        # Check that the reasons fields are the same between the inverted criteria view builder and the sub-criteria
        self.assertEqual(
            criteria.as_criteria_view_builder.reasons_fields,
            criteria.sub_criteria.reasons_fields,
        )

        # Check that query template is correct
        expected_query_template = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,
    SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64) AS fees_owed,
FROM
    `{project_id}.task_eligibility_criteria_general.another_state_agnostic_criteria_materialized`
"""
        self.assertEqual(expected_query_template, criteria.get_query_template())

    def test_inverted_criteria_empty_reasons(self) -> None:
        """Checks inverted criteria with an empty reasons list"""
        criteria = InvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=CRITERIA_1_STATE_AGNOSTIC
        )
        self.assertEqual(criteria.criteria_name, "NOT_MY_STATE_AGNOSTIC_CRITERIA")

        self.assertIsNone(criteria.state_code)

        self.assertEqual(
            criteria.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_general",
                table_id="not_my_state_agnostic_criteria_materialized",
            ),
        )

        self.assertEqual(criteria.meets_criteria_default, False)

        # Check that the reasons fields are the same between the inverted criteria view builder and the sub-criteria
        self.assertEqual(
            criteria.as_criteria_view_builder.reasons_fields,
            criteria.sub_criteria.reasons_fields,
        )

        # Check that query template is correct
        expected_query_template = """
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    NOT meets_criteria AS meets_criteria,
    reason,

FROM
    `{project_id}.task_eligibility_criteria_general.my_state_agnostic_criteria_materialized`
"""
        self.assertEqual(expected_query_template, criteria.get_query_template())

    def test_or_group_and_inverted_criteria_state_agnostic_criteria_name(self) -> None:
        """Checks OR group with a nested inverted criteria"""
        criteria_group = OrTaskCriteriaGroup(
            criteria_name="US_KY_CRITERIA_2_OR_NOT_5",
            sub_criteria_list=[
                CRITERIA_2_STATE_AGNOSTIC,
                InvertedTaskCriteriaBigQueryViewBuilder(CRITERIA_5_STATE_SPECIFIC),
            ],
            allowed_duplicate_reasons_keys=["fees_owed"],
            reasons_aggregate_function_override={"fees_owed": "MIN"},
        )
        # Check that a group with one state-specific and two state-agnostic criteria returns a state_code
        self.assertEqual(criteria_group.state_code, StateCode.US_KY)

        # Check that the state-specific criteria is stored in the right state criteria dataset
        self.assertEqual(
            criteria_group.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ky",
                table_id="criteria_2_or_not_5_materialized",
            ),
        )

        # Check that meets_criteria_default is True if and only if any sub-criteria is True
        self.assertEqual(criteria_group.meets_criteria_default, False)

        # Check that description field is aligned
        expected_description = """
Combines the following criteria queries using OR logic:
 - ANOTHER_STATE_AGNOSTIC_CRITERIA: Another state-agnostic criteria
 - US_KY_NOT_CRITERIA_5: A criteria that is met for every period of time when the US_KY_CRITERIA_5 criteria is not met, and vice versa."""
        self.assertEqual(criteria_group.description, expected_description)

        # Check that reasons fields are properly handled and combined
        expected_reasons_field_names = ["fees_owed", "offense_types"]
        actual_reasons_field_names = [
            field.name for field in criteria_group.reasons_fields
        ]
        self.assertEqual(actual_reasons_field_names, expected_reasons_field_names)

        # Check that query template is correct
        expected_query_template = f"""
WITH unioned_criteria AS (
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.another_state_agnostic_criteria_materialized`
    WHERE state_code = "US_KY"
    UNION ALL
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_us_ky.not_criteria_5_materialized`
)
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_OR(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT(MIN(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, ANY_VALUE(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types)) AS reason,
    MIN(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, ANY_VALUE(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types,
FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""
        self.assertEqual(expected_query_template, criteria_group.get_query_template())

    def test_multiple_nested_criteria(self) -> None:
        """Checks nested AND + OR + NOT criteria group"""
        criteria_group = AndTaskCriteriaGroup(
            criteria_name="US_KY_CRITERIA_1_AND_CRITERIA_2_OR_NOT_5",
            sub_criteria_list=[
                CRITERIA_1_STATE_AGNOSTIC,
                OrTaskCriteriaGroup(
                    criteria_name="US_KY_CRITERIA_2_OR_NOT_5",
                    sub_criteria_list=[
                        CRITERIA_2_STATE_AGNOSTIC,
                        InvertedTaskCriteriaBigQueryViewBuilder(
                            CRITERIA_5_STATE_SPECIFIC
                        ),
                    ],
                    allowed_duplicate_reasons_keys=["fees_owed"],
                ),
            ],
            allowed_duplicate_reasons_keys=[],
        )
        # Check that a group with one state-specific and two state-agnostic criteria returns a state_code
        self.assertEqual(criteria_group.state_code, StateCode.US_KY)

        self.assertEqual(
            criteria_group.table_for_query,
            BigQueryAddress(
                dataset_id="task_eligibility_criteria_us_ky",
                table_id="criteria_1_and_criteria_2_or_not_5_materialized",
            ),
        )

        # Check that description field is aligned
        expected_description = """
Combines the following criteria queries using AND logic:
 - MY_STATE_AGNOSTIC_CRITERIA: A state-agnostic criteria
 - US_KY_CRITERIA_2_OR_NOT_5: 
    Combines the following criteria queries using OR logic:
     - ANOTHER_STATE_AGNOSTIC_CRITERIA: Another state-agnostic criteria
     - US_KY_NOT_CRITERIA_5: A criteria that is met for every period of time when the US_KY_CRITERIA_5 criteria is not met, and vice versa."""
        self.assertEqual(criteria_group.description, expected_description)

        # Check that reasons fields are properly handled and combined
        expected_reasons_field_names = ["fees_owed", "offense_types"]
        actual_reasons_field_names = [
            field.name for field in criteria_group.reasons_fields
        ]
        self.assertEqual(actual_reasons_field_names, expected_reasons_field_names)

        # Check that query template is correct
        expected_query_template = f"""
WITH unioned_criteria AS (
    SELECT *, True AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_general.my_state_agnostic_criteria_materialized`
    WHERE state_code = "US_KY"
    UNION ALL
    SELECT *, False AS meets_criteria_default
    FROM `{{project_id}}.task_eligibility_criteria_us_ky.criteria_2_or_not_5_materialized`
)
,
{create_sub_sessions_with_attributes("unioned_criteria")}
SELECT
    state_code,
    person_id,
    start_date,
    end_date,
    LOGICAL_AND(
        COALESCE(meets_criteria, meets_criteria_default)
    ) AS meets_criteria,
    TO_JSON(STRUCT(MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, ANY_VALUE(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types)) AS reason,
    MAX(SAFE_CAST(JSON_VALUE(reason_v2, '$.fees_owed') AS FLOAT64)) AS fees_owed, ANY_VALUE(JSON_VALUE_ARRAY(reason_v2, '$.offense_types')) AS offense_types,
FROM
    sub_sessions_with_attributes
GROUP BY 1, 2, 3, 4
"""
        self.assertEqual(expected_query_template, criteria_group.get_query_template())
