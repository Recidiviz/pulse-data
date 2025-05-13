# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for inverted_task_criteria_big_query_view_builder.py"""
from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    InvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
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


class TestInvertedTaskCriteriaBigQueryViewBuilder(BigQueryEmulatorTestCase):
    """Tests for the InvertedTaskCriteriaBigQueryViewBuilder."""

    def test_inverted_criteria_state_specific_criteria_name(self) -> None:
        """Checks inverted state-specific criteria"""
        criteria = InvertedTaskCriteriaBigQueryViewBuilder(
            sub_criteria=CRITERIA_3_STATE_SPECIFIC
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
