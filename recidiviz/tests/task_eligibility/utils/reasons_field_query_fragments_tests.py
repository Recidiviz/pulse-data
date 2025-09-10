# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for functionality in reasons_field_query_fragments.py"""
from datetime import date

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.reasons_field_query_fragments import (
    extract_reasons_from_criteria,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.task_eligibility.task_eligibility_big_query_emulator_utils import (
    TEST_POPULATION_BUILDER,
    load_data_for_basic_task_eligibility_span_view,
)

CRITERIA_1: TaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name="MY_STATE_AGNOSTIC_CRITERIA",
    description="A state-agnostic criteria",
    criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.my_sessions_materialized`",
    meets_criteria_default=True,
    reasons_fields=[
        ReasonsField(
            name="fees_owed",
            type=bigquery.enums.StandardSqlTypeNames.FLOAT64,
            description="Amount of fees owed",
        ),
        ReasonsField(
            name="eligible_date",
            type=bigquery.enums.StandardSqlTypeNames.DATE,
            description="Date when eligibility begins",
        ),
        ReasonsField(
            name="test_reason",
            type=bigquery.enums.StandardSqlTypeNames.STRING,
            description="String reason",
        ),
    ],
)

CRITERIA_2: TaskCriteriaBigQueryViewBuilder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
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

BASIC_ELIGIBILITY_VIEW_BUILDER = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_XX,
    task_name="my_task_name",
    criteria_spans_view_builders=[CRITERIA_1, CRITERIA_2],
    candidate_population_view_builder=TEST_POPULATION_BUILDER,
)


class TestExtractReasonsFromCriteria(BigQueryEmulatorTestCase):
    """Tests for the `extract_reasons_from_criteria` method within utils/reasons_field_query_fragments.py"""

    def test_duplicate_reason_name_across_criteria(self) -> None:
        """Checks an error is raised when the reasons names are not unique across criteria"""
        criteria_reasons_fields = {
            CRITERIA_1: ["fees_owed", "eligible_date"],
            CRITERIA_2: ["fees_owed"],
        }
        with self.assertRaises(ValueError):
            _ = extract_reasons_from_criteria(
                criteria_reasons_fields,
                tes_view_builder=BASIC_ELIGIBILITY_VIEW_BUILDER,
                tes_table_name="test",
            )

    def test_criterion_not_in_tes_view_builder(self) -> None:
        """Checks an error is raised when the criterion is not in the TES view builder"""
        criteria_reasons_fields = {
            CRITERIA_1: ["eligible_date"],
            CRITERIA_2: ["fees_owed"],
        }
        basic_tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            criteria_spans_view_builders=[CRITERIA_1],
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
        )
        with self.assertRaises(ValueError):
            _ = extract_reasons_from_criteria(
                criteria_reasons_fields,
                tes_view_builder=basic_tes_view_builder,
                tes_table_name="test",
            )

    def test_reason_not_in_criterion(self) -> None:
        """Checks an error is raised when the reasons name is not in the criterion reasons list"""
        criteria_reasons_fields = {CRITERIA_1: ["fees_owed", "other_reason"]}
        with self.assertRaises(ValueError):
            _ = extract_reasons_from_criteria(
                criteria_reasons_fields,
                tes_view_builder=BASIC_ELIGIBILITY_VIEW_BUILDER,
                tes_table_name="test",
            )

    def test_extract_reasons_from_criteria(self) -> None:
        """Test the extract query fragment returns the expected data"""

        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=BASIC_ELIGIBILITY_VIEW_BUILDER.table_for_query,
            basic_eligibility_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": CRITERIA_1.criteria_name,
                            "reason": {
                                "fees_owed": 100,
                                "eligible_date": "2024-02-06",
                                "test_reason": "reason_text",
                            },
                        },
                        {
                            "criteria_name": CRITERIA_2.criteria_name,
                            "reason": {
                                "fees_owed": 0,
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": CRITERIA_1.criteria_name,
                            "reason": {
                                "fees_owed": 100,
                                "eligible_date": "2024-02-06",
                                "test_reason": "reason_text",
                            },
                        },
                        {
                            "criteria_name": CRITERIA_2.criteria_name,
                            "reason": {
                                "fees_owed": 0,
                            },
                        },
                    ],
                    "ineligible_criteria": [CRITERIA_1.criteria_name],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": CRITERIA_1.criteria_name,
                            "reason": {
                                "fees_owed": 100,
                                "eligible_date": "2024-01-01",
                                "test_reason": "reason_text_new",
                            },
                        },
                        {
                            "criteria_name": CRITERIA_2.criteria_name,
                            "reason": {
                                "fees_owed": 0,
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": CRITERIA_1.criteria_name,
                            "reason": {
                                "fees_owed": 100,
                                "eligible_date": "2024-01-01",
                                "test_reason": "reason_text_new",
                            },
                        },
                        {
                            "criteria_name": CRITERIA_2.criteria_name,
                            "reason": {
                                "fees_owed": 0,
                            },
                        },
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )

        criteria_reasons_fields = {
            CRITERIA_1: ["eligible_date", "test_reason"],
            CRITERIA_2: ["fees_owed"],
        }
        extract_query_fragment = extract_reasons_from_criteria(
            criteria_reason_fields=criteria_reasons_fields,
            tes_view_builder=BASIC_ELIGIBILITY_VIEW_BUILDER,
            tes_table_name=BASIC_ELIGIBILITY_VIEW_BUILDER.table_for_query.to_str(),
            index_columns=["state_code", "person_id", "start_date"],
            reason_column_prefix="test_",
        )

        self.run_query_test(
            query_str=extract_query_fragment,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "test_eligible_date": date(2024, 2, 6),
                    "test_test_reason": "reason_text",
                    "test_fees_owed": 0,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "test_eligible_date": date(2024, 1, 1),
                    "test_test_reason": "reason_text_new",
                    "test_fees_owed": 0,
                },
            ],
        )

        expected_query_string = """
    WITH criteria_reasons AS (
        SELECT
            state_code, person_id, start_date,
            JSON_VALUE(criteria_reason, '$.criteria_name') AS criteria_name,
            criteria_reason
        FROM task_eligibility_spans_us_xx.my_task_name__basic_materialized,
        UNNEST
            (JSON_QUERY_ARRAY(reasons_v2)) AS criteria_reason
        WHERE
            JSON_VALUE(criteria_reason, '$.criteria_name') IN ("MY_STATE_AGNOSTIC_CRITERIA", "ANOTHER_STATE_AGNOSTIC_CRITERIA")
    )
    , test_my_state_agnostic_criteria AS (
        SELECT
            state_code, person_id, start_date,
            SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.eligible_date') AS DATE) AS test_eligible_date,
            SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.test_reason') AS STRING) AS test_test_reason,
        FROM criteria_reasons
        WHERE criteria_name = "MY_STATE_AGNOSTIC_CRITERIA"
    )
    , test_another_state_agnostic_criteria AS (
        SELECT
            state_code, person_id, start_date,
            SAFE_CAST(JSON_VALUE(criteria_reason, '$.reason.fees_owed') AS FLOAT64) AS test_fees_owed,
        FROM criteria_reasons
        WHERE criteria_name = "ANOTHER_STATE_AGNOSTIC_CRITERIA"
    )
    SELECT
        state_code, person_id, start_date,
        test_eligible_date, test_test_reason, test_fees_owed
    FROM test_my_state_agnostic_criteria
    FULL OUTER JOIN test_another_state_agnostic_criteria USING (state_code, person_id, start_date)"""

        self.assertEqual(
            expected_query_string,
            extract_query_fragment,
        )
