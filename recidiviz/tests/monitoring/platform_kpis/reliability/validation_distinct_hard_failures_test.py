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
"""Tests for validation distinct hard failures"""
import os
from datetime import date, datetime
from typing import Any

import pandas as pd
from google.cloud import bigquery
from pytz import timezone

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.reliability.validation_distinct_hard_failures import (
    VALIDATION_DISTINCT_HARD_FAILURES_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_spans import (
    VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER,
)
from recidiviz.source_tables import externally_managed
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)
from recidiviz.validation.validation_output_views import (
    VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS,
)

VALIDATION_RESULTS_DIRECTORY = (
    f"{os.path.dirname(externally_managed.__file__)}/validation_results/"
)
VALIDATION_COMPLETIONS_SOURCE_TABLE_PATH = os.path.join(
    VALIDATION_RESULTS_DIRECTORY, "validations_completion_tracker.yaml"
)


class TestDistinctHardFailuresViewBuilder(SimpleBigQueryViewBuilderTestCase):
    """Tests for validation distinct hard failures"""

    maxDiff = None

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return VALIDATION_DISTINCT_HARD_FAILURES_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        completions_table = SourceTableConfig.from_file(
            VALIDATION_COMPLETIONS_SOURCE_TABLE_PATH
        )

        return {
            VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER.table_for_query: [
                bigquery.SchemaField(name="state_code", field_type="STRING"),
                bigquery.SchemaField(name="validation_name", field_type="STRING"),
                bigquery.SchemaField(
                    name="validation_failure_status", field_type="STRING"
                ),
                bigquery.SchemaField(name="failure_datetime", field_type="DATETIME"),
                bigquery.SchemaField(
                    name="validation_resolution_status", field_type="STRING"
                ),
                bigquery.SchemaField(name="resolution_datetime", field_type="DATETIME"),
            ],
            completions_table.address: completions_table.schema_fields,
        }

    @staticmethod
    def _build_completions() -> list[dict[str, Any]]:
        return [
            {
                "run_id": f"run_{day}",
                "success_timestamp": datetime(2024, 1, day),
                "num_validations_run": 1,
                "validations_runtime_sec": 1000,
                "region_code": "US_XX",
            }
            for day in range(1, 8)
        ] + [
            {
                "run_id": "run_0",
                "success_timestamp": datetime(2021, 6, 1),
                "num_validations_run": 1,
                "validations_runtime_sec": 1000,
                "region_code": "US_XX",
            }
        ]

    def test_spans(self) -> None:

        # -- validation_simple --
        # is a simple validation (shocker) -- started succeeding, failed a little,
        # and then resolved
        validation_simple_input = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_simple",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 4),
                "validation_resolution_status": "SUCCESS",
                "resolution_datetime": datetime(2024, 1, 6),
            }
        ]
        validation_simple_distinct_hard_failures = 1

        # -- validation_starts_failing --
        # validation started failing but then has succeeded ever since
        # and then resolved
        validation_starts_failing_input = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_starts_failing",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 1),
                "validation_resolution_status": "SUCCESS",
                "resolution_datetime": datetime(2024, 1, 3),
            }
        ]
        validation_starts_failing_distinct_hard_failures = 1

        # -- validation_resolution_types --
        # validation has multiple types of resolutions
        validation_resolution_types_input = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_resolution_types",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 3),
                "validation_resolution_status": "SUCCESS",
                "resolution_datetime": datetime(2024, 1, 4),
            },
            {
                "state_code": "US_XX",
                "validation_name": "validation_resolution_types",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 5),
                "validation_resolution_status": "FAIL_SOFT",
                "resolution_datetime": datetime(2024, 1, 6),
            },
        ]
        validation_resolution_types_distinct_hard_failures = 2

        # -- validation_was_not_run --
        # validation was not run, we should collapse windows either side
        validation_was_not_run_input = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_was_not_run",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 3),
                "validation_resolution_status": "SUCCESS",
                "resolution_datetime": datetime(2024, 1, 7),
            },
        ]
        validation_was_not_run_distinct_hard_failures = 1

        # -- validation_ongoing_with_single_failure --
        # validation is ongoing, with only a single, recent failure
        validation_ongoing_with_single_failure_input = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_ongoing_with_single_failure",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 7),
                "validation_resolution_status": "ONGOING",
                "resolution_datetime": None,
            },
        ]
        validation_ongoing_with_single_failure_distinct_hard_failures = 1

        # -- validation_ongoing_with_multiple_failures --
        # validation is failing currently, and has been for a bit
        validation_ongoing_with_multiple_failures_input = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_ongoing_with_multiple_failures",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 3),
                "validation_resolution_status": "ONGOING",
                "resolution_datetime": None,
            },
        ]
        validation_ongoing_with_multiple_failures_distinct_hard_failures = 1

        # -- validation_removed --
        # validation was last failing, but is no longer actively run -- we dont want
        # to count it amongst our ongoing validation failures
        validation_removed_input = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_removed",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 3),
                "validation_resolution_status": "REMOVED",
                "resolution_datetime": datetime(2024, 1, 4),
            },
        ]
        validation_removed_distinct_hard_failures = 1

        validation_hard_failure_spans = [
            *validation_simple_input,
            *validation_starts_failing_input,
            *validation_resolution_types_input,
            *validation_ongoing_with_single_failure_input,
            *validation_ongoing_with_multiple_failures_input,
            *validation_was_not_run_input,
            *validation_removed_input,
        ]

        distinct_hard_failures_for_2024_01_01 = (
            validation_simple_distinct_hard_failures
            + validation_starts_failing_distinct_hard_failures
            + validation_resolution_types_distinct_hard_failures
            + validation_was_not_run_distinct_hard_failures
            + validation_ongoing_with_single_failure_distinct_hard_failures
            + validation_ongoing_with_multiple_failures_distinct_hard_failures
            + validation_removed_distinct_hard_failures
        )

        expected_results = [
            {
                "state_code": "US_XX",
                "failure_month": month_with_zero.date(),
                "distinct_failures": (
                    2 if month_with_zero.date() > date(2024, 1, 1) else 0
                ),
            }
            for month_with_zero in pd.date_range(
                start=date(2021, 6, 1),
                end=date.fromtimestamp(
                    datetime.now(tz=timezone("US/Eastern")).timestamp()
                ),
                freq="MS",
            )
            if month_with_zero.date() != date(2024, 1, 1)
        ] + [
            {
                "state_code": "US_XX",
                "failure_month": date(2024, 1, 1),
                "distinct_failures": distinct_hard_failures_for_2024_01_01,
            },
        ]

        self.run_simple_view_builder_query_test_from_data(
            input_data={
                VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER.table_for_query: validation_hard_failure_spans,
                VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS: self._build_completions(),
            },
            expected_result=expected_results,
            enforce_order=False,
        )
