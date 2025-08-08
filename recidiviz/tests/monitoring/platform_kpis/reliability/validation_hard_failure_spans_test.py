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
"""Tests for validation hard failure spans."""
import os
from collections import defaultdict
from datetime import datetime
from typing import Any

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_spans import (
    VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER,
)
from recidiviz.source_tables import externally_managed
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)
from recidiviz.validation.validation_models import ValidationResultStatus
from recidiviz.validation.validation_output_views import (
    VALIDATION_RESULTS_BIGQUERY_ADDRESS,
    VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS,
)

VALIDATION_RESULTS_DIRECTORY = (
    f"{os.path.dirname(externally_managed.__file__)}/validation_results/"
)
VALIDATION_RESULTS_SOURCE_TABLE_PATH = os.path.join(
    VALIDATION_RESULTS_DIRECTORY, "validation_results.yaml"
)
VALIDATION_COMPLETIONS_SOURCE_TABLE_PATH = os.path.join(
    VALIDATION_RESULTS_DIRECTORY, "validations_completion_tracker.yaml"
)


class TestValidationHardFailureSpans(SimpleBigQueryViewBuilderTestCase):
    """Tests for hard failure spans"""

    maxDiff = None

    required_but_unused_fields = {
        "system_version": "v1",
        "run_date": "2024-01-01",
        "check_type": "check_one",
    }

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:
        results_table = SourceTableConfig.from_file(
            VALIDATION_RESULTS_SOURCE_TABLE_PATH
        )
        completions_table = SourceTableConfig.from_file(
            VALIDATION_COMPLETIONS_SOURCE_TABLE_PATH
        )

        return {
            results_table.address: results_table.schema_fields,
            completions_table.address: completions_table.schema_fields,
        }

    @staticmethod
    def _completion_from_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
        runs_to_count: dict[str, int] = defaultdict(int)
        for result in results:
            runs_to_count[result["run_id"]] += 1

        return [
            {
                "run_id": run,
                "success_timestamp": datetime(2024, 1, int(run.split("_")[-1])),
                "num_validations_run": num_runs,
                "validations_runtime_sec": 1000,
                "region_code": "US_XX",
            }
            for run, num_runs in runs_to_count.items()
        ]

    def build_test_validation_input(
        self, name: str, region: str, statuses: list[ValidationResultStatus | None]
    ) -> list[dict[str, Any]]:
        return [
            {
                "run_id": f"run_{i}",
                "region_code": region,
                "validation_name": name,
                "run_datetime": datetime(2024, 1, i),
                "validation_result_status": status.value if status else None,
                "did_run": status is not None,
                **self.required_but_unused_fields,
            }
            for i, status in enumerate(statuses, start=1)
        ]

    def test_spans(self) -> None:

        # -- validation_simple --
        # is a simple validation (shocker) -- started succeeding, failed a little,
        # and then resolved
        validation_simple_input = self.build_test_validation_input(
            name="validation_simple",
            region="US_XX",
            statuses=[
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.FAIL_HARD,
                ValidationResultStatus.FAIL_HARD,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
            ],
        )
        validation_simple_output = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_simple",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 4),
                "validation_resolution_status": "SUCCESS",
                "resolution_datetime": datetime(2024, 1, 6),
            }
        ]

        # -- validation_starts_failing --
        # validation started failing but then has succeeded ever since
        # and then resolved
        validation_starts_failing_input = self.build_test_validation_input(
            name="validation_starts_failing",
            region="US_XX",
            statuses=[
                ValidationResultStatus.FAIL_HARD,
                ValidationResultStatus.FAIL_HARD,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
            ],
        )
        validation_starts_failing_output = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_starts_failing",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 1),
                "validation_resolution_status": "SUCCESS",
                "resolution_datetime": datetime(2024, 1, 3),
            }
        ]

        # -- validation_resolution_types --
        # validation has multiple types of resolutions
        validation_resolution_types_input = self.build_test_validation_input(
            name="validation_resolution_types",
            region="US_XX",
            statuses=[
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.FAIL_SOFT,
                ValidationResultStatus.FAIL_HARD,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.FAIL_HARD,
                ValidationResultStatus.FAIL_SOFT,
                ValidationResultStatus.FAIL_SOFT,
            ],
        )
        validation_resolution_types_output = [
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

        # -- validation_was_not_run --
        # validation was not run, we should collapse windows either side
        validation_was_not_run_input = self.build_test_validation_input(
            name="validation_was_not_run",
            region="US_XX",
            statuses=[
                ValidationResultStatus.SUCCESS,
                None,
                ValidationResultStatus.FAIL_HARD,
                None,
                ValidationResultStatus.FAIL_HARD,
                None,
                ValidationResultStatus.SUCCESS,
            ],
        )
        validation_was_not_run_output = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_was_not_run",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 3),
                "validation_resolution_status": "SUCCESS",
                "resolution_datetime": datetime(2024, 1, 7),
            },
        ]

        # -- validation_ongoing_with_single_failure --
        # validation is ongoing, with only a single, recent failure
        validation_ongoing_with_single_failure_input = self.build_test_validation_input(
            name="validation_ongoing_with_single_failure",
            region="US_XX",
            statuses=[
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.FAIL_HARD,
            ],
        )
        validation_ongoing_with_single_failure_output = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_ongoing_with_single_failure",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 7),
                "validation_resolution_status": "ONGOING",
                "resolution_datetime": None,
            },
        ]

        # -- validation_ongoing_with_multiple_failures --
        # validation is failing currently, and has been for a bit
        validation_ongoing_with_multiple_failures_input = (
            self.build_test_validation_input(
                name="validation_ongoing_with_multiple_failures",
                region="US_XX",
                statuses=[
                    ValidationResultStatus.SUCCESS,
                    ValidationResultStatus.SUCCESS,
                    ValidationResultStatus.FAIL_HARD,
                    ValidationResultStatus.FAIL_HARD,
                    ValidationResultStatus.FAIL_HARD,
                    ValidationResultStatus.FAIL_HARD,
                    ValidationResultStatus.FAIL_HARD,
                ],
            )
        )
        validation_ongoing_with_multiple_failures_output = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_ongoing_with_multiple_failures",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 3),
                "validation_resolution_status": "ONGOING",
                "resolution_datetime": None,
            },
        ]

        # -- validation_removed --
        # validation was last failing, but is no longer actively run -- we dont want
        # to count it amongst our ongoing validation failures
        validation_removed_input = self.build_test_validation_input(
            name="validation_removed",
            region="US_XX",
            statuses=[
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.SUCCESS,
                ValidationResultStatus.FAIL_HARD,
                ValidationResultStatus.FAIL_HARD,
            ],
        )
        validation_removed_output = [
            {
                "state_code": "US_XX",
                "validation_name": "validation_removed",
                "validation_failure_status": "FAIL_HARD",
                "failure_datetime": datetime(2024, 1, 3),
                "validation_resolution_status": "REMOVED",
                "resolution_datetime": datetime(2024, 1, 4),
            },
        ]

        validation_results = [
            *validation_simple_input,
            *validation_starts_failing_input,
            *validation_resolution_types_input,
            *validation_ongoing_with_single_failure_input,
            *validation_ongoing_with_multiple_failures_input,
            *validation_was_not_run_input,
            *validation_removed_input,
        ]
        completion_results = self._completion_from_results(validation_results)

        self.run_simple_view_builder_query_test_from_data(
            input_data={
                VALIDATION_RESULTS_BIGQUERY_ADDRESS: validation_results,
                VALIDATIONS_COMPLETION_TRACKER_BIGQUERY_ADDRESS: completion_results,
            },
            expected_result=[
                *validation_simple_output,
                *validation_starts_failing_output,
                *validation_resolution_types_output,
                *validation_ongoing_with_single_failure_output,
                *validation_ongoing_with_multiple_failures_output,
                *validation_was_not_run_output,
                *validation_removed_output,
            ],
            enforce_order=False,
        )
