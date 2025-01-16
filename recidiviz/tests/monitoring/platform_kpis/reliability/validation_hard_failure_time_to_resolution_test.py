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
"""Tests for validation hard failure time to resolution view."""
from datetime import date, datetime

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_spans import (
    VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER,
)
from recidiviz.monitoring.platform_kpis.reliability.validation_hard_failure_time_to_resolution import (
    VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_VIEW_BUILDER,
)
from recidiviz.tests.big_query.simple_big_query_view_builder_test_case import (
    SimpleBigQueryViewBuilderTestCase,
)
from recidiviz.utils.types import assert_type


class TestHardFailureTimeToResolutionViewBuilder(SimpleBigQueryViewBuilderTestCase):
    """Tests for validation hard failure time to resolution view."""

    @property
    def view_builder(self) -> SimpleBigQueryViewBuilder:
        return VALIDATION_HARD_FAILURE_TIME_TO_RESOLUTION_VIEW_BUILDER

    @property
    def parent_schemas(self) -> dict[BigQueryAddress, list[bigquery.SchemaField]]:

        return {
            assert_type(
                VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER.materialized_address,
                BigQueryAddress,
            ): [
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
            ]
        }

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
        validation_simple_resolution_hours = 48

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
        validation_starts_failing_resolution_hours = 48

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
        validation_resolution_types_resolution_hours = 48

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
        validation_was_not_run_resolution_hours = 96

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
        validation_ongoing_with_single_failure_resolution_hours = 0

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
        validation_ongoing_with_multiple_failures_resolution_hours = 0

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
        validation_removed_resolution_hours = 24

        validation_hard_failure_spans = [
            *validation_simple_input,
            *validation_starts_failing_input,
            *validation_resolution_types_input,
            *validation_ongoing_with_single_failure_input,
            *validation_ongoing_with_multiple_failures_input,
            *validation_was_not_run_input,
            *validation_removed_input,
        ]

        total_time_to_resolution = (
            validation_simple_resolution_hours
            + validation_starts_failing_resolution_hours
            + validation_resolution_types_resolution_hours
            + validation_was_not_run_resolution_hours
            + validation_ongoing_with_single_failure_resolution_hours
            + validation_ongoing_with_multiple_failures_resolution_hours
            + validation_removed_resolution_hours
        )

        self.run_simple_view_builder_query_test_from_data(
            input_data={
                assert_type(
                    VALIDATION_HARD_FAILURE_SPANS_VIEW_BUILDER.materialized_address,
                    BigQueryAddress,
                ): validation_hard_failure_spans,
            },
            expected_result=[
                {
                    "state_code": "US_XX",
                    "failure_month": date(2024, 1, 1),
                    "total_time_to_resolution": total_time_to_resolution,
                    # i think the emulator's approx quintiles might be wrong, when this
                    # same test data is run through big query it spits out
                    # 0, 0, 0, 48, 96
                    "percentile_0": 48,
                    "percentile_25": 48,
                    "percentile_50": 24,
                    "percentile_75": 96,
                    "percentile_100": 24,
                }
            ],
        )
