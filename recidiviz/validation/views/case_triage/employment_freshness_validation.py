# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""A view that can be used to validate that BigQuery has fresh employment data
"""
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views.case_triage.utils import DT_CLAUSE, MAX_DAYS_STALE
from recidiviz.validation.views.dataset_config import VIEWS_DATASET
from recidiviz.validation.views.utils.freshness_validation import (
    FreshnessValidation,
    FreshnessValidationAssertion,
)

UPDDATE_DT_CLAUSE = StrictStringFormatter().format(DT_CLAUSE, datetime_field="upddate")

EMPLOYMENT_FRESHNESS_VALIDATION_VIEW_BUILDER = FreshnessValidation(
    dataset=VIEWS_DATASET,
    view_id="case_triage_employment_freshness",
    description="Builds validation table to ensure ingested employment data meets Case Triage SLAs",
    assertions=[
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="RAW_DATA_WAS_IMPORTED",
            description="Checks that we've imported raw data in the last 24 hours, but not the received data is timely",
            dataset=raw_tables_dataset_for_region("us_id"),
            table="cis_employment",
            date_column_clause="CAST(update_datetime AS DATE)",
            allowed_days_stale=MAX_DAYS_STALE,
        ),
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="RAW_DATA_WAS_EDITED_WITHIN_EXPECTED_PERIOD",
            description="Checks that the imported data contains edits from within the freshness threshold",
            dataset=raw_tables_dataset_for_region("us_id"),
            table="cis_employment",
            date_column_clause=UPDDATE_DT_CLAUSE,
            allowed_days_stale=MAX_DAYS_STALE,
        ),
    ],
).to_big_query_view_builder()

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        EMPLOYMENT_FRESHNESS_VALIDATION_VIEW_BUILDER.build_and_print()
