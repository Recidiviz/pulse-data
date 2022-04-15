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
"""A view that can be used to validate that BigQuery has fresh assessment data
"""
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    STATE_BASE_DATASET,
)
from recidiviz.case_triage.views.dataset_config import (
    CASE_TRIAGE_DATASET,
    CASE_TRIAGE_FEDERATED_DATASET,
)
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

UPDT_DT_CLAUSE = StrictStringFormatter().format(DT_CLAUSE, datetime_field="updt_dt")
TST_DT_CLAUSE = StrictStringFormatter().format(DT_CLAUSE, datetime_field="tst_dt")

ASSESSMENT_FRESHNESS_VALIDATION_VIEW_BUILDER = FreshnessValidation(
    dataset=VIEWS_DATASET,
    view_id="case_triage_risk_assessment_freshness",
    description="Builds validation table to ensure ingested assessment data meets Case Triage SLAs",
    assertions=[
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="RAW_DATA_WAS_IMPORTED",
            description="Checks that we've imported raw data in the last 24 hours, but not the received data is timely",
            dataset=raw_tables_dataset_for_region("us_id"),
            table="ofndr_tst",
            date_column_clause="CAST(update_datetime AS DATE)",
            allowed_days_stale=MAX_DAYS_STALE,
        ),
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="RAW_DATA_WAS_EDITED_WITHIN_EXPECTED_PERIOD",
            description="Checks that the imported data contains edits from within the freshness threshold",
            dataset=raw_tables_dataset_for_region("us_id"),
            table="ofndr_tst",
            date_column_clause=UPDT_DT_CLAUSE,
            allowed_days_stale=MAX_DAYS_STALE,
        ),
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="RAW_DATA_CONTAINS_RELEVANT_DATA_FROM_EXPECTED_PERIOD",
            description="Checks that the imported data contains assessments from within the freshness threshold",
            dataset=raw_tables_dataset_for_region("us_id"),
            table="ofndr_tst",
            date_column_clause=TST_DT_CLAUSE,
            allowed_days_stale=MAX_DAYS_STALE,
        ),
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="STATE_TABLES_CONTAIN_FRESH_DATA",
            description="Checks that the state tables were successfully updated",
            dataset=STATE_BASE_DATASET,
            table="state_assessment",
            date_column_clause="assessment_date",
            allowed_days_stale=MAX_DAYS_STALE,
            filter_clause="state_code = 'US_ID'",
        ),
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="NORMALIZED_STATE_TABLES_CONTAIN_FRESH_DATA",
            description="Checks that the normalized_state tables were successfully updated",
            dataset=NORMALIZED_STATE_DATASET,
            table="state_assessment",
            date_column_clause="assessment_date",
            allowed_days_stale=MAX_DAYS_STALE,
            filter_clause="state_code = 'US_ID'",
        ),
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="CASE_TRIAGE_ETL_CONTAINS_FRESH_DATA",
            description="Checks that the Case Triage ETL was successfully updated",
            dataset=CASE_TRIAGE_DATASET,
            table="etl_clients_materialized",
            date_column_clause="most_recent_assessment_date",
            allowed_days_stale=MAX_DAYS_STALE,
            filter_clause="state_code = 'US_ID'",
        ),
        FreshnessValidationAssertion(
            region_code="US_ID",
            assertion_name="CASE_TRIAGE_ETL_CONTAINS_FRESH_DATA_AFTER_EXPORT",
            description="Checks that the Case Triage ETL was successfully updated after the Cloud SQL export to BigQuery",
            dataset=CASE_TRIAGE_FEDERATED_DATASET,
            table="etl_clients",
            date_column_clause="most_recent_assessment_date",
            allowed_days_stale=MAX_DAYS_STALE,
            filter_clause="state_code = 'US_ID'",
        ),
    ],
).to_big_query_view_builder()


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ASSESSMENT_FRESHNESS_VALIDATION_VIEW_BUILDER.build_and_print()
