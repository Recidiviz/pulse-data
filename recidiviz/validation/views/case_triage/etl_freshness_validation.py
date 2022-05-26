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
"""A view that can be used to validate that the Case Triage ETL has been exported within SLA."""
from recidiviz.case_triage.views.dataset_config import (
    CASE_TRIAGE_CLOUDSQL_CONNECTION,
    CASE_TRIAGE_CLOUDSQL_LOCATION,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET
from recidiviz.validation.views.utils.freshness_validation import (
    FreshnessValidation,
    FreshnessValidationAssertion,
)

# TODO(#8758): Import the actual ETL_TABLES constant once dashboard_user_restrictions has
# an `exported_at` column.
ETL_TABLES = ["etl_clients", "etl_officers", "etl_opportunities"]

ETL_IMPORTED_ASSERTIONS = [
    FreshnessValidationAssertion(
        region_code="US_ID",
        assertion_name=f"{etl_table.upper()}_WAS_IMPORTED_TO_CLOUDSQL",
        description="Checks that we've imported recent data in the last 24 hours",
        source_data_query=FreshnessValidationAssertion.build_cloudsql_connection_source_data_query(
            location=CASE_TRIAGE_CLOUDSQL_LOCATION,
            connection=CASE_TRIAGE_CLOUDSQL_CONNECTION,
            table=etl_table,
            date_column_clause="CAST(exported_at AS DATE)",
        ),
    )
    for etl_table in ETL_TABLES
]

ETL_DATA_ASSERTIONS = [
    FreshnessValidationAssertion(
        region_code="US_ID",
        assertion_name="CASE_TRIAGE_CLOUDSQL_CONTAINS_FRESH_CONTACTS_AFTER_EXPORT",
        description="Checks that imported data contained recent contacts",
        source_data_query=FreshnessValidationAssertion.build_cloudsql_connection_source_data_query(
            location=CASE_TRIAGE_CLOUDSQL_LOCATION,
            connection=CASE_TRIAGE_CLOUDSQL_CONNECTION,
            table="etl_clients",
            date_column_clause="most_recent_face_to_face_date",
            filter_clause="state_code = 'US_ID'",
        ),
    ),
    FreshnessValidationAssertion(
        region_code="US_ID",
        assertion_name="CASE_TRIAGE_CLOUDSQL_CONTAINS_FRESH_ASSESSMENTS_AFTER_EXPORT",
        description="Checks that imported data contained recent assessments",
        source_data_query=FreshnessValidationAssertion.build_cloudsql_connection_source_data_query(
            location=CASE_TRIAGE_CLOUDSQL_LOCATION,
            connection=CASE_TRIAGE_CLOUDSQL_CONNECTION,
            table="etl_clients",
            date_column_clause="most_recent_assessment_date",
            filter_clause="state_code = 'US_ID'",
        ),
    ),
]

ETL_FRESHNESS_VALIDATION_VIEW_BUILDER = FreshnessValidation(
    dataset=VIEWS_DATASET,
    view_id="case_triage_etl_freshness",
    description="Builds validation table to ensure Case Triage ETL tables are exported within SLA.",
    assertions=ETL_IMPORTED_ASSERTIONS + ETL_DATA_ASSERTIONS,
).to_big_query_view_builder()

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ETL_FRESHNESS_VALIDATION_VIEW_BUILDER.build_and_print()
