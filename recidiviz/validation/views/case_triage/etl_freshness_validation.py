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
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.case_triage.utils import FRESHNESS_FRAGMENT
from recidiviz.validation.views.dataset_config import VIEWS_DATASET
from recidiviz.validation.views.utils.freshness_validation import (
    FreshnessValidation,
    FreshnessValidationAssertion,
)

ETL_TABLES = ["etl_clients", "etl_officers", "etl_opportunities"]

ETL_EXPORTED_ASSERTIONS = [
    FreshnessValidationAssertion.build_assertion(
        region_code="US_ID",
        assertion=f"{etl_table.upper()}_WAS_EXPORTED",
        description="Checks that we've exported data in the last 24 hours",
        dataset="case_triage",
        table=f"{etl_table}_materialized",
        freshness_clause=f"CAST(exported_at AS DATE) BETWEEN {FRESHNESS_FRAGMENT}",
    )
    for etl_table in ETL_TABLES
]

ETL_EXPORTED_CLOUDSQL_BIGQUERY_ASSERTIONS = [
    FreshnessValidationAssertion.build_assertion(
        region_code="US_ID",
        assertion=f"{etl_table.upper()}_WAS_EXPORTED_FROM_CLOUDSQL_TO_BIGQUERY",
        description="Checks that we've exported data in the last 24 hours after data was exported from CloudSQL back to BigQuery",
        dataset="case_triage_federated",
        table=etl_table,
        freshness_clause=f"CAST(exported_at AS DATE) BETWEEN {FRESHNESS_FRAGMENT}",
    )
    for etl_table in ETL_TABLES
]

ETL_FRESHNESS_VALIDATION_VIEW_BUILDER = FreshnessValidation(
    dataset=VIEWS_DATASET,
    view_id="case_triage_etl_freshness",
    description="Builds validation table to ensure Case Triage ETL tables are exported within SLA.",
    assertions=ETL_EXPORTED_ASSERTIONS + ETL_EXPORTED_CLOUDSQL_BIGQUERY_ASSERTIONS,
).to_big_query_view_builder()

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        ETL_FRESHNESS_VALIDATION_VIEW_BUILDER.build_and_print()
