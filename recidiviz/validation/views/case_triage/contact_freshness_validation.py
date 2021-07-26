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
"""A view that can be used to validate that BigQuery has fresh contacts data
"""

from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.case_triage.utils import FRESHNESS_FRAGMENT
from recidiviz.validation.views.dataset_config import VIEWS_DATASET
from recidiviz.validation.views.utils.freshness_validation import (
    FreshnessValidation,
    FreshnessValidationAssertion,
)

CONTACT_FRESHNESS_VALIDATION_VIEW_BUILDER = FreshnessValidation(
    dataset=VIEWS_DATASET,
    view_id="case_triage_f2f_contact_freshness",
    description="Builds validation table to ensure ingested f2f contact data meets Case Triage SLAs",
    assertions=[
        FreshnessValidationAssertion.build_assertion(
            region_code="US_ID",
            assertion="RAW_DATA_WAS_IMPORTED",
            description="Checks that we've imported raw data in the last 24 hours, but not the received data is timely",
            dataset="us_id_raw_data",
            table="sprvsn_cntc",
            freshness_clause=f"CAST(update_datetime AS DATE) BETWEEN {FRESHNESS_FRAGMENT}",
        ),
        FreshnessValidationAssertion.build_assertion(
            region_code="US_ID",
            assertion="RAW_DATA_WAS_EDITED_WITHIN_EXPECTED_PERIOD",
            description="Checks that the imported data contains edits from within the freshness threshold",
            dataset="us_id_raw_data",
            table="sprvsn_cntc",
            freshness_clause=f"SAFE_CAST(SPLIT(updt_dt, ' ')[OFFSET(0)] AS DATE) BETWEEN {FRESHNESS_FRAGMENT}",
        ),
        FreshnessValidationAssertion.build_assertion(
            region_code="US_ID",
            assertion="RAW_DATA_CONTAINS_RELEVANT_DATA_FROM_EXPECTED_PERIOD",
            description="Checks that the imported data contains contacts from within the freshness threshold",
            dataset="us_id_raw_data",
            table="sprvsn_cntc",
            freshness_clause=f"SAFE_CAST(SPLIT(cntc_dt, ' ')[OFFSET(0)] AS DATE) BETWEEN {FRESHNESS_FRAGMENT}",
        ),
        FreshnessValidationAssertion.build_assertion(
            region_code="US_ID",
            assertion="STATE_TABLES_CONTAIN_FRESH_DATA",
            description="Checks that the state tables were successfully updated",
            dataset="state",
            table="state_supervision_contact",
            freshness_clause=f"state_code = 'US_ID' AND contact_date BETWEEN {FRESHNESS_FRAGMENT}",
        ),
        FreshnessValidationAssertion.build_assertion(
            region_code="US_ID",
            assertion="CASE_TRIAGE_ETL_CONTAINS_FRESH_DATA",
            description="Checks that the Case Triage ETL was successfully updated",
            dataset="case_triage",
            table="etl_clients_materialized",
            freshness_clause=f"state_code = 'US_ID' AND most_recent_face_to_face_date BETWEEN {FRESHNESS_FRAGMENT}",
        ),
    ],
).to_big_query_view_builder()


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CONTACT_FRESHNESS_VALIDATION_VIEW_BUILDER.build_and_print()
