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
"""Creates a view that surfaces sessions during which clients are ineligible for
Compliant Reporting due to judge/court order."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    STATE_BASE_DATASET,
    US_TN_RAW_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_VIEW_NAME = (
    "us_tn_compliant_reporting_judicial_order_ineligible"
)

US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_VIEW_DESCRIPTION = "Creates a view that surfaces sessions during which clients are ineligible for Compliant Reporting due judge/court order"

US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_QUERY_TEMPLATE = """
    SELECT 
        person_id,
        "US_TN" AS state_code,
        CAST(CAST(ContactNoteDateTime AS DATETIME) AS DATE) AS start_date,
        CAST(NULL AS DATE) AS end_date,
        FALSE AS judicial_order_eligible,
    FROM `{project_id}.{raw_dataset}.ContactNoteType_latest` a
    INNER JOIN `{project_id}.{base_dataset}.state_person_external_id` pei
            ON a.OffenderID = pei.external_id
            AND pei.state_code = 'US_TN'
    WHERE ContactNoteType IN ('DEIJ','DECR')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY start_date) = 1
"""

US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    base_dataset=STATE_BASE_DATASET,
    view_id=US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_VIEW_NAME,
    description=US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_VIEW_DESCRIPTION,
    view_query_template=US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_QUERY_TEMPLATE,
    raw_dataset=US_TN_RAW_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_COMPLIANT_REPORTING_JUDICIAL_ORDER_INELIGIBLE_VIEW_BUILDER.build_and_print()
