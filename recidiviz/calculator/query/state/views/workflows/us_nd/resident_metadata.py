# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""North Dakota resident metadata"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state.dataset_config import (
    SESSIONS_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.task_eligibility.utils.us_nd_query_fragments import (
    parole_review_dates_query,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ND_RESIDENT_METADATA_VIEW_NAME = "us_nd_resident_metadata"

US_ND_RESIDENT_METADATA_VIEW_DESCRIPTION = """
North Dakota resident metadata
"""


US_ND_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE = f"""
WITH parole_review_dates AS ({parole_review_dates_query()}), 
latest_parole_review_dates AS (
    SELECT *
    FROM parole_review_dates
    QUALIFY ROW_NUMBER() OVER(PARTITION BY state_code, person_id ORDER BY parole_review_date DESC) = 1
),
parole_date AS (
    SELECT 
        peid.state_code,
        peid.person_id,
        MAX(SAFE_CAST(LEFT(eo.PAROLE_DATE, 10) AS DATE)) AS parole_date,
    FROM `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.elite_offendersentenceaggs_latest` eo
    INNER JOIN `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` peid
    ON peid.external_id = eo.OFFENDER_BOOK_ID
        AND peid.state_code = 'US_ND'
        AND peid.id_type = 'US_ND_ELITE_BOOKING'
    WHERE eo.PAROLE_DATE IS NOT NULL
        AND SAFE_CAST(LEFT(eo.FINAL_SENT_EXP_DATE, 10) AS DATE) > CURRENT_DATE
    GROUP BY 1,2
)

SELECT 
    prd.*,
    pd.parole_date,
FROM `{{project_id}}.{{sessions_dataset}}.compartment_sessions_materialized` cs
INNER JOIN latest_parole_review_dates prd
    USING(person_id, state_code)
LEFT JOIN parole_date pd
    USING(person_id, state_code)
WHERE cs.compartment_level_1 = 'INCARCERATION'
    AND cs.state_code = 'US_ND'
    AND {today_between_start_date_and_nullable_end_date_exclusive_clause(
            start_date_column="cs.start_date",
            end_date_column="cs.end_date"
        )}
"""

US_ND_RESIDENT_METADATA_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=US_ND_RESIDENT_METADATA_VIEW_NAME,
    view_query_template=US_ND_RESIDENT_METADATA_VIEW_QUERY_TEMPLATE,
    description=US_ND_RESIDENT_METADATA_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_dataset=raw_tables_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_ND, instance=DirectIngestInstance.PRIMARY
    ),
    sessions_dataset=SESSIONS_DATASET,
    should_materialize=True,
)


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ND_RESIDENT_METADATA_VIEW_BUILDER.build_and_print()
