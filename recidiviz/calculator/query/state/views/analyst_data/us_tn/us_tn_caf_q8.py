# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Computes Q8 of TN's CAF form (detainer information) at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    create_sub_sessions_with_attributes,
)
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_CAF_Q8_VIEW_NAME = "us_tn_caf_q8"

US_TN_CAF_Q8_VIEW_DESCRIPTION = """Computes Q8 of TN's CAF form (detainer information) at any point in time.
    See details of CAF Form here https://drive.google.com/file/d/18ez172yx3Tpx-rFaNseJv3dgMh7cGezg/view?usp=sharing
    """

US_TN_CAF_Q8_QUERY_TEMPLATE = f"""
    WITH detainers AS (
        -- As discussed with TTs in TN, a detainer is "relevant" until it has been lifted, so we use that as
        -- our end date
        SELECT
            state_code,
            person_id,
            DATE(DetainerReceivedDate) AS start_date,
            DATE(DetainerLiftDate) AS end_date,
            CASE WHEN DetainerFelonyFlag = 'X' THEN 5
                 WHEN DetainerMisdemeanorFlag = 'X' THEN 3
                 END AS detainer_score
        FROM 
            `{{project_id}}.{{raw_data_up_to_date_views_dataset}}.Detainer_latest` dis
        INNER JOIN
            `{{project_id}}.{{normalized_state_dataset}}.state_person_external_id` pei
        ON
            dis.OffenderID = pei.external_id
        AND
            pei.state_code = 'US_TN'
        WHERE
            DetainerFelonyFlag = 'X' OR DetainerMisdemeanorFlag = 'X'
    )
    ,
    {create_sub_sessions_with_attributes('detainers')}
    /* If someone has more than one detainer during the same time, they will have
    multiple sub-sessions. We deduplicate by taking the maximum
    score associated with each sub-session */
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        MAX(detainer_score) AS q8_score,
    FROM
        sub_sessions_with_attributes
    GROUP BY
        1,2,3,4
        
"""

US_TN_CAF_Q8_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_CAF_Q8_VIEW_NAME,
    description=US_TN_CAF_Q8_VIEW_DESCRIPTION,
    view_query_template=US_TN_CAF_Q8_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_CAF_Q8_VIEW_BUILDER.build_and_print()
