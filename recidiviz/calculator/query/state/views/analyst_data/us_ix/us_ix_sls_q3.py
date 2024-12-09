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
"""Computes Q3 (prior sentence severity) of Idaho's Reclassification of Security Level form at any point in time"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SENTENCE_SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_IX_SLS_Q3_VIEW_NAME = "us_ix_sls_q3"

US_IX_SLS_Q3_VIEW_DESCRIPTION = """Computes Q3 (prior sentence severity) of Idaho's Reclassification of Security Level
form at any point in time. See details of the Reclassification Form here:
https://drive.google.com/file/d/1lmzOgjjlcMuBlVvYEqfWOlbQ5HGjZpJ_/view?usp=sharing"""

US_IX_SLS_Q3_QUERY_TEMPLATE = """
    SELECT 
        span.state_code,
        span.person_id,
        --find the earliest high severity felony sentence date for each person 
        MIN(imposed_date) AS start_date,
        --once completed, a high severity felony will always result in a score of 3
        NULL AS end_date,
        3 AS q3_score,
    FROM `{project_id}.{sentence_sessions_dataset}.sentence_serving_period_materialized` span
    INNER JOIN `{project_id}.{sentence_sessions_dataset}.sentences_and_charges_materialized` sent
        USING (state_code, person_id, sentence_id)
    INNER JOIN `{project_id}.{normalized_state_dataset}.state_charge` sc
        ON sc.person_id = sent.person_id 
        AND sc.charge_id = sent.charge_id
    INNER JOIN `{project_id}.{us_ix_raw_data_up_to_date_dataset}.scl_OffenseType_latest` scl
        ON sc.offense_type = scl.OffenseTypeId
    WHERE span.state_code = 'US_IX'
        --only query high severity felonies
        AND sent.classification_type = 'FELONY'
        AND scl.SeverityIdForInitial ='5'
        --only query completed sentences
        AND end_date_exclusive IS NOT NULL
    GROUP BY 1,2
"""

US_IX_SLS_Q3_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_IX_SLS_Q3_VIEW_NAME,
    description=US_IX_SLS_Q3_VIEW_DESCRIPTION,
    view_query_template=US_IX_SLS_Q3_QUERY_TEMPLATE,
    sentence_sessions_dataset=SENTENCE_SESSIONS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    us_ix_raw_data_up_to_date_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_IX, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_IX_SLS_Q3_VIEW_BUILDER.build_and_print()
