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
"""Materialized view for zero tolerance contact codes in TN"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    NORMALIZED_STATE_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_latest_views_dataset_for_region
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_DISCIPLINARIES_PREPROCESSED_VIEW_NAME = "us_tn_disciplinaries_preprocessed"

US_TN_DISCIPLINARIES_PREPROCESSED_VIEW_DESCRIPTION = (
    """Materialized view for zero tolerance contact codes in TN"""
)

US_TN_DISCIPLINARIES_PREPROCESSED_QUERY_TEMPLATE = """
    WITH incidents AS (
        SELECT inc.person_id,
                inc.state_code,
                inc.incident_date AS disciplinary_date,
                inc.incident_type,
                inc.incident_type_raw_text,
                inc_outcome.hearing_date AS disposition_date,
                JSON_EXTRACT_SCALAR(inc.incident_metadata, "$.Class") AS disciplinary_class,
                JSON_EXTRACT_SCALAR(inc.incident_metadata, "$.InjuryLevel") AS injury_level,
                JSON_EXTRACT_SCALAR(inc.incident_metadata, "$.Disposition") AS disposition,
                IncidentID,
          FROM `{project_id}.{normalized_state_dataset}.state_incarceration_incident` inc
          LEFT JOIN `{project_id}.{normalized_state_dataset}.state_incarceration_incident_outcome` inc_outcome
            USING(incarceration_incident_id)
          -- TODO(#20693): Remove hack when entity deletion exists
          INNER JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            ON inc.person_id = pei.person_id
            AND inc.state_code = pei.state_code
          INNER JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.Disciplinary_latest` disc
            ON pei.external_id = disc.OffenderID
            AND SPLIT(inc.external_id,'-')[SAFE_OFFSET(1)] = disc.IncidentID        
    )
    SELECT *,
        CASE 
            WHEN incident_type_raw_text IN ('AOO', 'AOW', 'ASO', 'ASW', 'AVO', 'ASV')
            AND injury_level = '3'
                THEN 7
            WHEN incident_type_raw_text IN ('AOO', 'ASO', 'AVO') 
            AND injury_level IN ('1','2')
                THEN 3
            WHEN incident_type_raw_text IN ('AOW', 'ASW', 'ASV')  
            AND injury_level IN ('1','2')
                THEN 5
        END AS assault_score,
    FROM
        incidents
        
"""

US_TN_DISCIPLINARIES_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    view_id=US_TN_DISCIPLINARIES_PREPROCESSED_VIEW_NAME,
    description=US_TN_DISCIPLINARIES_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_TN_DISCIPLINARIES_PREPROCESSED_QUERY_TEMPLATE,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_TN, instance=DirectIngestInstance.PRIMARY
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_DISCIPLINARIES_PREPROCESSED_VIEW_BUILDER.build_and_print()
