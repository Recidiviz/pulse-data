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
"""US_MI - supervision level raw text mappings to relevant levels """

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

US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_NAME = (
    "us_mi_supervision_level_raw_text_mappings"
)

US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_DESCRIPTION = """Supervision level raw text mappings to relevant levels for Workflows opportunities"""

US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_QUERY_TEMPLATE = """
/* OMNI */
SELECT DISTINCT
    'OMNI' as source,
    REGEXP_EXTRACT(sp.supervision_level_raw_text, r'(\\d+)') AS supervision_level_raw_text,
    description,
    IF(LOWER(ref.description) LIKE '%sai%',TRUE, NULL) AS is_sai,
    IF(((LOWER(ref.description) LIKE '%ems%' OR LOWER(ref.description) LIKE '%gps%')
      AND (LOWER(ref.description) NOT LIKE '%non-ems%' AND LOWER(ref.description) NOT LIKE '%non-gps%')), TRUE, NULL) AS is_em,
    IF(LOWER(ref.description) LIKE '%admin%' OR LOWER(ref.description) LIKE '%mail%',TRUE, NULL) AS is_minimum_excluded,
    IF(LOWER(ref.description) LIKE '%telephone%',TRUE, NULL) AS is_telephone,
    IF(LOWER(ref.description) LIKE '%person%',TRUE, NULL) AS is_minimum_in_person,
    IF(LOWER(ref.description) LIKE '%low%',TRUE, NULL) AS is_minimum_low,
FROM `{project_id}.{normalized_state_dataset}.state_supervision_period` sp
LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.ADH_REFERENCE_CODE_latest` ref 
    ON REGEXP_EXTRACT(sp.supervision_level_raw_text, r'(\\d+)') = ref.reference_code_id
WHERE state_code = 'US_MI'
    -- raw text without an underscore indicate the raw text came from OMNI
    AND NOT REGEXP_CONTAINS(supervision_level_raw_text, r'_')
    AND sp.supervision_level_raw_text IS NOT NULL

UNION ALL 
/* COMS */ 
SELECT DISTINCT
    'COMS' as source,
    supervision_level_raw_text AS supervision_level_raw_text,
    supervision_level_raw_text AS description,
    IF(LOWER(supervision_level_raw_text) LIKE '%special alternative incarceration%',TRUE, NULL) AS is_sai,
    IF(((LOWER(supervision_level_raw_text) LIKE '%ems%' OR LOWER(supervision_level) LIKE '%gps%')
      AND (LOWER(supervision_level_raw_text) NOT LIKE '%non-ems%' AND LOWER(supervision_level) NOT LIKE '%non-gps%')), TRUE, NULL) AS is_em,
    IF(LOWER(supervision_level_raw_text) LIKE '%admin%',TRUE, NULL) AS is_minimum_excluded,
    IF(LOWER(supervision_level_raw_text) LIKE '%trs%',TRUE, NULL) AS is_telephone,
    IF(LOWER(supervision_level_raw_text) LIKE '%person%',TRUE, NULL) AS is_minimum_in_person,
    IF(LOWER(supervision_level_raw_text) LIKE '%low%' and LOWER(supervision_level_raw_text) not like '%level a%',TRUE, NULL) AS is_minimum_low,
FROM `{project_id}.{normalized_state_dataset}.state_supervision_period` sp
WHERE state_code = 'US_MI' 
    -- raw text with an underscore indicate the raw text came from COMS
    AND REGEXP_CONTAINS(supervision_level_raw_text, r'_')
    AND sp.supervision_level_raw_text IS NOT NULL
"""

US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    view_id=US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_NAME,
    dataset_id=ANALYST_VIEWS_DATASET,
    description=US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_DESCRIPTION,
    view_query_template=US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_QUERY_TEMPLATE,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
        state_code=StateCode.US_MI,
        instance=DirectIngestInstance.PRIMARY,
    ),
    should_materialize=False,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_SUPERVISION_LEVEL_RAW_TEXT_MAPPINGS_VIEW_BUILDER.build_and_print()
