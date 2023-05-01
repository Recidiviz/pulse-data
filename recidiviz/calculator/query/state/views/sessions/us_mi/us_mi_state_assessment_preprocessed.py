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
"""Preprocessed view of COMPAS assessments in Michigan to be used in assessment score sessions"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#20530) deprecate this view in favor of moving this logic further upstream in ingest process
US_MI_STATE_ASSESSMENT_PREPROCESSED_VIEW_NAME = "us_mi_state_assessment_preprocessed"

US_MI_STATE_ASSESSMENT_PREPROCESSED_VIEW_DESCRIPTION = """Preprocessed view of COMPAS assessments in Michigan to be used
in assessment score sessions. Deduplicates assessment scores for VFO/NFVO scales such that each person has at most
one assessment_level_raw_text value for each day.
"""

US_MI_STATE_ASSESSMENT_PREPROCESSED_QUERY_TEMPLATE = """
WITH class_raw_text_mapping AS (
/* This CTE selects relevant assessment class, types, and scales and creates a new more generalized external_id.
 Then it uses the priority levels in assessment_level_dedup_priority to create a view unique on person_id, 
assessment_date, and assessment_class_raw_text_mapping*/
    SELECT 
      IF(assessment_class_raw_text IN ("8138", "8042"), "NON-VFO", "VFO") AS assessment_class_raw_text_mapping,
      --only select person_external_id, assessment_type, and assessment_id from state_assessment.external_id
      --excludes the last segment which is the scale i.e. 8138, 8139 
      --can be linked back to all relevant rows in state_assessment by removing the last segment in state_assessment.external_id before joining
      REGEXP_EXTRACT(external_id, r'\\w+-\\w+-\\w+') AS external_id,
      * EXCEPT (external_id)
    FROM `{project_id}.{normalized_state_dataset}.state_assessment`
    LEFT JOIN `{project_id}.{sessions_dataset}.assessment_level_dedup_priority`
        USING(assessment_level)
    WHERE state_code = "US_MI"
      AND assessment_class = "RISK"
      AND assessment_type = "COMPAS"
      AND assessment_class_raw_text in ("8043", "8042", "8138", "8139")
    QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id,assessment_date,assessment_class_raw_text_mapping ORDER BY COALESCE(assessment_level_priority, 999))=1
)
/* Finally, this CTE concatenates the VFO level to the NVFO level to create a new assessment_level_raw_text that
can be used to recommend a supervision level. The rest of the fields are (such as assessment_metadata, etc) 
are tied to the VFO assessment record */ 
    SELECT 
    --defer to the vfo score for all other fields 
    vfo.* EXCEPT(assessment_class_raw_text_mapping,assessment_level_raw_text, assessment_level_priority),
    IF((vfo.assessment_level = "INTERNAL_UNKNOWN" OR nvfo.assessment_level = "INTERNAL_UNKNOWN"), "INTERNAL_UNKNOWN", CONCAT(vfo.assessment_level,"/",nvfo.assessment_level)) AS assessment_level_raw_text
    FROM class_raw_text_mapping vfo
    INNER JOIN class_raw_text_mapping nvfo
      ON vfo.person_id = nvfo.person_id
      AND vfo.assessment_date = nvfo.assessment_date
      AND vfo.assessment_class_raw_text_mapping = "VFO"
      AND nvfo.assessment_class_raw_text_mapping = "NON-VFO"
"""

US_MI_STATE_ASSESSMENT_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_MI_STATE_ASSESSMENT_PREPROCESSED_VIEW_NAME,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    sessions_dataset=SESSIONS_DATASET,
    description=US_MI_STATE_ASSESSMENT_PREPROCESSED_VIEW_DESCRIPTION,
    view_query_template=US_MI_STATE_ASSESSMENT_PREPROCESSED_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_MI_STATE_ASSESSMENT_PREPROCESSED_VIEW_BUILDER.build_and_print()
