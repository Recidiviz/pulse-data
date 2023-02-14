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
"""Defines a candidate population view containing all people who are on parole or dual
supervision who are not on intensive supervision, or not paroled from the Special Alternative to Incarceration (SAI).
"""
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_latest_views_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_MI_PAROLE_DUAL_SUPERVISION_POPULATION_NOT_HIGH_OR_SAI"

_DESCRIPTION = """Defines a candidate population view containing all people who are on parole or dual
supervision who are not on intensive supervision, or not paroled from the Special Alternative to Incarceration (SAI),
as tracked by data in our `sessions` dataset. Here, intensive supervision levels are mapped to 
supervision level = "HIGH", and SAI levels are determined by raw text codes. 
"""

_QUERY_TEMPLATE = """
    SELECT DISTINCT
        state_code,
        person_id,
        start_date,
        end_date_exclusive AS end_date,
    FROM {project_id}.{sessions_dataset}.dataflow_sessions_materialized,
        UNNEST(session_attributes) attr
    LEFT JOIN `{project_id}.{raw_data_up_to_date_views_dataset}.ADH_REFERENCE_CODE_latest` ref 
        ON attr.correctional_level_raw_text = ref.reference_code_id
    WHERE state_code = "US_MI"
        AND attr.compartment_level_1 = "SUPERVISION"
        AND attr.compartment_level_2 IN ("PAROLE", "DUAL")
      --exclude intensive supervision which is mapped to supervision level = "HIGH"
        AND attr.correctional_level NOT IN ("HIGH")
      --exclude sessions that are parole from SAI
        AND LOWER(ref.description) NOT LIKE '%parole sai%'
"""

VIEW_BUILDER: StateSpecificTaskCandidatePopulationBigQueryViewBuilder = (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
        state_code=StateCode.US_MI,
        population_name=_POPULATION_NAME,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
        population_spans_query_template=_QUERY_TEMPLATE,
        raw_data_up_to_date_views_dataset=raw_latest_views_dataset_for_region(
            state_code=StateCode.US_MI,
            instance=DirectIngestInstance.PRIMARY,
        ),
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
