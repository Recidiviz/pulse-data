# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Selects all spans of time in which a person is currently incarcerated in Arkansas
under the Protect Arkansas Act (PAA), identified by cohort_code = '4'. Uses
compartment_level_0 super-sessions so that start_date reflects the beginning of the
overall incarceration term regardless of internal transfers between incarceration types."""
from recidiviz.calculator.query.state.dataset_config import (
    ANALYST_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "US_AR_PAA_INCARCERATED_POPULATION"

_QUERY_TEMPLATE = """
    SELECT
        s.state_code,
        s.person_id,
        s.start_date,
        s.end_date,
    FROM `{project_id}.{sessions_dataset}.compartment_level_0_super_sessions_materialized` s
    INNER JOIN `{project_id}.{analyst_dataset}.us_ar_sentence_dates_preprocessed_materialized` sd
        USING (person_id)
    WHERE s.state_code = 'US_AR'
      AND s.compartment_level_0 = 'INCARCERATION'
      AND s.end_date IS NULL
      AND sd.cohort_code = '4'
"""

VIEW_BUILDER: StateSpecificTaskCandidatePopulationBigQueryViewBuilder = (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
        state_code=StateCode.US_AR,
        population_name=_POPULATION_NAME,
        population_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        sessions_dataset=SESSIONS_DATASET,
        analyst_dataset=ANALYST_VIEWS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
