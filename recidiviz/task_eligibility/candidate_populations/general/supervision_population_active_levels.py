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
"""Defines a candidate population view containing all people who are actively
supervised on parole/probation at any point in time, and whose supervision levels are not Unassigned, In Custody, or Interstate Compact.
"""
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "SUPERVISION_POPULATION_ACTIVE_LEVELS"

_DESCRIPTION = """Selects all spans of time in which a person is actively supervised on parole/probation/dual and
whose supervision levels are not Unassigned, In Custody, or Interstate Compact, as tracked by data in our `sessions` dataset.
"""

_QUERY_TEMPLATE = """
/*{description}*/
-- TODO(#15060): Update to use query fragment once #14951 is merged in
SELECT DISTINCT
    state_code,
    person_id,
    start_date,
    -- Convert end_date from inclusive to exclusive
    DATE_ADD(end_date, INTERVAL 1 DAY) AS end_date,
FROM `{project_id}.{sessions_dataset}.dataflow_sessions_materialized`,
UNNEST(session_attributes) AS attr
WHERE attr.compartment_level_1 = "SUPERVISION"
    AND attr.compartment_level_2 IN ("PAROLE","PROBATION","DUAL")
    AND attr.correctional_level NOT IN (
        "UNASSIGNED",
        "IN_CUSTODY",
        "INTERSTATE_COMPACT"
    )
"""

VIEW_BUILDER: StateAgnosticTaskCandidatePopulationBigQueryViewBuilder = (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder(
        population_name=_POPULATION_NAME,
        population_spans_query_template=_QUERY_TEMPLATE,
        description=_DESCRIPTION,
        sessions_dataset=SESSIONS_DATASET,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
