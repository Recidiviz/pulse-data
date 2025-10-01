# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Selects all spans of time in which a person is on supervision, using the same logic
employed by client record. This is used as a sub-criteria for Tasks criteria to filter
the population to the set of clients who are eligible and surfaced in client_record.
"""

from recidiviz.calculator.query.state.views.workflows.client_record_supervision_sessions import (
    STATES_WITH_OUT_OF_STATE_CLIENTS_INCLUDED,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_POPULATION_NAME = "PRIORITIZED_SUPERVISION_POPULATION_WITH_OFFICER"

_QUERY_TEMPLATE = f"""
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
FROM `{{project_id}}.sessions.dataflow_sessions_deduped_by_system_type_materialized`,
UNNEST (session_attributes) attr
LEFT JOIN `{{project_id}}.sessions.compartment_level_1_dedup_priority`
USING (compartment_level_1)
WHERE
    (attr.compartment_level_1 = "SUPERVISION"
        OR (
            -- TODO(#42395): Update when dataflow sessions out of state labeling is fixed
            -- Include other supervision sessions w/ supervision custodial authosrity
            attr.custodial_authority = "SUPERVISION_AUTHORITY"
            AND attr.compartment_level_1 IN ("SUPERVISION_OUT_OF_STATE", "INVESTIGATION")
        )
        OR (
            state_code IN ({STATES_WITH_OUT_OF_STATE_CLIENTS_INCLUDED})
            AND attr.compartment_level_1 = "SUPERVISION_OUT_OF_STATE"
        )
    )
    AND attr.supervising_officer_external_id IS NOT NULL
-- Prioritize the supervision row over the other compartment types
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, start_date ORDER BY IFNULL(priority, 999)
) = 1
"""

VIEW_BUILDER: StateAgnosticTaskCandidatePopulationBigQueryViewBuilder = (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder(
        population_name=_POPULATION_NAME,
        population_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
