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

from google.cloud import bigquery

from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.calculator.query.state.views.workflows.firestore.client_record_ctes import (
    STATES_WITH_OUT_OF_STATE_CLIENTS_INCLUDED,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#45794): Deprecate this criteria once candidate populations have been incorporated into Tasks
_CRITERIA_NAME = "PRIORITIZED_SUPERVISION_POPULATION"

_QUERY_TEMPLATE = f"""
SELECT
    state_code,
    person_id,
    start_date,
    end_date_exclusive AS end_date,
    TRUE AS meets_criteria,
    TO_JSON(STRUCT(
        TRUE AS on_prioritized_supervision
    )) AS reason,
    TRUE AS on_prioritized_supervision,
FROM `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_deduped_by_system_type_materialized`,
UNNEST (session_attributes) attr
LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_1_dedup_priority`
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
-- Prioritize the supervision row over the other compartment types
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY state_code, person_id, start_date ORDER BY IFNULL(priority, 999)
) = 1
"""

VIEW_BUILDER: StateAgnosticTaskCriteriaBigQueryViewBuilder = (
    StateAgnosticTaskCriteriaBigQueryViewBuilder(
        criteria_name=_CRITERIA_NAME,
        criteria_spans_query_template=_QUERY_TEMPLATE,
        description=__doc__,
        sessions_dataset=SESSIONS_DATASET,
        reasons_fields=[
            ReasonsField(
                name="on_prioritized_supervision",
                type=bigquery.enums.StandardSqlTypeNames.BOOL,
                description="Specifies whether client is on prioritized supervision",
            ),
        ],
    )
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
