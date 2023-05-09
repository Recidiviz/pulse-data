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
"""Spans of time with the projected max completion date for clients under supervision
as indicated by the sentences that were active during that span."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.sessions_query_fragments import (
    join_sentence_spans_to_compartment_sessions,
)
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_projected_completion_date_spans"

_VIEW_DESCRIPTION = """
Spans of time with the projected max completion date for clients under supervision as
indicated by the sentences that were active during that span.
"""

STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION = [
    StateCode.US_ND,
    StateCode.US_MI,
]

_QUERY_TEMPLATE = f"""
    SELECT
        span.state_code,
        span.person_id,
        span.start_date,
        span.end_date_exclusive,
        span.end_date_exclusive AS end_date,
        MAX(sent.projected_completion_date_max) AS projected_completion_date_max,
    {join_sentence_spans_to_compartment_sessions(compartment_level_1_to_overlap="SUPERVISION")}
    WHERE
        -- Exclude incarceration sentences for states that store all supervision
        -- sentence data (including parole)
        -- separately in supervision sentences
        (sent.state_code NOT IN ("{{excluded_incarceration_states}}") OR sent.sentence_type = "SUPERVISION")
        -- Exclude states with state-specific logic
        AND sent.state_code NOT IN ("US_ID", "US_IX", "US_TN")
    GROUP BY 1, 2, 3, 4
UNION ALL
    SELECT * 
    FROM `{{project_id}}.{{sessions_dataset}}.us_id_supervision_projected_completion_date_spans_materialized`
UNION ALL 
    SELECT * 
    FROM `{{project_id}}.{{sessions_dataset}}.us_ix_supervision_projected_completion_date_spans_materialized`
UNION ALL
    SELECT * 
    FROM `{{project_id}}.{{sessions_dataset}}.us_tn_supervision_projected_completion_date_spans_materialized`
"""

SUPERVISION_LATEST_PROJECTED_COMPLETION_DATE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    sessions_dataset=SESSIONS_DATASET,
    excluded_incarceration_states='", "'.join(
        [state.name for state in STATES_WITH_NO_INCARCERATION_SENTENCES_ON_SUPERVISION]
    ),
    should_materialize=True,
    clustering_fields=["state_code", "person_id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_LATEST_PROJECTED_COMPLETION_DATE_VIEW_BUILDER.build_and_print()
