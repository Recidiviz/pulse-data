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
"""Flag open incarceration/supervision sessions that have no corresponding active
(v2) sentence, so there are no sentences where the most recent status snapshot
has a serving-type sentence status. This can occur because there are no sentence
status snapshots ingested for the person incarcerated/supervised, or all of the
relevant sentences have a terminated status type from the most recent snapshot."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.sessions.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

OPEN_SESSIONS_WITHOUT_ACTIVE_SENTENCES_VIEW_NAME = (
    "open_sessions_without_active_sentences"
)

OPEN_SESSIONS_WITHOUT_ACTIVE_SENTENCES_QUERY_TEMPLATE = f"""
SELECT
    sessions.state_code,
    sessions.state_code AS region_code,
    sessions.person_id,
    sessions.compartment_level_1,
    sessions.compartment_level_2,
    TRUE AS has_open_session,
    sentences.person_id IS NOT NULL AS has_active_sentence,
FROM
    `{{project_id}}.{COMPARTMENT_SESSIONS_VIEW_BUILDER.table_for_query.to_str()}` sessions
-- Restrict to states with `state_sentence_status_snapshot` hydrated
INNER JOIN (
    SELECT DISTINCT
        state_code,
    FROM `{{project_id}}.normalized_state.state_sentence_status_snapshot`
)
USING
    (state_code)
LEFT JOIN
    `{{project_id}}.sentence_sessions_v2_all.overlapping_sentence_serving_periods_materialized` sentences
ON
    sessions.state_code = sentences.state_code
    AND sessions.person_id = sentences.person_id
    AND sentences.end_date_exclusive IS NULL
WHERE
    sessions.compartment_level_1 IN ("INCARCERATION", "SUPERVISION")
    -- Drop compartments that are less likely to have corresponding sentence data
    AND sessions.compartment_level_2 NOT IN (
        "TEMPORARY_CUSTODY",
        "INTERNAL_UNKNOWN",
        "INFORMAL_PROBATION",
        "ABSCONSION",
        "WARRANT_STATUS"
    )
    AND sessions.end_date_exclusive IS NULL
"""


OPEN_SESSIONS_WITHOUT_ACTIVE_SENTENCES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=OPEN_SESSIONS_WITHOUT_ACTIVE_SENTENCES_VIEW_NAME,
    view_query_template=OPEN_SESSIONS_WITHOUT_ACTIVE_SENTENCES_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OPEN_SESSIONS_WITHOUT_ACTIVE_SENTENCES_VIEW_BUILDER.build_and_print()
