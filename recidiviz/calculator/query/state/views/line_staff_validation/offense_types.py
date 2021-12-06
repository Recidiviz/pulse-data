# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
""" Offense types

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.offense_types
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OFFENSE_TYPES_VIEW_NAME = "offense_types"

OFFENSE_TYPES_DESCRIPTION = """
"""

OFFENSE_TYPES_QUERY_TEMPLATE = """
 WITH latest_sentences AS (    
    SELECT compartment_sessions.state_code,
        state_person_external_id.external_id as person_external_id,
        compartment_sessions.person_id,
        MAX(compartment_sessions.session_id) AS session_id
    FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized` compartment_sessions
    JOIN `{project_id}.{sessions_dataset}.compartment_sentences_materialized` compartment_sentences
        ON compartment_sentences.state_code = compartment_sessions.state_code
        AND compartment_sentences.person_id = compartment_sessions.person_id
        AND compartment_sentences.session_id = compartment_sentences.session_id
    JOIN `{project_id}.state.state_person_external_id` state_person_external_id
        ON state_person_external_id.state_code = compartment_sessions.state_code
        AND state_person_external_id.person_id = compartment_sessions.person_id
        AND IF(compartment_sessions.state_code = 'US_PA', state_person_external_id.id_type = 'US_PA_PBPP', TRUE)
    GROUP BY compartment_sessions.state_code,
    state_person_external_id.external_id,
    compartment_sessions.person_id
)
SELECT 
    latest_sentences.state_code,
    latest_sentences.person_external_id,
    latest_sentences.person_id,
    latest_sentences.session_id,
    ARRAY_TO_STRING(compartment_sentences.offense_type, ',') AS offense_type
FROM latest_sentences 
JOIN `{project_id}.{sessions_dataset}.compartment_sentences_materialized` compartment_sentences USING (state_code, person_id, session_id);

"""

OFFENSE_TYPES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=OFFENSE_TYPES_VIEW_NAME,
    should_materialize=True,
    view_query_template=OFFENSE_TYPES_QUERY_TEMPLATE,
    description=OFFENSE_TYPES_DESCRIPTION,
    sessions_dataset=dataset_config.SESSIONS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OFFENSE_TYPES_VIEW_BUILDER.build_and_print()
