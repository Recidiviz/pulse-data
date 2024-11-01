# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""View of all US_ME case notes to be imported to cloud storage."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import CASE_NOTES_PROTOTYPE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CASE_NOTES_VIEW_NAME = "case_notes"

CASE_NOTES_VIEW_DESCRIPTION = """All case notes formatted as JSON. This allows for structured import to cloud storage per
    https://cloud.google.com/generative-ai-app-builder/docs/prepare-data#bigquery-structured.
    
    The id field is the concatenation of state_code, external_id, and note_id so that it's unique per row.
    The jsonData field is a JSON string with the following fields:
        state_code, external_id, note_id, note_body, note_title, note_date, note_type, note_mode
    """

CASE_NOTES_QUERY_TEMPLATE = """
    SELECT id, jsonData
    FROM
        (
        SELECT
            notes.state_code,
            notes.external_id,
            CONCAT("US_IX", "_", notes.external_id, "_", note_id) as id,
            TO_JSON_STRING(STRUCT(notes.state_code, notes.external_id, note_id, note_body, note_title, note_date, note_type, note_mode))
                as jsonData,
        FROM `{project_id}.{case_notes_prototype_dataset}.us_ix_case_notes` notes
    
        UNION ALL    
    
        SELECT
            notes.state_code,
            notes.external_id,
            CONCAT("US_ME", "_", notes.external_id, "_", note_id) as id,
            TO_JSON_STRING(STRUCT(notes.state_code, notes.external_id, note_id, note_body, note_title, note_date, note_type, note_mode))
                as jsonData,
        FROM `{project_id}.{case_notes_prototype_dataset}.us_me_case_notes` notes
        ) all_notes
    # Filter to only those who are currently on supervision
    # TODO(#32764): Also include those who are currently incarcerated once a use case requires it
    INNER JOIN `{project_id}.normalized_state.state_person_external_id` spei
        ON all_notes.state_code = spei.state_code AND all_notes.external_id = spei.external_id
    INNER JOIN `{project_id}.sessions.compartment_sessions_materialized` cs
        ON cs.state_code = spei.state_code AND cs.person_id = spei.person_id
    WHERE cs.compartment_level_1 = 'SUPERVISION' AND cs.end_date_exclusive IS NULL
"""

CASE_NOTES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=CASE_NOTES_PROTOTYPE_DATASET,
    view_id=CASE_NOTES_VIEW_NAME,
    view_query_template=CASE_NOTES_QUERY_TEMPLATE,
    description=CASE_NOTES_VIEW_DESCRIPTION,
    case_notes_prototype_dataset=CASE_NOTES_PROTOTYPE_DATASET,
    should_materialize=True,
    clustering_fields=["id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_NOTES_VIEW_BUILDER.build_and_print()
