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
from recidiviz.calculator.query.state.views.prototypes.case_note_search.us_ix.case_notes_template import (
    US_IX_CASE_NOTES_TEMPLATE,
)
from recidiviz.calculator.query.state.views.prototypes.case_note_search.us_me.case_notes_template import (
    US_ME_CASE_NOTES_TEMPLATE,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CASE_NOTES_VIEW_NAME = "case_notes"

CASE_NOTES_VIEW_DESCRIPTION = """Case notes for all US_ME and US_IX cases that are currently on supervision. This view is used:
1. as a precursor for the case_notes_data_store view which backs a datastore for Vertex AI search
2. for exact match case note search, which requires a direct search on the table
3. for metric export of individual case notes, which backs the unstructured data for the data store
"""

CASE_NOTES_QUERY_TEMPLATE = f"""
WITH 
    ix_case_notes AS 
        ({US_IX_CASE_NOTES_TEMPLATE}),
    me_case_notes AS 
        ({US_ME_CASE_NOTES_TEMPLATE}),
    all_notes AS 
    (
        SELECT * FROM ix_case_notes
        UNION ALL
        SELECT * FROM me_case_notes
    ) 
    SELECT all_notes.state_code, all_notes.external_id, all_notes.note_id, all_notes.note_body, all_notes.note_title, all_notes.note_date, all_notes.note_type, all_notes.note_mode, CONCAT(all_notes.state_code, "_", all_notes.external_id, "_", all_notes.note_id) as id,
    FROM all_notes
    # Filter to only those who are currently on supervision
    # TODO(#32764): Also include those who are currently incarcerated once a use case requires it
    INNER JOIN `{{project_id}}.normalized_state.state_person_external_id` spei
        ON all_notes.state_code = spei.state_code AND all_notes.external_id = spei.external_id
    INNER JOIN `{{project_id}}.sessions.compartment_sessions_materialized` cs
        ON cs.state_code = spei.state_code AND cs.person_id = spei.person_id
    WHERE cs.compartment_level_1 = 'SUPERVISION' AND cs.end_date_exclusive IS NULL
"""

CASE_NOTES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=CASE_NOTES_PROTOTYPE_DATASET,
    view_id=CASE_NOTES_VIEW_NAME,
    view_query_template=CASE_NOTES_QUERY_TEMPLATE,
    description=CASE_NOTES_VIEW_DESCRIPTION,
    should_materialize=True,
    clustering_fields=["id"],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_NOTES_VIEW_BUILDER.build_and_print()
