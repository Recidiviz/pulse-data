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
"""A view that can helps identify invalid snooze notes so we can ask ME to go fix them.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.ingest.views.dataset_config import NORMALIZED_STATE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INVALID_SNOOZE_NOTES_VIEW_NAME = "invalid_snooze_notes"

INVALID_SNOOZE_NOTES_DESCRIPTION = """
Builds existence validation table to highlight invalid snooze notes.
"""

INVALID_SNOOZE_NOTES_QUERY_TEMPLATE = """
SELECT 
    state_code,
    state_code as region_code,
    person_id, 
    external_id, 
    supervision_contact_metadata
FROM `{project_id}.{state_dataset}.state_supervision_contact`
WHERE state_code = 'US_ME' AND supervision_contact_metadata LIKE '%malformatted_note%'
"""

INVALID_SNOOZE_NOTES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INVALID_SNOOZE_NOTES_VIEW_NAME,
    view_query_template=INVALID_SNOOZE_NOTES_QUERY_TEMPLATE,
    description=INVALID_SNOOZE_NOTES_DESCRIPTION,
    state_dataset=NORMALIZED_STATE_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INVALID_SNOOZE_NOTES_VIEW_BUILDER.build_and_print()
