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

"""A view revealing when a person_id is duplicated on a given day in client_record_archive"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_VIEW_NAME = (
    "client_record_archive_duplicate_person_ids"
)

CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_DESCRIPTION = (
    """Duplicate person_ids found on a given day in client_record_archive"""
)

CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        date_of_supervision,
        state_code as region_code,
        COUNT(DISTINCT person_id) AS unique_person_ids,
        COUNT(person_id) AS client_records,
    FROM `{project_id}.{workflows_dataset}.client_record_archive_materialized`
    WHERE (
        state_code != "US_TN" 
        -- this is an expected condition in TN because new people may appear
        -- in the raw data this view uses before they are ingested; therefore
        -- in TN only we can ignore NULL person_id values for this validation
        OR person_id IS NOT NULL
    )
    GROUP BY 1, 2
    ORDER BY 1 DESC
"""

CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_VIEW_NAME,
    view_query_template=CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_QUERY_TEMPLATE,
    description=CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_DESCRIPTION,
    workflows_dataset=state_dataset_config.WORKFLOWS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_ARCHIVE_DUPLICATE_PERSON_IDS_VIEW_BUILDER.build_and_print()
