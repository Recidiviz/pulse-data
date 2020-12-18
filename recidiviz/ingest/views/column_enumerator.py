# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""A view collector that generates a BigQuery view that enumerates the columns
across all tables in the state dataset."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.ingest.views.dataset_config import VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COLUMN_ENUMERATOR_QUERY_TEMPLATE = \
    """
SELECT
    table_name,
    column_name
FROM
    `{project_id}.state.INFORMATION_SCHEMA.COLUMNS`
WHERE
    table_catalog = '{project_id}'
    AND table_schema = 'state'

"""


COLUMN_ENUMERATOR_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id='ingest_state_metadata_columns',
    view_query_template=COLUMN_ENUMERATOR_QUERY_TEMPLATE,
)


if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        COLUMN_ENUMERATOR_VIEW_BUILDER.build_and_print()
