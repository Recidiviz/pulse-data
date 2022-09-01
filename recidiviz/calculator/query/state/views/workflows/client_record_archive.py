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
"""View of archived client_record.json exports from GCS"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    EXPORT_ARCHIVES_DATASET,
    STATE_BASE_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.populate_missing_exports_template import (
    populate_missing_export_dates,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_RECORD_ARCHIVE_VIEW_NAME = "client_record_archive"

CLIENT_RECORD_ARCHIVE_VIEW_DESCRIPTION = (
    """View of archived client_record.json exports from GCS"""
)

CLIENT_RECORD_ARCHIVE_QUERY_TEMPLATE = """
    /* {description} */
    WITH
    split_path AS (
        SELECT
            *,
            SPLIT(SUBSTRING(_FILE_NAME, 6), "/") AS path_parts,
        FROM `{project_id}.{export_archives_dataset}.workflows_client_record_archive`
        -- exclude temp files we may have inadvertently archived
        WHERE _FILE_NAME NOT LIKE "%/staging/%"
    )
    , records_by_state_by_date AS (
        -- dedupes repeat uploads for the same date
        SELECT DISTINCT
            * EXCEPT (path_parts, remaining_criteria_needed),
            CAST(remaining_criteria_needed AS INT64) AS remaining_criteria_needed,
            DATE(path_parts[OFFSET(1)]) AS export_date,
            path_parts[OFFSET(2)] AS state_code,
        FROM split_path
    )
    {populate_missing_export_dates}
    SELECT
        generated_export_date AS date_of_supervision,
        person_id,
        records_by_state_by_date.*,
    FROM date_to_archive_map
    LEFT JOIN records_by_state_by_date USING (state_code, export_date)
    LEFT JOIN `{project_id}.{state_base_dataset}.state_person_external_id` pei
        ON records_by_state_by_date.state_code = pei.state_code
        AND records_by_state_by_date.person_external_id = pei.external_id
"""

CLIENT_RECORD_ARCHIVE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENT_RECORD_ARCHIVE_VIEW_NAME,
    description=CLIENT_RECORD_ARCHIVE_VIEW_DESCRIPTION,
    view_query_template=CLIENT_RECORD_ARCHIVE_QUERY_TEMPLATE,
    should_materialize=True,
    export_archives_dataset=EXPORT_ARCHIVES_DATASET,
    state_base_dataset=STATE_BASE_DATASET,
    populate_missing_export_dates=populate_missing_export_dates(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_ARCHIVE_VIEW_BUILDER.build_and_print()
