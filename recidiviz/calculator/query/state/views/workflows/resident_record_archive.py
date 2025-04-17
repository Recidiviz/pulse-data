# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""View of archived resident_record.json exports from GCS"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    EXPORT_ARCHIVES_DATASET,
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RESIDENT_RECORD_ARCHIVE_VIEW_NAME = "resident_record_archive"

RESIDENT_RECORD_ARCHIVE_VIEW_DESCRIPTION = (
    """View of archived resident_record.json exports from GCS"""
)

RESIDENT_RECORD_ARCHIVE_QUERY_TEMPLATE = """
    WITH
    split_path AS (
        SELECT
            *,
            SPLIT(SUBSTRING(_FILE_NAME, 6), "/") AS path_parts,
        FROM `{project_id}.{export_archives_dataset}.workflows_resident_record_archive`
        -- exclude temp files we may have inadvertently archived
        WHERE _FILE_NAME NOT LIKE "%/staging/%"
    )
    , records_by_state_by_date AS (
        SELECT DISTINCT
            split_path.* EXCEPT (path_parts, person_external_id, all_eligible_opportunities, person_id),
            ARRAY_TO_STRING(all_eligible_opportunities, ",") AS all_eligible_opportunities,
            DATE(path_parts[SAFE_OFFSET(1)]) AS export_date,
            IF(path_parts[OFFSET(2)] = "US_MI" AND NOT STARTS_WITH(person_external_id, "0"), "0" || person_external_id, person_external_id) AS person_external_id,
            IF(path_parts[SAFE_OFFSET(2)]="US_ID", "US_IX", path_parts[SAFE_OFFSET(2)]) AS state_code
        FROM split_path
    )
    SELECT
        records_by_state_by_date.*,
        person_id
    FROM records_by_state_by_date
    -- Pull the person_id from the latest version of ingested data
    INNER JOIN `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` latest_person_id
        ON latest_person_id.state_code = records_by_state_by_date.state_code
        AND latest_person_id.person_external_id = records_by_state_by_date.person_external_id
        AND latest_person_id.system_type = "INCARCERATION"
"""

RESIDENT_RECORD_ARCHIVE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=RESIDENT_RECORD_ARCHIVE_VIEW_NAME,
    description=RESIDENT_RECORD_ARCHIVE_VIEW_DESCRIPTION,
    view_query_template=RESIDENT_RECORD_ARCHIVE_QUERY_TEMPLATE,
    workflows_dataset=WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
    export_archives_dataset=EXPORT_ARCHIVES_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RESIDENT_RECORD_ARCHIVE_VIEW_BUILDER.build_and_print()
