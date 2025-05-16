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
"""View of archived supervision_officers.csv exports from GCS"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_officers_archive"

_DESCRIPTION = """
    View of archived outliers-etl-data-archive/*/supervision_officers.csv 
    and insights-etl-data-archive/*/supervision_officers.json exports from GCS
"""

_QUERY_TEMPLATE = """
WITH
split_path AS (
    SELECT
        external_id,
        staff_id,
        full_name,
        pseudonymized_id,
        CASE
            WHEN supervisor_external_id IS NULL THEN []
            ELSE [supervisor_external_id]
        END AS supervisor_external_ids,
        supervision_district,
        specialized_caseload_type,
        CASE 
            WHEN state_code = "US_ID" THEN "US_IX"
            ELSE state_code
        END AS state_code,
        SPLIT(SUBSTRING(_FILE_NAME, 6), "/") AS path_parts,
    FROM `{project_id}.export_archives.outliers_supervision_officers_archive`

    UNION ALL
    
    SELECT
        external_id,
        staff_id,
        full_name,
        pseudonymized_id,
        supervisor_external_ids,
        supervision_district,
        specialized_caseload_type,
        CASE 
            WHEN state_code = "US_ID" THEN "US_IX"
            ELSE state_code
        END AS state_code,
        SPLIT(SUBSTRING(_FILE_NAME, 6), "/") AS path_parts,
    FROM `{project_id}.export_archives.insights_supervision_officers_archive`
)

SELECT
    split_path.* EXCEPT (path_parts),
    DATE(path_parts[SAFE_OFFSET(1)]) AS export_date
FROM split_path
-- exclude temp files we may have inadvertently archived
WHERE ARRAY_LENGTH(path_parts) = 4
"""

SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICERS_ARCHIVE_VIEW_BUILDER.build_and_print()
