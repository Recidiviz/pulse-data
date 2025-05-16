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
"""View of archived supervision_officer_outlier_status.csv exports from GCS"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

_VIEW_NAME = "supervision_officer_outlier_status_archive"

_DESCRIPTION = """
    View of archived outliers-etl-data-archive/*/supervision_officer_outlier_status.csv 
    and insights-etl-data-archive/*/supervision_officer_outlier_status.json exports from GCS
"""

_QUERY_TEMPLATE = """
WITH
split_path AS (
    SELECT
        officer_id,
        metric_id, 
        period,
        end_date,
        metric_rate, 
        NULL AS category_type,
        -- The old archive was before we started having multiple possible category types, so we can
        -- just say that any surfaced officer was being surfaced as the primary category type
        TRUE AS is_surfaced_category_type,
        caseload_type,
        target,
        threshold,
        status,
        NULL AS top_x_pct,
        NULL AS top_x_pct_percentile_value,
        NULL AS is_top_x_pct,
        CASE 
            WHEN state_code = "US_ID" THEN "US_IX"
            ELSE state_code
        END AS state_code,
        SPLIT(SUBSTRING(_FILE_NAME, 6), "/") AS path_parts
    FROM `{project_id}.export_archives.outliers_supervision_officer_outlier_status_archive`

    UNION ALL

    SELECT
        officer_id,
        metric_id, 
        period,
        end_date,
        metric_rate, 
        category_type,
        is_surfaced_category_type,
        caseload_type,
        target,
        threshold,
        status,
        top_x_pct,
        top_x_pct_percentile_value,
        is_top_x_pct,
        CASE 
            WHEN state_code = "US_ID" THEN "US_IX"
            ELSE state_code
        END AS state_code,
        SPLIT(SUBSTRING(_FILE_NAME, 6), "/") AS path_parts
    FROM `{project_id}.export_archives.insights_supervision_officer_outlier_status_archive`
)
, archives_with_date AS (
    SELECT DISTINCT
        split_path.* EXCEPT (path_parts),
        DATE(path_parts[SAFE_OFFSET(1)]) AS export_date
    FROM split_path
    -- exclude temp files we may have inadvertently archived
    WHERE ARRAY_LENGTH(path_parts) = 4
)
SELECT * EXCEPT (is_surfaced_category_type),
CASE
    WHEN is_surfaced_category_type IS NOT NULL THEN is_surfaced_category_type
    -- These are the dates of the full state launch of the opportunities module, which uses the
    -- supervisorHomepageWorkflows feature variant. This FV also gates specialized caseload
    -- disaggregation because we wanted to launch them together. The dates here aren't perfect
    -- because many supervisors got the feature variant ahead of the full state launch, but it's
    -- close enough and much easier to hardcode them than to join with roster archives to
    -- determine exactly when each supervisor got the FV.
    WHEN state_code = "US_IX" AND export_date >= "2024-11-20" THEN IFNULL(category_type, "ALL") = "SEX_OFFENSE_BINARY"
    WHEN state_code = "US_TN" AND export_date >= "2024-12-13" THEN IFNULL(category_type, "ALL") = "SEX_OFFENSE_BINARY"
    ELSE IFNULL(category_type, "ALL") = "ALL"
END AS is_surfaced_category_type
FROM archives_with_date
"""

SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=OUTLIERS_VIEWS_DATASET,
    view_id=_VIEW_NAME,
    description=_DESCRIPTION,
    view_query_template=_QUERY_TEMPLATE,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_OUTLIER_STATUS_ARCHIVE_VIEW_BUILDER.build_and_print()
