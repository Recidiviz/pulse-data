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
    WORKFLOWS_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.views.workflows.user_event_template import (
    first_ix_export_date,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

CLIENT_RECORD_ARCHIVE_VIEW_NAME = "client_record_archive"

CLIENT_RECORD_ARCHIVE_VIEW_DESCRIPTION = (
    """View of archived client_record.json exports from GCS"""
)

CLIENT_RECORD_ARCHIVE_QUERY_TEMPLATE = """
    WITH
    split_path AS (
        SELECT
            *,
            SPLIT(SUBSTRING(_FILE_NAME, 6), "/") AS path_parts,
            "SUPERVISION" AS system_type,
        FROM `{project_id}.{export_archives_dataset}.workflows_client_record_archive`
        -- exclude temp files we may have inadvertently archived
        WHERE _FILE_NAME NOT LIKE "%/staging/%"
    )
    , records_by_state_by_date AS (
        -- dedupes repeat uploads for the same date
        SELECT DISTINCT
            * EXCEPT (path_parts, person_external_id, remaining_criteria_needed, all_eligible_opportunities),
            IF(path_parts[OFFSET(2)] = "US_MI" AND NOT STARTS_WITH(person_external_id, "0"), "0" || person_external_id, person_external_id) AS person_external_id,
            CAST(remaining_criteria_needed AS INT64) AS remaining_criteria_needed,
            ARRAY_TO_STRING(all_eligible_opportunities, ",") AS all_eligible_opportunities,
            DATE(path_parts[OFFSET(1)]) AS export_date,
            -- Because we export US_IX records as US_ID, use the export date to determine which state code to use
            IF(path_parts[OFFSET(2)] = "US_ID" AND DATE(path_parts[OFFSET(1)]) >= "{first_ix_export_date}", "US_IX", path_parts[OFFSET(2)]) AS state_code,
        FROM split_path
    )
    SELECT
        person_id,
        records_by_state_by_date.* EXCEPT (system_type),
    FROM records_by_state_by_date
    LEFT JOIN `{project_id}.{workflows_dataset}.person_id_to_external_id_materialized` 
        USING (state_code, system_type, person_external_id)
"""

CLIENT_RECORD_ARCHIVE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=WORKFLOWS_VIEWS_DATASET,
    view_id=CLIENT_RECORD_ARCHIVE_VIEW_NAME,
    description=CLIENT_RECORD_ARCHIVE_VIEW_DESCRIPTION,
    view_query_template=CLIENT_RECORD_ARCHIVE_QUERY_TEMPLATE,
    should_materialize=True,
    export_archives_dataset=EXPORT_ARCHIVES_DATASET,
    workflows_dataset=WORKFLOWS_VIEWS_DATASET,
    first_ix_export_date=first_ix_export_date,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CLIENT_RECORD_ARCHIVE_VIEW_BUILDER.build_and_print()
