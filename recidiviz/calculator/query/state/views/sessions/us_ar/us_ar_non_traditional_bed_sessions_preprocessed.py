# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Non-traditional bed spans in Arkansas. Includes 309, Work Release, County Jail Backup, 
Reentry Centers, and Community Corrections Centers. This view adds flags to AR location sessions
for each of these categories, without aggregating across location sessions."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import list_to_query_string
from recidiviz.calculator.query.state.dataset_config import SESSIONS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_VIEW_NAME = (
    "us_ar_non_traditional_bed_sessions_preprocessed"
)

US_AR_REENTRY_CENTERS = [
    "MALVERN COVENANT RECOVERY RE-ENTRY FACILITY",
    "HIDDEN CREEK",
    "WINGS TO RECOVER",
    "SAFE HAVEN RE-ENTRY",
    "IN HIS WINGS RE-ENTRY FACILITY",
    "PROSPERITY HANDS",
]

US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_QUERY_TEMPLATE = f"""
WITH location_subtypes AS (
    SELECT
        person_id,
        sess.state_code,
        start_date,
        end_date_exclusive,
        location_name,
        location_type,
        COALESCE(JSON_EXTRACT_SCALAR(location_metadata, '$.location_subtype'), 'UNKNOWN') AS location_subtype,
    FROM `{{project_id}}.sessions.location_sessions_materialized` sess
    LEFT JOIN `{{project_id}}.reference_views.location_metadata_materialized` meta
    ON
        sess.state_code = meta.state_code
        AND sess.location = meta.location_external_id
    WHERE sess.state_code = 'US_AR'
)
,
non_traditional_bed_sessions AS (
    SELECT
        person_id,
        state_code,
        start_date,
        end_date_exclusive,
        location_name,
        location_type,
        -- "Act 309" is a program where people sentenced to state prison are held in county or city jails. 
        -- They are assigned to jobs in or outside the jail.
        location_subtype IN ('B6', 'B7', 'BA', 'BB') AS in_309,
        location_subtype IN ('B2') AS in_work_release,
        -- D5 includes both community corrections centers and reentry centers. These are parsed
        -- in the next CTE.
        location_subtype IN ('D5') AS is_d5,
        location_subtype IN ("B8", "BC") AS in_county_jail_backup,
        location_subtype,
    FROM location_subtypes
)
SELECT
    person_id,
    state_code,
    start_date,
    end_date_exclusive,
    location_name,
    location_type,
    location_subtype,
    in_county_jail_backup,
    in_309,
    in_work_release,
    IF(is_d5 AND location_name IN ({list_to_query_string(US_AR_REENTRY_CENTERS, quoted=True)}), TRUE, FALSE) AS in_reentry_center,
    IF(is_d5 AND location_name NOT IN ({list_to_query_string(US_AR_REENTRY_CENTERS, quoted=True)}), TRUE, FALSE) AS in_community_corrections_center,
FROM non_traditional_bed_sessions
"""

US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_VIEW_NAME,
    view_query_template=US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_QUERY_TEMPLATE,
    description=__doc__,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_AR_NON_TRADITIONAL_BED_SESSIONS_PREPROCESSED_VIEW_BUILDER.build_and_print()
