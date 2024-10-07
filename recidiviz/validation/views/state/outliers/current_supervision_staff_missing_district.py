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
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.    See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.    If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
View that returns a list of supervision staff that are relevant for outliers 
(officers with current metrics, current supervisors of those officers, and district managers)
that either are missing district information or have district information that doesn't
map to anything in location metadata
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    OUTLIERS_VIEWS_DATASET,
    REFERENCE_VIEWS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "current_supervision_staff_missing_district"

_VIEW_DESCRIPTION = (
    "View that returns a list of supervision staff that are relevant for outliers  "
    "(officers with current metrics, current supervisors of those officers, and district managers) "
    "that either are missing district information or have district information that doesn't "
    "map to anything in location metadata. "
    "\n "
    "For the email tool, district information is strictly required for supervisors and district managers for the CC functionality to work. "
    "For the web tool, district information is not strictly required since staff with missing districts will be displayed in the 'Unknown' district category. "
    "However, we ideally want as few people to show up under this 'Unknown' category as possible. "
    "\n "
    "Missing supervision_district_of_staff for an officer or supervisor indicates that the staff person is missing "
    "an open StateStaffLocationPeriod with valued supervision district information "
    "and we are unable to infer district information from client location in dataflow sessions (ultimately from StateSupervisionPeriod). "
    "Missing supervision_district_of_staff for a district manager indicates district is missing in the state-specific leadership reference file that the district managers product view pulls from for that district manager. "
    "Valued supervision_district_of_staff but NULL supervision_district_in_location_metadata indicates that the district we have for the staff person is missing from the location metadata for that state. "
)

_QUERY_TEMPLATE = """
    WITH officers_with_metrics AS (
        SELECT DISTINCT
            o.state_code,
            o.external_id,
            o.supervisor_external_id,
            "SUPERVISION_OFFICER" AS role_subtype,
            supervision_district
        FROM `{project_id}.{outliers_dataset}.supervision_officers_materialized` o
        LEFT JOIN `{project_id}.{outliers_dataset}.supervision_officer_outlier_status_materialized` m
            ON o.state_code = m.state_code AND o.external_id = m.officer_id
        WHERE
            m.period = 'YEAR'
            AND m.end_date = DATE_TRUNC(CURRENT_DATE("US/Eastern"), MONTH)
    ), 
    supervisors_for_officers_with_metrics AS (
        SELECT DISTINCT
            s.state_code,
            s.external_id,
            "SUPERVISION_OFFICER_SUPERVISOR" AS role_subtype,
            s.supervision_district
        FROM `{project_id}.{outliers_dataset}.supervision_officer_supervisors_materialized` s
        INNER JOIN officers_with_metrics
            ON officers_with_metrics.state_code = s.state_code AND officers_with_metrics.supervisor_external_id = s.external_id
    ),
    district_managers AS (
        SELECT DISTINCT
            s.state_code,
            s.external_id,
            "SUPERVISION_DISTRICT_MANAGER" AS role_subtype,
            s.supervision_district
        FROM `{project_id}.{outliers_dataset}.supervision_district_managers_materialized` s
    ),
    relevant_supervision_staff AS (
        SELECT 
            * EXCEPT (supervisor_external_id)
        FROM officers_with_metrics

        UNION ALL 

        SELECT 
            *
        FROM supervisors_for_officers_with_metrics

        UNION ALL

        SELECT 
            *
        FROM district_managers
    ),
    locations AS (
        SELECT 
            state_code,
            TRIM(JSON_VALUE(location_metadata, '$.supervision_district_name')) AS supervision_district_name,
            TRIM(JSON_VALUE(location_metadata, '$.supervision_district_id')) AS supervision_district_id
        FROM `{project_id}.{reference_views_dataset}.location_metadata_materialized` l
    )

    SELECT
        relevant_supervision_staff.state_code,
        relevant_supervision_staff.state_code AS region_code,
        relevant_supervision_staff.external_id as external_id,
        relevant_supervision_staff.role_subtype,
        relevant_supervision_staff.supervision_district as supervision_district_of_staff,
        l.supervision_district_name as supervision_district_in_location_metadata
    FROM relevant_supervision_staff
    LEFT JOIN locations l
        ON relevant_supervision_staff.state_code = l.state_code 
        AND UPPER(relevant_supervision_staff.supervision_district) = UPPER(l.supervision_district_name)
    WHERE 
        relevant_supervision_staff.supervision_district IS NULL 
        OR l.supervision_district_id IS NULL
"""

CURRENT_SUPERVISION_STAFF_MISSING_DISTRICT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
    outliers_dataset=OUTLIERS_VIEWS_DATASET,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_SUPERVISION_STAFF_MISSING_DISTRICT_VIEW_BUILDER.build_and_print()
