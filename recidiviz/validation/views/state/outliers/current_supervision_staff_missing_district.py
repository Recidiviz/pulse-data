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
View that returns a list of ingested StateStaff individuals that have an open role period with subtype 
SUPERVISION_OFFICER or SUPERVISION_OFFICER_SUPERVISOR but don't have an open location period
with valued district information
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "current_supervision_staff_missing_district"

_VIEW_DESCRIPTION = (
    "View that returns a list of ingested StateStaff individuals that have an open role period with subtype  "
    "SUPERVISION_OFFICER, SUPERVISION_OFFICER_SUPERVISOR, or SUPERVISION_DISTRICT_MANAGER but don't have an "
    "associated open location period with valued district information. "
    "Missing district ids for supervisor and district managers is currently launch blocking for the Outliers email tool (still TBD whether it'll be launch blocking for the web tool). "
    "Missing district ids or name is not currently launch blocking officers for the Outliers email tool or web tool, but we still expect that it would be mostly non-missing since we only want to infer districts or display districts as unknown when strictly necessary. "
)

_QUERY_TEMPLATE = f"""
    WITH 
    current_supervision_staff AS (
    SELECT 
        state_code,
        staff_id,
        role_subtype
    FROM `{{project_id}}.normalized_state.state_staff_role_period`
    WHERE 
        role_type = 'SUPERVISION_OFFICER' AND 
        role_subtype IN ('SUPERVISION_OFFICER', 'SUPERVISION_OFFICER_SUPERVISOR', 'SUPERVISION_DISTRICT_MANAGER') AND
        {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    ),
    current_locations AS (
    SELECT
        period.state_code,
        period.staff_id,
        period.location_external_id,
        JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_district_id') AS supervision_district_id,
        JSON_EXTRACT_SCALAR(location_metadata, '$.supervision_district_name') AS supervision_district_name
    FROM `{{project_id}}.normalized_state.state_staff_location_period` period
    LEFT JOIN `{{project_id}}.reference_views.location_metadata_materialized` metadata ON period.location_external_id = metadata.location_external_id
    WHERE 
        {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    )

    SELECT DISTINCT
        current_supervision_staff.state_code AS region_code,
        current_supervision_staff.staff_id,
        current_supervision_staff.role_subtype,
        loc.location_external_id,
        loc.supervision_district_id,
        loc.supervision_district_name
    FROM current_supervision_staff
    LEFT JOIN current_locations loc USING(staff_id, state_code)
    WHERE 
        supervision_district_id IS NULL OR
        supervision_district_name IS NULL
"""

CURRENT_SUPERVISION_STAFF_MISSING_DISTRICT_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_SUPERVISION_STAFF_MISSING_DISTRICT_VIEW_BUILDER.build_and_print()
