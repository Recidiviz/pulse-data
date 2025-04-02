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
"""A parole and/or probation officer/agent who serves in a manager or supervisor role and doesn't supervise a typical caseload"""
from recidiviz.big_query.selected_columns_big_query_view import (
    SelectedColumnsBigQueryViewBuilder,
)
from recidiviz.calculator.query.bq_utils import (
    get_pseudonymized_id_query_str,
    today_between_start_date_and_nullable_end_date_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.outliers.utils import (
    most_recent_staff_attrs_cte,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_SUPERVISORS_VIEW_NAME = "supervision_officer_supervisors"

SUPERVISION_OFFICER_SUPERVISORS_DESCRIPTION = """A parole and/or probation officer/agent who serves in a manager or supervisor role and doesn't supervise a typical caseload"""


SUPERVISION_OFFICER_SUPERVISORS_QUERY_TEMPLATE = f"""
WITH attrs AS (
    {most_recent_staff_attrs_cte()}    
)
, supervision_officer_supervisors AS (
    SELECT
        attrs.officer_id AS external_id,
        attrs.staff_id,
        attrs.state_code,
        staff.full_name,
        -- This pseudonymized_id will match the one for the user in the auth0 roster. Hashed
        -- attributes must be kept in sync with recidiviz.auth.helpers.generate_pseudonymized_id.
        {get_pseudonymized_id_query_str("IF(attrs.state_code = 'US_IX', 'US_ID', attrs.state_code) || attrs.officer_id")} AS pseudonymized_id,
        staff.email,
        COALESCE(attrs.supervision_district_name,attrs.supervision_district_name_inferred) AS supervision_district,
        IF(attrs.state_code = 'US_CA', attrs.supervision_office_name, attrs.supervision_unit_name) AS supervision_unit,
        CASE attrs.state_code
            WHEN "US_ND" THEN attrs.supervision_office_name
            ELSE COALESCE(attrs.supervision_district_name,attrs.supervision_district_name_inferred)
        END AS supervision_location_for_list_page,
        CASE 
            WHEN attrs.state_code IN ("US_CA", "US_ND") THEN attrs.supervision_office_name
            ELSE COALESCE(attrs.supervision_district_name,attrs.supervision_district_name_inferred)
        END AS supervision_location_for_supervisor_page,
    FROM (
        -- A supervision supervisor in the Outliers product is anyone that has an open supervisor period as a  
        -- supervisor. 
        SELECT DISTINCT state_code, supervisor_staff_external_id AS external_id
        FROM `{{project_id}}.normalized_state.state_staff_supervisor_period` sp
        WHERE {today_between_start_date_and_nullable_end_date_clause("start_date", "end_date")}
    ) supervision_staff
    INNER JOIN attrs
        ON attrs.state_code = supervision_staff.state_code AND attrs.officer_id = supervision_staff.external_id 
    INNER JOIN `{{project_id}}.normalized_state.state_staff` staff 
        ON attrs.staff_id = staff.staff_id AND attrs.state_code = staff.state_code
)
SELECT 
    {{columns}}
FROM supervision_officer_supervisors
"""

SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER = SelectedColumnsBigQueryViewBuilder(
    dataset_id=dataset_config.OUTLIERS_VIEWS_DATASET,
    view_id=SUPERVISION_OFFICER_SUPERVISORS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_SUPERVISORS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_SUPERVISORS_DESCRIPTION,
    should_materialize=True,
    columns=[
        "state_code",
        "external_id",
        "staff_id",
        "full_name",
        "pseudonymized_id",
        "supervision_district",
        "supervision_unit",
        "email",
        "supervision_location_for_list_page",
        "supervision_location_for_supervisor_page",
    ],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_SUPERVISORS_VIEW_BUILDER.build_and_print()
