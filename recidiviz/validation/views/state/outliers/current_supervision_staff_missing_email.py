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
View that returns a list of supervision staff that are relevant for the outliers email tool 
(current supervisors of officers with metrics and district managers)
that are missing email information.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import OUTLIERS_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views.dataset_config import VIEWS_DATASET

_VIEW_NAME = "current_supervision_staff_missing_email"

_VIEW_DESCRIPTION = (
    "View that returns a list of supervision staff that are relevant for the outliers email tool "
    "(current supervisors of officers with metrics and district managers) "
    "that are missing email information. "
    "Emails are required for the outliers email tool for both supervisors and district managers for the email and cc functionality to work. "
    "Emails are currently not required for the outliers web tool. "
    "A missing email for a supervisor indicates that an email was not ingested as part of the StateStaff record for that supervisor. "
    "A missing email for a district manager indicates that email is missing in the state-specific leadership reference file that the district managers product view pulls from for that district manager."
)

_QUERY_TEMPLATE = """
    WITH officers_with_metrics AS (
    SELECT DISTINCT
        o.state_code,
        o.external_id,
        o.supervisor_external_id
    FROM `{project_id}.{outliers_dataset}.supervision_officers_materialized` o
    LEFT JOIN `{project_id}.{outliers_dataset}.supervision_officer_metrics_materialized` m
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
        s.email
    FROM `{project_id}.{outliers_dataset}.supervision_officer_supervisors_materialized` s
    INNER JOIN officers_with_metrics
        ON officers_with_metrics.state_code = s.state_code AND officers_with_metrics.supervisor_external_id = s.external_id
    ),
    district_managers AS (
    SELECT DISTINCT
        s.state_code,
        s.external_id,
        "DISTRICT_MANAGER" AS role_subtype,
        s.email
    FROM `{project_id}.{outliers_dataset}.supervision_district_managers_materialized` s
    ),
    relevant_supervision_staff AS (
    SELECT 
        *
    FROM supervisors_for_officers_with_metrics

    UNION ALL

    SELECT 
        *
    FROM district_managers
    )

    SELECT
        relevant_supervision_staff.state_code,
        relevant_supervision_staff.state_code AS region_code,
        relevant_supervision_staff.external_id as external_id,
        relevant_supervision_staff.role_subtype
    FROM relevant_supervision_staff
    WHERE email is NULL
"""

CURRENT_SUPERVISION_STAFF_MISSING_EMAIL_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=VIEWS_DATASET,
    view_id=_VIEW_NAME,
    view_query_template=_QUERY_TEMPLATE,
    description=_VIEW_DESCRIPTION,
    should_materialize=True,
    outliers_dataset=OUTLIERS_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CURRENT_SUPERVISION_STAFF_MISSING_EMAIL_VIEW_BUILDER.build_and_print()
