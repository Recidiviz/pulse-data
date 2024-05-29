# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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

"""A view that validates that person_caseload_location_sessions matches person_record.

The current day snapshot of person_caseload_location_sessions should align with the location and caseload information in person_record."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_NAME = "person_caseload_location_sessions"

PERSON_CASELOAD_LOCATION_SESSIONS_DESCRIPTION = """
    Validates that every row in person_record has a corresponding record in person_caseload_location_sessions with
    matching caseload.
"""

PERSON_CASELOAD_LOCATION_SESSIONS_QUERY_TEMPLATE = """
    SELECT
        pcls.state_code as region_code,
        spei.person_id,
        pcls.compartment_level_1,
        pcls.caseload_id,
        pr.officer_id,
        pcls.location_id,
        pr.location
    FROM `{project_id}.sessions.person_caseload_location_sessions_materialized` pcls
    INNER JOIN `{project_id}.normalized_state.state_person_external_id` spei
        ON spei.person_id = pcls.person_id
        AND spei.state_code = pcls.state_code
    RIGHT JOIN `{project_id}.workflows_views.person_record_materialized` pr
        ON spei.external_id = pr.person_external_id
        AND spei.state_code = pr.state_code
    WHERE
        pcls.end_date_exclusive IS NULL;
"""


PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_NAME,
    view_query_template=PERSON_CASELOAD_LOCATION_SESSIONS_QUERY_TEMPLATE,
    description=PERSON_CASELOAD_LOCATION_SESSIONS_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_CASELOAD_LOCATION_SESSIONS_VIEW_BUILDER.build_and_print()
