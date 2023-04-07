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
"""
View that preprocesses state staff periods to extract relevant attributes and external id's.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import (
    NORMALIZED_STATE_DATASET,
    REFERENCE_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_NAME = (
    "supervision_officer_attribute_sessions"
)

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_DESCRIPTION = """
View that preprocesses state staff periods to extract relevant attributes and external id's.
"""

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_QUERY_TEMPLATE = """
#TODO(#18022): Join in staff role periods to only extract staff with SUPERVISION_OFFICER role.
SELECT
    a.state_code,
    c.external_id AS officer_id,
    start_date,
    end_date,
    end_date AS end_date_exclusive,
    JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_district_id") AS supervision_district,
    JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_office_id") AS supervision_office,
    JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_unit_id") AS supervision_unit,
    JSON_EXTRACT_SCALAR(location_metadata, "$.supervision_unit_name") AS supervision_unit_name,
FROM
    `{project_id}.{normalized_state_dataset}.state_staff_location_period` a
LEFT JOIN
    `{project_id}.{reference_views_dataset}.location_metadata_materialized` b
USING
    (state_code, location_external_id)
LEFT JOIN
    `{project_id}.{normalized_state_dataset}.state_staff_external_id` c
USING
    (state_code, staff_id)
"""

SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=SESSIONS_DATASET,
    view_id=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_NAME,
    view_query_template=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_QUERY_TEMPLATE,
    description=SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_DESCRIPTION,
    normalized_state_dataset=NORMALIZED_STATE_DATASET,
    reference_views_dataset=REFERENCE_VIEWS_DATASET,
    clustering_fields=["state_code", "officer_id"],
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_OFFICER_ATTRIBUTE_SESSIONS_VIEW_BUILDER.build_and_print()
