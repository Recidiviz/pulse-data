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
"""
View that combines the client and resident records together
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

PERSON_RECORD_VIEW_NAME = "person_record"

PERSON_RECORD_DESCRIPTION = (
    "View that combines the client and resident records together"
)

PERSON_RECORD_QUERY_TEMPLATE = """
SELECT
    person_external_id,
    state_code,
    person_name,
    officer_id,
    supervision_level AS correctional_level,
    supervision_start_date AS start_date,
    expiration_date AS end_date,
    district AS location,
    pseudonymized_id,
    all_eligible_opportunities
FROM `{project_id}.{workflows_views_dataset}.client_record_materialized` client_record

UNION ALL

SELECT
    person_external_id,
    state_code,
    person_name,
    officer_id,
    custody_level AS correctional_level,
    admission_date AS start_date,
    release_date AS end_date,
    facility_id AS location,
    pseudonymized_id,
    all_eligible_opportunities
FROM `{project_id}.{workflows_views_dataset}.resident_record_materialized` resident_record
"""

PERSON_RECORD_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.WORKFLOWS_VIEWS_DATASET,
    view_id=PERSON_RECORD_VIEW_NAME,
    view_query_template=PERSON_RECORD_QUERY_TEMPLATE,
    description=PERSON_RECORD_DESCRIPTION,
    workflows_views_dataset=dataset_config.WORKFLOWS_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PERSON_RECORD_VIEW_BUILDER.build_and_print()
