# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""BQ View containing US_ID case updates from agnt_case_update"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ID_CASE_UPDATE_INFO_VIEW_NAME = "us_id_case_update_info"

US_ID_CASE_UPDATE_INFO_DESCRIPTION = """Provides agent case update notes for people on supervision in US_ID
    """

US_ID_CASE_UPDATE_INFO_QUERY_TEMPLATE = """
    /*{description}*/
    WITH person_id_with_external_ids AS (
        SELECT
            person_id,
            external_id AS person_external_id
        FROM
            `{project_id}.{base_dataset}.state_person_external_id`
        WHERE
            state_code = 'US_ID' AND id_type = 'US_ID_DOC'),
    agnt_case_updt AS (
        SELECT
            agnt_case_updt_id,
            ofndr_num,
            DATE(SAFE_CAST(create_dt AS DATETIME)) AS create_dt,
            create_by_usr_id,
            agnt_note_title
        FROM
            `{project_id}.us_id_raw_data_up_to_date_views.agnt_case_updt_latest`)
    SELECT
        * EXCEPT (ofndr_num)
    FROM
        person_id_with_external_ids
    JOIN
        agnt_case_updt
    ON person_id_with_external_ids.person_external_id = agnt_case_updt.ofndr_num
"""

US_ID_CASE_UPDATE_INFO_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=US_ID_CASE_UPDATE_INFO_VIEW_NAME,
    view_query_template=US_ID_CASE_UPDATE_INFO_QUERY_TEMPLATE,
    description=US_ID_CASE_UPDATE_INFO_DESCRIPTION,
    base_dataset=dataset_config.STATE_BASE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ID_CASE_UPDATE_INFO_VIEW_BUILDER.build_and_print()
