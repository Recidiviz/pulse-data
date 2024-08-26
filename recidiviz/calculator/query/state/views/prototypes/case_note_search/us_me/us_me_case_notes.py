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
"""View of all US_ME case notes"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import CASE_NOTES_PROTOTYPE_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_ME_CASE_NOTES_VIEW_NAME = "us_me_case_notes"

US_ME_CASE_NOTES_VIEW_DESCRIPTION = """All US_ME case notes"""

US_ME_CASE_NOTES_QUERY_TEMPLATE = """
    SELECT
        'US_ME' as state_code,
        Cis_100_Client_Id as external_id,
        Note_Id as note_id,
        Note_Tx as note_body,
        Short_Note_Tx as note_title,
        Note_Date as note_date,
        E_Note_Type_Desc as note_type,
        E_Mode_Desc as note_mode
    FROM `{project_id}.us_me_raw_data_up_to_date_views.CIS_204_GEN_NOTE_latest` n
    INNER JOIN `{project_id}.us_me_raw_data_up_to_date_views.CIS_2041_NOTE_TYPE_latest` ncd
        ON ncd.Note_Type_Cd = n.Cis_2041_Note_Type_Cd
    INNER JOIN `{project_id}.us_me_raw_data_up_to_date_views.CIS_2040_CONTACT_MODE_latest` cncd
        ON n.Cis_2040_Contact_Mode_Cd = cncd.Contact_Mode_Cd
"""

US_ME_CASE_NOTES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=CASE_NOTES_PROTOTYPE_DATASET,
    view_id=US_ME_CASE_NOTES_VIEW_NAME,
    view_query_template=US_ME_CASE_NOTES_QUERY_TEMPLATE,
    description=US_ME_CASE_NOTES_VIEW_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_ME_CASE_NOTES_VIEW_BUILDER.build_and_print()
