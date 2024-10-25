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
"""View logic to prepare US_IX case notes for search"""

US_ME_CASE_NOTES_TEMPLATE = """
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
