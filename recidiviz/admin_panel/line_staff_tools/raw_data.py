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
"""Stores information for admin panel raw data uploads"""
from typing import Dict

import attr


@attr.s(frozen=True)
class RawDataConfig:
    table_name: str = attr.ib()
    schema: Dict[str, Dict[str, str]] = attr.ib()


# Schema definitions go here.
# They contain a mapping from column name in the excel file to BigQuery Schema definition.
# For tables that exist, the schema definition can be downloaded with (for example):
#   bq show --schema --format=prettyjson recidiviz-staging:static_reference_tables.us_tn_standards_admin | pbcopy
# To create this dict, copy/paste the schema definition, separately copy/paste the column names in the excel file
# and do a find/replace to convert \t to \n. Then, use your editor's version of column select to paste the column
# names as the keys to the dict. In VSCode this is done with Shift+Alt. You may need to add newlines in the middle
# # for rows that line-wrap.

US_TN_STANDARDS_DUE_SCHEMA = {
    "Region": {"mode": "NULLABLE", "name": "Region", "type": "STRING"},
    "Site": {"mode": "NULLABLE", "name": "Site", "type": "STRING"},
    "Staff ID": {"mode": "NULLABLE", "name": "Staff_ID", "type": "STRING"},
    "Staff First Name": {
        "mode": "NULLABLE",
        "name": "Staff_First_Name",
        "type": "STRING",
    },
    "Staff Last Name": {
        "mode": "NULLABLE",
        "name": "Staff_Last_Name",
        "type": "STRING",
    },
    "Offender ID": {
        "mode": "NULLABLE",
        "name": "Offender_ID",
        "type": "INTEGER",
    },
    "First Name": {
        "mode": "NULLABLE",
        "name": "First_Name",
        "type": "STRING",
    },
    "Last Name": {
        "mode": "NULLABLE",
        "name": "Last_Name",
        "type": "STRING",
    },
    "Address Line1": {
        "mode": "NULLABLE",
        "name": "Address_Line1",
        "type": "STRING",
    },
    "Address Line2": {
        "mode": "NULLABLE",
        "name": "Address_Line2",
        "type": "STRING",
    },
    "Address City": {
        "mode": "NULLABLE",
        "name": "Address_City",
        "type": "STRING",
    },
    "Address State": {
        "mode": "NULLABLE",
        "name": "Address_State",
        "type": "STRING",
    },
    "Address Zip": {
        "mode": "NULLABLE",
        "name": "Address_Zip",
        "type": "STRING",
    },
    "Phone Number": {
        "mode": "NULLABLE",
        "name": "Phone_Number",
        "type": "INTEGER",
    },
    "Age": {"mode": "NULLABLE", "name": "Age", "type": "INTEGER"},
    "Assignment Type": {
        "mode": "NULLABLE",
        "name": "Assignment_Type",
        "type": "STRING",
    },
    "Case Type": {
        "mode": "NULLABLE",
        "name": "Case_Type",
        "type": "STRING",
    },
    "Plan Start Date": {
        "mode": "NULLABLE",
        "name": "Plan_Start_Date",
        "type": "DATE",
    },
    "Supervision Level": {
        "mode": "NULLABLE",
        "name": "Supervision_Level",
        "type": "STRING",
    },
    "Prior Supervision Level": {
        "mode": "NULLABLE",
        "name": "Prior_Supervision_Level",
        "type": "STRING",
    },
    "EXP Date": {"mode": "NULLABLE", "name": "EXP_Date", "type": "DATE"},
    "180 Day Prior EXP": {
        "mode": "NULLABLE",
        "name": "_180_Day_Prior_EXP",
        "type": "DATE",
    },
    "Freq of FAC Contact-GT 1": {
        "mode": "NULLABLE",
        "name": "Freq_of_FAC_Contact_GT_1",
        "type": "INTEGER",
    },
    "FAC Note 3": {
        "mode": "NULLABLE",
        "name": "FAC_Note_3",
        "type": "DATE",
    },
    "FAC Note 2": {
        "mode": "NULLABLE",
        "name": "FAC_Note_2",
        "type": "DATE",
    },
    "FAC Note 1": {
        "mode": "NULLABLE",
        "name": "FAC_Note_1",
        "type": "DATE",
    },
    "FAC Note Due": {
        "mode": "NULLABLE",
        "name": "FAC_Note_Due",
        "type": "DATE",
    },
    "Last FAC Note": {
        "mode": "NULLABLE",
        "name": "Last_FAC_Note",
        "type": "DATE",
    },
    "Last FAC Note Type": {
        "mode": "NULLABLE",
        "name": "Last_FAC_Note_Type",
        "type": "STRING",
    },
    "Last FACV Date": {
        "mode": "NULLABLE",
        "name": "Last_FACV_Date",
        "type": "DATE",
    },
    "Last FAC1 Date": {
        "mode": "NULLABLE",
        "name": "Last_FAC1_Date",
        "type": "DATE",
    },
    "Last FAC2 Date": {
        "mode": "NULLABLE",
        "name": "Last_FAC2_Date",
        "type": "DATE",
    },
    "Last HOMF Note": {
        "mode": "NULLABLE",
        "name": "Last_HOMF_Note",
        "type": "DATE",
    },
    "Last HOMF Time": {
        "mode": "NULLABLE",
        "name": "Last_HOMF_Time",
        "type": "TIME",
    },
    "Last AHOM Note": {
        "mode": "NULLABLE",
        "name": "Last_AHOM_Note",
        "type": "DATE",
    },
    "Last HOMC Note": {
        "mode": "NULLABLE",
        "name": "Last_HOMC_Note",
        "type": "DATE",
    },
    "Last HOMV Note": {
        "mode": "NULLABLE",
        "name": "Last_HOMV_Note",
        "type": "DATE",
    },
    "Last HV Type": {
        "mode": "NULLABLE",
        "name": "Last_HV_Type",
        "type": "STRING",
    },
    "HV Note Due": {
        "mode": "NULLABLE",
        "name": "HV_Note_Due",
        "type": "DATE",
    },
    "Last RIS Note": {
        "mode": "NULLABLE",
        "name": "Last_RIS_Note",
        "type": "DATE",
    },
    "Last CCRI Note": {
        "mode": "NULLABLE",
        "name": "Last_CCRI_Note",
        "type": "DATE",
    },
    "Last CCR Date": {
        "mode": "NULLABLE",
        "name": "Last_CCR_Date",
        "type": "DATE",
    },
    "Last Strong R": {
        "mode": "NULLABLE",
        "name": "Last_Strong_R",
        "type": "DATE",
    },
    "Risk Level": {
        "mode": "NULLABLE",
        "name": "Risk_Level",
        "type": "STRING",
    },
    "Alcohol/Drug Needs": {
        "mode": "NULLABLE",
        "name": "Alcohol_Drug_Needs",
        "type": "STRING",
    },
    "Mental Health Needs": {
        "mode": "NULLABLE",
        "name": "Mental_Health_Needs",
        "type": "STRING",
    },
    "RIS Note Due": {
        "mode": "NULLABLE",
        "name": "RIS_Note_Due",
        "type": "DATE",
    },
    "DRU Note Due": {
        "mode": "NULLABLE",
        "name": "DRU_Note_Due",
        "type": "DATE",
    },
    "Last Sanctions Date": {
        "mode": "NULLABLE",
        "name": "Last_Sanctions_Date",
        "type": "DATE",
    },
    "Last Sanctions Type": {
        "mode": "NULLABLE",
        "name": "Last_Sanctions_Type",
        "type": "STRING",
    },
    "Last DRU Note": {
        "mode": "NULLABLE",
        "name": "Last_DRU_Note",
        "type": "DATE",
    },
    "Last DRU Type": {
        "mode": "NULLABLE",
        "name": "Last_DRU_Type",
        "type": "STRING",
    },
    "Last DRUL Note": {
        "mode": "NULLABLE",
        "name": "Last_DRUL_Note",
        "type": "DATE",
    },
    "Last EMP Note": {
        "mode": "NULLABLE",
        "name": "Last_EMP_Note",
        "type": "DATE",
    },
    "Last EMP Type(s)": {
        "mode": "NULLABLE",
        "name": "Last_EMP_Type_s_",
        "type": "STRING",
    },
    "EMP Note Due": {
        "mode": "NULLABLE",
        "name": "EMP_Note_Due",
        "type": "DATE",
    },
    "Last XEMP Date": {
        "mode": "NULLABLE",
        "name": "Last_XEMP_Date",
        "type": "DATE",
    },
    "Last EMPT Date": {
        "mode": "NULLABLE",
        "name": "Last_EMPT_Date",
        "type": "DATE",
    },
    "Last OCP Note": {
        "mode": "NULLABLE",
        "name": "Last_OCP_Note",
        "type": "DATE",
    },
    "OCP Note Due": {
        "mode": "NULLABLE",
        "name": "OCP_Note_Due",
        "type": "DATE",
    },
    "Last SOT Note": {
        "mode": "NULLABLE",
        "name": "Last_SOT_Note",
        "type": "DATE",
    },
    "SOT Note Due": {
        "mode": "NULLABLE",
        "name": "SOT_Note_Due",
        "type": "DATE",
    },
    "Last SOTT Date": {
        "mode": "NULLABLE",
        "name": "Last_SOTT_Date",
        "type": "DATE",
    },
    "Last RSS Note": {
        "mode": "NULLABLE",
        "name": "Last_RSS_Note",
        "type": "DATE",
    },
    "Last RSS Type": {
        "mode": "NULLABLE",
        "name": "Last_RSS_Type",
        "type": "STRING",
    },
    "RSS Note Due": {
        "mode": "NULLABLE",
        "name": "RSS_Note_Due",
        "type": "DATE",
    },
    "Last FEE Note": {
        "mode": "NULLABLE",
        "name": "Last_FEE_Note",
        "type": "DATE",
    },
    "FEE Note Due": {
        "mode": "NULLABLE",
        "name": "FEE_Note_Due",
        "type": "DATE",
    },
    "Last ARR Note": {
        "mode": "NULLABLE",
        "name": "Last_ARR_Note",
        "type": "DATE",
    },
    "Last ARR Type": {
        "mode": "NULLABLE",
        "name": "Last_ARR_Type",
        "type": "STRING",
    },
    "ARR Note Due": {
        "mode": "NULLABLE",
        "name": "ARR_Note_Due",
        "type": "DATE",
    },
    "Last SPE Note": {
        "mode": "NULLABLE",
        "name": "Last_SPE_Note",
        "type": "DATE",
    },
    "SPE Note Due": {
        "mode": "NULLABLE",
        "name": "SPE_Note_Due",
        "type": "DATE",
    },
    "Last SPET Date": {
        "mode": "NULLABLE",
        "name": "Last_SPET_Date",
        "type": "DATE",
    },
    "Last ZZZI Date": {
        "mode": "NULLABLE",
        "name": "Last_ZZZI_Date",
        "type": "DATE",
    },
    "ZZZI GT 45 days": {
        "mode": "NULLABLE",
        "name": "ZZZI_GT_45_days",
        "type": "BOOLEAN",
    },
    "date_of_standards": {
        "mode": "NULLABLE",
        "name": "date_of_standards",
        "type": "DATE",
    },
}

US_TN_STANDARDS_ADMIN_SCHEMA = {
    "Region": {"mode": "NULLABLE", "name": "Region", "type": "STRING"},
    "Site": {"mode": "NULLABLE", "name": "Site", "type": "STRING"},
    "Staff ID": {"mode": "NULLABLE", "name": "Staff_ID", "type": "STRING"},
    "Staff First Name": {
        "mode": "NULLABLE",
        "name": "Staff_First_Name",
        "type": "STRING",
    },
    "Staff Last Name": {
        "mode": "NULLABLE",
        "name": "Staff_Last_Name",
        "type": "STRING",
    },
    "Offender ID": {
        "mode": "NULLABLE",
        "name": "Offender_ID",
        "type": "INTEGER",
    },
    "First Name": {
        "mode": "NULLABLE",
        "name": "First_Name",
        "type": "STRING",
    },
    "Last Name": {
        "mode": "NULLABLE",
        "name": "Last_Name",
        "type": "STRING",
    },
    "Address Line1": {
        "mode": "NULLABLE",
        "name": "Address_Line1",
        "type": "STRING",
    },
    "Address Line2": {
        "mode": "NULLABLE",
        "name": "Address_Line2",
        "type": "STRING",
    },
    "Address City": {
        "mode": "NULLABLE",
        "name": "Address_City",
        "type": "STRING",
    },
    "Address State": {
        "mode": "NULLABLE",
        "name": "Address_State",
        "type": "STRING",
    },
    "Address Zip": {
        "mode": "NULLABLE",
        "name": "Address_Zip",
        "type": "STRING",
    },
    "Assignment Type": {
        "mode": "NULLABLE",
        "name": "Assignment_Type",
        "type": "STRING",
    },
    "Case Type": {
        "mode": "NULLABLE",
        "name": "Case_Type",
        "type": "STRING",
    },
    "Plan Start Date": {
        "mode": "NULLABLE",
        "name": "Plan_Start_Date",
        "type": "DATE",
    },
    "Supervision Level": {
        "mode": "NULLABLE",
        "name": "Supervision_Level",
        "type": "STRING",
    },
    "Prior Supervision Level": {
        "mode": "NULLABLE",
        "name": "Prior_Supervision_Level",
        "type": "STRING",
    },
    "Birth Date": {
        "mode": "NULLABLE",
        "name": "Birth_Date",
        "type": "DATE",
    },
    "Phone Number": {
        "mode": "NULLABLE",
        "name": "Phone_Number",
        "type": "INTEGER",
    },
    "Last ARR note": {
        "mode": "NULLABLE",
        "name": "Last_ARR_note",
        "type": "DATE",
    },
    "ARR Note Due": {
        "mode": "NULLABLE",
        "name": "ARR_Note_Due",
        "type": "DATE",
    },
    "Last VER note": {
        "mode": "NULLABLE",
        "name": "Last_VER_note",
        "type": "DATE",
    },
    "VER Note Due": {
        "mode": "NULLABLE",
        "name": "VER_Note_Due",
        "type": "DATE",
    },
    "Last ISC note": {
        "mode": "NULLABLE",
        "name": "Last_ISC_note",
        "type": "DATE",
    },
    "ISC Note Due": {
        "mode": "NULLABLE",
        "name": "ISC_Note_Due",
        "type": "DATE",
    },
    "Last NCIC Note": {
        "mode": "NULLABLE",
        "name": "Last_NCIC_Note",
        "type": "DATE",
    },
    "NCIC Note Due": {
        "mode": "NULLABLE",
        "name": "NCIC_Note_Due",
        "type": "DATE",
    },
    "date_of_standards": {
        "mode": "NULLABLE",
        "name": "date_of_standards",
        "type": "DATE",
    },
}
