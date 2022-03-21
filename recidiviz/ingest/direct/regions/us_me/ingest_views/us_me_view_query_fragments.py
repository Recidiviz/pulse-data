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
"""Shared helper fragments for the US_ME ingest view queries."""

# TODO(#10573): Investigate using sentencing data to determine revocation admission reasons
# A quick analysis showed that a 7 day look back period captured most of the instances when a person transitioned from
# supervision to incarceration because of a revocation.
NUM_DAYS_STATUS_LOOK_BACK = 7

REGEX_TIMESTAMP_NANOS_FORMAT = r"\.\d+"

# TODO(#10111): Reconsider filtering test clients in ingest view
VIEW_CLIENT_FILTER_CONDITION = """(
        -- Filters out clients that are test or duplicate accounts
        NOT REGEXP_CONTAINS(First_Name, r'^\\^|(?i)(duplicate)')
        AND Last_Name NOT IN ('^', '^^')
        AND NOT (
            Middle_Name IS NOT NULL 
            AND REGEXP_CONTAINS(Middle_Name, r'(?i)(testing|duplicate)')
        )
        AND First_Name NOT IN (
            'NOT A CLIENT'
        )
    )"""


VIEW_SENTENCE_ADDITIONAL_TABLES = r"""
WITH charges AS (
    SELECT
        charge.Charge_Id,
        charge.Cis_100_Client_Id as Client_Id,
        charge.Offense_Date,
        charge.Referral_Date,
        charge.Cis_4020_Offense_Class_Cd as Offense_Class_Cd,
        charge.Cis_9003_Jurisd_County_Cd as Jurisdiction_County_Cd,
        charge.Comments_Tx,
        charge.Juvenile_Ind,
        charge_status.E_Charge_Outcome_Desc,
        offense_type.E_Offence_Type_Desc,
        offense_type.Mejis_Offns_Class_Tx,
        offense_type.Mejis_Offns_Title_Tx,
        offense_type.Mejis_Offns_Section_Tx,
    FROM {CIS_400_CHARGE} charge
    LEFT JOIN {CIS_4000_CHARGE_STATUS} charge_status on charge.CIS_4000_Charge_Outcome_Cd = charge_status.Charge_Outcome_Cd
    LEFT JOIN {CIS_4003_OFFENCE_TYPE} offense_type on charge.Cis_4003_Offence_Type_Cd = offense_type.Offence_Type_Cd
),
terms as (
    SELECT
        Term_Id,
        Cis_100_Client_Id as Client_Id,
        term_status.E_Term_Status_Desc,
        Intake_Date,
        Early_Cust_Rel_Date,
        Max_Cust_Rel_Date,
        Comm_Rel_Date,
    FROM {CIS_319_TERM} term
    LEFT JOIN {CIS_1200_TERM_STATUS} term_status on term.Cis_1200_Term_Status_Cd = term_status.Term_Status_Cd
),
judges as (
    SELECT
        professional.Professional_Id,
        professional.First_Name,
        professional.Last_Name,
        professional_type.E_Professional_Type_Desc,
    FROM {CIS_9904_PROFESSIONAL} professional
    JOIN {CIS_9903_PROFESSIONAL_TYPE} professional_type on professional.Cis_9903_Professional_Type_Cd = professional_type.Professional_Type_Cd
),
conditions as (
    SELECT
        condition_court_order.Cis_401_Court_Order_Id as Court_Order_Id,
        STRING_AGG(condition_type.E_Condition_Type_Desc, '\n' ORDER BY E_Condition_Type_Desc asc) as conditions,
    FROM {CIS_403_CONDITION} condition
    LEFT JOIN {CIS_409_CRT_ORDER_CNDTION_HDR} condition_court_order on condition.Cis_408_Condition_Hdr_Id = condition_court_order.Cis_408_Condition_Hdr_Id
    LEFT JOIN {CIS_4030_CONDITION_TYPE} condition_type on condition.Cis_4030_Condition_Type_Cd = condition_type.Condition_Type_Cd
    WHERE condition_court_order.Cis_401_Court_Order_Id is not null
    GROUP BY Court_Order_Id
),
"""


VIEW_SENTENCE_COLUMN_SELECTIONS = """
SELECT
    sentence.*,
    condition.conditions,
    charge.Offense_Date,
    charge.Referral_Date,
    charge.Offense_Class_Cd,
    charge.Jurisdiction_County_Cd,
    charge.Comments_Tx,
    charge.E_Charge_Outcome_Desc,
    charge.E_Offence_Type_Desc,
    charge.Mejis_Offns_Class_Tx,
    charge.Mejis_Offns_Title_Tx,
    charge.Mejis_Offns_Section_Tx,
    judge.First_Name,
    judge.Last_Name,
    judge.E_Professional_Type_Desc,
    term.E_Term_Status_Desc,
    term.Intake_Date as Term_Intake_Date,
    term.Early_Cust_Rel_Date as Term_Early_Cust_Rel_Date,
    term.Max_Cust_Rel_Date as Term_Max_Cust_Rel_Date,
    term.Comm_Rel_Date as Term_Comm_Rel_Date,
FROM sentences sentence
LEFT JOIN charges charge on sentence.Charge_Id = charge.Charge_Id
LEFT JOIN terms term on sentence.Term_Id = term.Term_Id
LEFT JOIN conditions condition on sentence.Court_Order_Id = condition.Court_Order_Id
LEFT JOIN judges judge on sentence.Judge_Professional_Id = judge.Professional_Id
WHERE charge.Juvenile_Ind != 'Y'
"""

CURRENT_STATUS_ORDER_BY = """
    CASE current_status
        WHEN 'County Jail' THEN 10
        WHEN 'Incarcerated' THEN 20
        WHEN 'Interstate Compact Out' THEN 30
        WHEN 'Interstate Compact In' THEN 40
        WHEN 'Interstate Active Detainer' THEN 50
        WHEN 'Probation' THEN 60
        WHEN 'SCCP' THEN 70
        WHEN 'Escape' THEN 80
        WHEN 'Parole' THEN 90
        WHEN 'Partial Revocation - County Jail' THEN 100
        WHEN 'Partial Revocation - incarcerated' THEN 110
        WHEN 'Pending Violation' THEN 120
        WHEN 'Pending Violation - Incarcerated' THEN 130
        WHEN 'Warrant Absconded' THEN 140
        WHEN 'Inactive' THEN 150
        WHEN 'Referral' THEN 160
        WHEN 'Active' THEN 170
        ELSE 200
    END
"""

MOVEMENT_TYPE_ORDER_BY = """
    CASE 
        WHEN movement_type = 'Sentence/Disposition' THEN 10
        WHEN movement_type = 'Detention' THEN 20
        WHEN movement_type = 'Escape' THEN 30
        WHEN movement_type = 'Furlough' THEN 40
        WHEN movement_type = 'Furlough Hospital' THEN 50
        WHEN movement_type = 'Transfer' THEN 60
        WHEN movement_type = 'Release' THEN 70
        WHEN movement_type = 'Discharge' THEN 80
        ELSE 100
    END
"""
