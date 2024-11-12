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
"""Helper fragments to import data for case notes in PA"""


def case_when_special_case() -> str:
    return """CASE WHEN supervision_type_raw_text LIKE '%05%'
        OR supervision_type_raw_text LIKE '%04%'"""


def violations_helper() -> str:
    """pulls all violations within the last 12 months"""
    # note - we ended up removing this from sidebar due to concerns about carceral impact, but leaving helper here
    # in case we want to add back in
    return """
        SELECT pei.external_id,
            'Violations in the last 12 months' AS criteria,
            COALESCE(violation_type_raw_text, 'None') AS note_title,
            CASE WHEN violation_type_raw_text IS NULL THEN ''
              WHEN violation_type_raw_text = 'H06' THEN 'Failure to report upon release'
              WHEN violation_type_raw_text = 'H09' THEN 'Absconding'
              WHEN violation_type_raw_text = 'H04' THEN 'Pending criminal charges (UCV) Detained/Not detained'
              WHEN violation_type_raw_text = 'M20' THEN 'Conviction of Misdemeanor Offense'
              WHEN violation_type_raw_text = 'M13' THEN 'Conviction of a summary offense (a minor criminal, not civil offense)'
              WHEN violation_type_raw_text = 'M04' THEN 'Travel violations'
              WHEN violation_type_raw_text = 'H01' THEN 'Changing residence without permission'
              WHEN violation_type_raw_text = 'M02' THEN 'Failure to report as instructed'
              WHEN violation_type_raw_text = 'M19' THEN 'Failure to notify agent of arrest or citation within 72 hrs'
              WHEN violation_type_raw_text = 'L07' THEN 'Failure to notify agent of change in status/employment'
              WHEN violation_type_raw_text = 'M01' THEN 'Failure to notify agent of change in status/employment'
              WHEN violation_type_raw_text = 'L08' THEN 'Positive urine, drugs'
              WHEN violation_type_raw_text = 'M03' THEN 'Positive urine, drugs'
              WHEN violation_type_raw_text = 'H12' THEN 'Positive urine, drugs'
              WHEN violation_type_raw_text = 'H10' THEN 'Possession of offense weapon'
              WHEN violation_type_raw_text = 'H11' THEN 'Possession of firearm'
              WHEN violation_type_raw_text = 'H08' THEN 'Assaultive behavior'
              WHEN violation_type_raw_text = 'L06' THEN 'Failure to pay court ordered fees, restitution'
              WHEN violation_type_raw_text = 'L01' THEN 'Failure to participate in community service'
              WHEN violation_type_raw_text = 'L03' THEN 'Failure to pay supervision fees'
              WHEN violation_type_raw_text = 'L04' THEN 'Failure to pay urinalysis fees'
              WHEN violation_type_raw_text = 'L05' THEN 'Failure to support dependents'
              WHEN violation_type_raw_text = 'M05' THEN 'Possession of contraband, cell phones, etc.'
              WHEN violation_type_raw_text = 'M06' THEN 'Failure to take medications as prescribed'
              WHEN violation_type_raw_text = 'M07' THEN 'Failure to maintain employment'
              WHEN violation_type_raw_text = 'M08' THEN 'Failure to participate or maintain treatment'
              WHEN violation_type_raw_text = 'M09' THEN 'Entering prohibited establishments'
              WHEN violation_type_raw_text = 'M10' THEN 'Associating with gang members, co-defendants, etc'
              WHEN violation_type_raw_text = 'M11' THEN 'Failure to abide by written instructions'
              WHEN violation_type_raw_text = 'M12' THEN 'Failure to abide by field imposed special conditions'
              WHEN violation_type_raw_text = 'L02' THEN 'Positive urine, alcohol (Previous History)'
              WHEN violation_type_raw_text = 'M14' THEN 'Positive urine, alcohol (Previous History)'
              WHEN violation_type_raw_text = 'H03' THEN 'Positive urine, alcohol (Previous History)'
              WHEN violation_type_raw_text = 'M15' THEN 'Violating curfew'
              WHEN violation_type_raw_text = 'M16' THEN 'Violating electronic monitoring'
              WHEN violation_type_raw_text = 'M17' THEN 'Failure to provide urine'
              WHEN violation_type_raw_text = 'M18' THEN 'Failure to complete treatment'
              WHEN violation_type_raw_text = 'H02' THEN 'Associating with crime victims'
              WHEN violation_type_raw_text = 'H05' THEN 'Failure to abide by Board Imposed Special Conditions'
              WHEN violation_type_raw_text = 'H07' THEN 'Removal from Treatment/CCC Failure'
              ELSE 'Other' END AS note_body,
            violation_date AS event_date 
        FROM `{project_id}.{normalized_state_dataset}.state_supervision_violation` v
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_supervision_violation_type_entry` vt
            ON v.person_id = vt.person_id
            AND v.supervision_violation_id = vt.supervision_violation_id
            AND vt.state_code = 'US_PA'
        LEFT JOIN `{project_id}.{normalized_state_dataset}.state_person_external_id` pei
            ON v.person_id = pei.person_id
            AND pei.id_type = 'US_PA_PBPP'
        WHERE v.state_code = 'US_PA'
            AND v.violation_date IS NOT NULL
            AND v.violation_date >= DATE_ADD(CURRENT_DATE("US/Pacific"), INTERVAL -1 YEAR)
        """


def statute_is_conspiracy_or_attempt() -> str:
    return """(statute LIKE '%C0901%' -- criminal attempt
              OR statute LIKE '%C0903%' -- criminal conspiracy
              OR statute LIKE '%18.901%'
              OR statute LIKE '%18.903%'
              OR statute LIKE '0901%' 
              OR statute LIKE '0903%' 
              OR statute LIKE '18901%' 
              OR statute LIKE '18903%'               
              OR statute LIKE '%CC901%' 
              OR statute LIKE '%CC903%'    
              OR statute LIKE '%CS0901%' 
              OR statute LIKE '%CS0903%') 
              """


def description_refers_to_assault() -> str:
    return """(description LIKE '%ASLT%'
                OR description LIKE '%AS\\'LT%'
                OR description LIKE '%ALST%'
                OR description LIKE '%ASSAU%'
                OR description LIKE '%ASSLT%'
                OR description LIKE '%ASS\\'LT%'
                OR description LIKE 'ASS%')"""
