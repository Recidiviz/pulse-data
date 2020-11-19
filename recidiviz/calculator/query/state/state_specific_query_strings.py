# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Functions that return state-specific logic used in BigQuery queries."""


def state_supervision_specific_district_groupings(district_column: str, judicial_district_column: str) -> str:
    return f"""IFNULL(CASE WHEN supervision_type = 'PROBATION' THEN
        {state_specific_judicial_district_groupings(judicial_district_column)}
        ELSE {district_column} END, 'EXTERNAL_UNKNOWN')"""


def state_specific_judicial_district_groupings(judicial_district_column: str) -> str:
    return f"""(CASE WHEN state_code = 'US_ND'
               AND ({judicial_district_column} IS NULL
                    OR {judicial_district_column} IN ('OUT_OF_STATE', 'EXTERNAL_UNKNOWN', 'FEDERAL'))
                THEN 'OTHER'
               WHEN {judicial_district_column} IS NULL THEN 'EXTERNAL_UNKNOWN'
               ELSE {judicial_district_column} END)"""


def state_specific_race_or_ethnicity_groupings(race_or_ethnicity_column: str = 'race_or_ethnicity') -> str:
    return f"""CASE WHEN state_code = 'US_ND' AND ({race_or_ethnicity_column} IS NULL OR {race_or_ethnicity_column} IN
              ('EXTERNAL_UNKNOWN', 'ASIAN', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER')) THEN 'OTHER'
              ELSE {race_or_ethnicity_column} END AS race_or_ethnicity"""


def state_specific_assessment_bucket(output_column_name: str = 'assessment_score_bucket') -> str:
    return f"""-- TODO(#3135): remove this aggregation once the dashboard supports LOW_MEDIUM
        CASE WHEN state_code = 'US_MO' AND assessment_score_bucket = 'LOW_MEDIUM' THEN 'LOW'
        ELSE assessment_score_bucket END AS {output_column_name}"""


def state_specific_most_severe_violation_type_subtype_grouping() -> str:
    return """CASE WHEN state_code = 'US_MO' AND most_severe_violation_type = 'TECHNICAL' THEN
                CASE WHEN most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' THEN most_severe_violation_type_subtype
                     WHEN most_severe_violation_type_subtype = 'LAW_CITATION' THEN 'MISDEMEANOR'
                     ELSE most_severe_violation_type END
                WHEN state_code = 'US_PA' THEN
                    CASE WHEN most_severe_violation_type = 'TECHNICAL' THEN most_severe_violation_type_subtype
                         ELSE most_severe_violation_type END
                WHEN most_severe_violation_type IS NULL THEN 'NO_VIOLATIONS'
                ELSE most_severe_violation_type
            END AS violation_type"""


def state_specific_officer_recommendation() -> str:
    return """CASE WHEN state_code = 'US_MO' THEN
                CASE WHEN most_severe_response_decision = 'SHOCK_INCARCERATION' THEN 'CODS'
                WHEN most_severe_response_decision = 'WARRANT_ISSUED' THEN 'CAPIAS'
                ELSE most_severe_response_decision END
           WHEN state_code = 'US_PA' THEN
                -- TODO(#3596): Remove this once we differentiate returns from true revocations 
                CASE WHEN most_severe_response_decision = 'REVOCATION' THEN 'PLACEMENT_IN_DOC_FACILITY'
                ELSE most_severe_response_decision END
           ELSE most_severe_response_decision
      END AS officer_recommendation"""


def state_specific_violation_count_type_grouping() -> str:
    return """CASE WHEN state_code = 'US_MO' AND violation_count_type = 'LAW_CITATION' THEN 'MISDEMEANOR'
              ELSE violation_count_type
        END as violation_count_type"""


def state_specific_violation_count_type_categories() -> str:
    return """-- US_MO categories --
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'ASC', count, 0)) AS association_count,
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'DIR', count, 0)) AS directive_count,
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'EMP', count, 0)) AS employment_count,
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'INT', count, 0)) AS intervention_fee_count,
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'RES', count, 0)) AS residency_count,
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'SPC', count, 0)) AS special_count,
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'SUP', count, 0)) AS supervision_strategy_count,
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'TRA', count, 0)) AS travel_count,
        SUM(IF(state_code = 'US_MO' AND violation_count_type = 'WEA', count, 0)) AS weapon_count,
        -- US_PA categories --
        SUM(IF(state_code = 'US_PA' AND violation_count_type = 'ELEC_MONITORING', count, 0)) as elec_monitoring_count,
        SUM(IF(state_code = 'US_PA' AND violation_count_type = 'LOW_TECH', count, 0)) as low_tech_count,
        SUM(IF(state_code = 'US_PA' AND violation_count_type = 'MED_TECH', count, 0)) as med_tech_count,
        SUM(IF(state_code = 'US_PA' AND violation_count_type = 'HIGH_TECH', count, 0)) as high_tech_count"""


def state_specific_supervision_level() -> str:
    return """IFNULL(
                (CASE WHEN state_code = 'US_PA' THEN
                  CASE WHEN supervision_level IN ('LIMITED', 'INTERNAL_UNKNOWN') THEN 'SPECIAL'
                       WHEN supervision_level = 'HIGH' THEN 'ENHANCED'
                       ELSE supervision_level END
                ELSE supervision_level END),
           'EXTERNAL_UNKNOWN') AS supervision_level"""


def state_specific_facility_exclusion() -> str:
    return """-- Revisit these exclusions when #3657 and #3723 are complete --
      AND (state_code != 'US_ND' OR facility not in ('OOS', 'CPP'))"""
