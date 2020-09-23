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
    return f"""CASE WHEN state_code = 'US_ND' AND {race_or_ethnicity_column} IN
              ('EXTERNAL_UNKNOWN', 'ASIAN', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER') THEN 'OTHER'
              ELSE {race_or_ethnicity_column} END AS race_or_ethnicity"""


def state_specific_assessment_bucket() -> str:
    return """-- TODO(#3135): remove this aggregation once the dashboard supports LOW_MEDIUM
        CASE WHEN state_code = 'US_MO' AND assessment_score_bucket = 'LOW_MEDIUM' THEN 'LOW'
        ELSE assessment_score_bucket END as assessment_score_bucket"""


def state_specific_most_severe_violation_type_subtype_grouping() -> str:
    return """CASE WHEN state_code = 'US_MO' AND most_severe_violation_type = 'TECHNICAL' THEN
                CASE WHEN most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' THEN most_severe_violation_type_subtype
                     WHEN most_severe_violation_type_subtype = 'LAW_CITATION' THEN 'MISDEMEANOR'
                     ELSE most_severe_violation_type END
                WHEN most_severe_violation_type IS NULL THEN 'NO_VIOLATIONS'
                ELSE most_severe_violation_type
            END AS violation_type"""


def state_specific_officer_recommendation() -> str:
    return """CASE WHEN state_code = 'US_MO' THEN
                CASE WHEN most_severe_response_decision = 'SHOCK_INCARCERATION' THEN 'CODS'
                WHEN most_severe_response_decision = 'WARRANT_ISSUED' THEN 'CAPIAS'
                ELSE most_severe_response_decision END
           ELSE most_severe_response_decision
      END AS officer_recommendation"""
