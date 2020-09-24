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

"""Helper functions for building BQ views."""


def unnest_column(input_column_name, output_column_name) -> str:
    return f"UNNEST ([{input_column_name}, 'ALL']) AS {output_column_name}"


def unnest_district(district_column='supervising_district_external_id') -> str:
    return unnest_column(district_column, 'district')


def unnest_supervision_type(supervision_type_column='supervision_type') -> str:
    return unnest_column(supervision_type_column, 'supervision_type')


def unnest_charge_category(category_column='case_type') -> str:
    return unnest_column(category_column, 'charge_category')


def unnest_metric_period_months() -> str:
    return "UNNEST ([1, 3, 6, 12, 36]) AS metric_period_months"


def unnest_race_and_ethnicity() -> str:
    return """UNNEST(
    SPLIT(
      IFNULL(
        ARRAY_TO_STRING((
            SELECT ARRAY_AGG(col) 
            FROM UNNEST(ARRAY_CONCAT(COALESCE(SPLIT(race, ','), []), 
                                     COALESCE(SPLIT(ethnicity, ','), []))) AS col
            WHERE col IS NOT NULL AND col != 'NOT_HISPANIC' AND col != 'EXTERNAL_UNKNOWN'
          ),
          ','), 
        'EXTERNAL_UNKNOWN')
    )
  ) race_or_ethnicity"""


def metric_period_condition(month_offset=1) -> str:
    return f"""DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH),
                                                INTERVAL metric_period_months - {month_offset} MONTH)"""


def current_month_condition() -> str:
    return """year = EXTRACT(YEAR FROM CURRENT_DATE('US/Pacific'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Pacific'))"""


def period_to_sentence_group_joins(period_type: str, sentence_type: str) -> str:
    """Returns joins to connect supervision or incarceration periods to sentence groups through either supervision
    or incarceration sentences. {project_id} and {base_dataset} arguments are not evaluated at this stage."""
    return f"""`{{project_id}}.{{base_dataset}}.state_{period_type}_period` period
      LEFT JOIN
        `{{project_id}}.{{base_dataset}}.state_{sentence_type}_sentence_{period_type}_period_association`
      USING ({period_type}_period_id)
      LEFT JOIN
        `{{project_id}}.{{base_dataset}}.state_{sentence_type}_sentence`
      USING ({sentence_type}_sentence_id)
      LEFT JOIN
        `{{project_id}}.{{base_dataset}}.state_sentence_group`
      USING (sentence_group_id)"""


def supervision_specific_district_groupings(district_column: str, judicial_district_column: str) -> str:
    return f"""IFNULL(CASE WHEN supervision_type = 'PROBATION' THEN
        {state_specific_judicial_district_groupings(judicial_district_column)}
        ELSE {district_column} END, 'EXTERNAL_UNKNOWN')"""


# TODO(#3675): Formalize state-specific logic in queries
def state_specific_judicial_district_groupings(judicial_district_column: str) -> str:
    return f"""(CASE WHEN state_code = 'US_ND'
               AND ({judicial_district_column} IS NULL
                    OR {judicial_district_column} IN ('OUT_OF_STATE', 'EXTERNAL_UNKNOWN', 'FEDERAL'))
                THEN 'OTHER'
               WHEN {judicial_district_column} IS NULL THEN 'EXTERNAL_UNKNOWN'
               ELSE {judicial_district_column} END)"""


def most_severe_violation_type_subtype_grouping() -> str:
    return """CASE WHEN most_severe_violation_type = 'TECHNICAL' THEN
                CASE WHEN most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' THEN most_severe_violation_type_subtype
                     WHEN most_severe_violation_type_subtype = 'LAW_CITATION' THEN 'MISDEMEANOR'
                     ELSE most_severe_violation_type END
                WHEN most_severe_violation_type IS NULL THEN 'NO_VIOLATIONS'
                ELSE most_severe_violation_type
            END AS violation_type"""


# TODO(#3675): Formalize state-specific logic in queries
def state_specific_race_or_ethnicity_groupings(race_or_ethnicity_column: str = 'race_or_ethnicity') -> str:
    return f"""CASE WHEN state_code = 'US_ND' AND {race_or_ethnicity_column} IN
              ('EXTERNAL_UNKNOWN', 'ASIAN', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER') THEN 'OTHER'
              ELSE {race_or_ethnicity_column} END AS race_or_ethnicity"""


def state_specific_supervision_level() -> str:
    return """IFNULL((CASE WHEN state_code = 'US_PA' AND supervision_level IN ('LIMITED', 'INTERNAL_UNKNOWN')
          THEN 'SPECIAL'
          ELSE supervision_level
          END), 'EXTERNAL_UNKNOWN')  AS supervision_level"""
