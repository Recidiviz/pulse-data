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
    return """UNNEST(SPLIT(IFNULL(ARRAY_TO_STRING(
                    (SELECT ARRAY_AGG(col) FROM UNNEST([race, ethnicity]) AS col 
                     WHERE col IS NOT NULL AND col != 'NOT_HISPANIC'),
                    ','), 'EXTERNAL_UNKNOWN'))) race_or_ethnicity"""


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
