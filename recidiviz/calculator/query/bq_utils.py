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
# pylint: disable=line-too-long


from typing import List, Optional, Set


def exclude_rows_with_missing_fields(required_columns: Set[str]) -> str:
    """Returns a WHERE clause to filter out rows that are missing values for any of the
    given columns"""
    conditions = [f"{column} IS NOT NULL" for column in required_columns]
    return f"WHERE {' AND '.join(conditions)}" if conditions else ""


def unnest_column(input_column_name: str, output_column_name: str) -> str:
    return f"UNNEST ([{input_column_name}, 'ALL']) AS {output_column_name}"


def unnest_district(district_column: str = "supervising_district_external_id") -> str:
    return unnest_column(district_column, "district")


def unnest_supervision_type(supervision_type_column: str = "supervision_type") -> str:
    return unnest_column(supervision_type_column, "supervision_type")


def unnest_charge_category(category_column: str = "case_type") -> str:
    return unnest_column(category_column, "charge_category")


def unnest_reported_violations() -> str:
    return (
        "UNNEST ([CAST(reported_violations AS STRING), 'ALL']) AS reported_violations"
    )


def unnest_metric_period_months() -> str:
    return "UNNEST ([1, 3, 6, 12, 36]) AS metric_period_months"


def unnest_rolling_average_months() -> str:
    return "UNNEST ([1, 3, 6]) AS rolling_average_months"


def unnest_rolling_window_days() -> str:
    return "UNNEST ([1, 15, 30, 90, 180]) AS rolling_window_days"


def metric_period_condition(month_offset: int = 1) -> str:
    return f"""DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH),
                                                INTERVAL metric_period_months - {month_offset} MONTH)"""


def thirty_six_month_filter() -> str:
    """Returns a query string for filtering to the last 36 months, including the current month."""
    return """DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Eastern'), MONTH), INTERVAL 35 MONTH)"""


def current_month_condition() -> str:
    return """year = EXTRACT(YEAR FROM CURRENT_DATE('US/Eastern'))
        AND month = EXTRACT(MONTH FROM CURRENT_DATE('US/Eastern'))"""


def age_bucket_grouping(
    age_column: str = "age", use_external_unknown_when_null: bool = False
) -> str:
    null_statement = (
        f"WHEN {age_column} IS NULL THEN 'EXTERNAL_UNKNOWN'"
        if use_external_unknown_when_null
        else ""
    )
    return f"""CASE WHEN {age_column} <= 24 THEN '<25'
                WHEN {age_column} <= 29 THEN '25-29'
                WHEN {age_column} <= 34 THEN '30-34'
                WHEN {age_column} <= 39 THEN '35-39'
                WHEN {age_column} >= 40 THEN '40<'
                {null_statement}
            END AS age_bucket"""


def most_severe_violation_type_subtype_grouping() -> str:
    return """CASE WHEN most_severe_violation_type = 'TECHNICAL' THEN
                CASE WHEN most_severe_violation_type_subtype = 'SUBSTANCE_ABUSE' THEN most_severe_violation_type_subtype
                     WHEN most_severe_violation_type_subtype = 'LAW_CITATION' THEN 'MISDEMEANOR'
                     ELSE most_severe_violation_type END
                WHEN most_severe_violation_type IS NULL THEN 'NO_VIOLATIONS'
                ELSE most_severe_violation_type
            END AS violation_type"""


def clean_up_supervising_officer_external_id() -> str:
    return """REPLACE(REPLACE(REPLACE(REPLACE(supervising_officer_external_id, ' - ', ' '), '-', ' '), ':', ''), ' ', '_')"""


def generate_district_id_from_district_name(district_name_field: str) -> str:
    return (
        f"""REPLACE(REGEXP_REPLACE({district_name_field}, r"[',-]", ''), ' ' , '_')"""
    )


# TODO(#12146): Exclude absconsion periods from US_ID in the
#  UsIdSupervisionCaseCompliance delegate
def hack_us_id_absconsions(dataflow_metric_table: str) -> str:
    return f"""
        WITH latest_periods AS (
          SELECT
            person_id,
            state_code
          FROM
            `{{project_id}}.{{state_base_dataset}}.state_supervision_period`
          WHERE
            termination_date IS NULL
            AND (state_code != 'US_ID' OR admission_reason != 'ABSCONSION')
        )
        SELECT *
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.{dataflow_metric_table}` metric
        INNER JOIN latest_periods lp
        USING (person_id, state_code)
        WHERE supervising_officer_external_id IS NOT NULL
            AND supervision_level IS NOT NULL
    """


def add_age_groups(age_field: str = "age") -> str:
    return f"""
            CASE 
                WHEN {age_field} < 25 THEN "<25"
                WHEN {age_field} >= 25 and {age_field} <= 29 THEN "25-29"
                WHEN {age_field} >= 30 and {age_field} <= 35 THEN "30-34"
                WHEN {age_field} >= 35 and {age_field} <= 39 THEN "35-39"
                WHEN {age_field} >= 40 and {age_field} <= 44 THEN "40-44"
                WHEN {age_field} >= 45 and {age_field} <= 49 THEN "45-49"
                WHEN {age_field} >= 50 and {age_field} <= 54 THEN "50-54"
                WHEN {age_field} >= 55 and {age_field} <= 59 THEN "55-59"
                WHEN {age_field} >= 60 THEN "60+"
                WHEN {age_field} is null THEN NULL
            end AS age_group,
    """


def filter_to_enabled_states(state_code_column: str, enabled_states: List[str]) -> str:
    return f"""WHERE {state_code_column} in ({', '.join(f"'{state}'" for state in enabled_states)})"""


def length_of_stay_month_groups(
    length_of_stay_months_expr: str = "length_of_stay_months",
) -> str:
    """Given a field that contains the length of stay in months,
    returns a CASE statement that divides it into bins up to 5 years."""

    return f"""CASE
        WHEN {length_of_stay_months_expr} < 3 THEN 'months_0_3'
        WHEN {length_of_stay_months_expr} < 6 THEN 'months_3_6'
        WHEN {length_of_stay_months_expr} < 9 THEN 'months_6_9'
        WHEN {length_of_stay_months_expr} < 12 THEN 'months_9_12'
        WHEN {length_of_stay_months_expr} < 15 THEN 'months_12_15'
        WHEN {length_of_stay_months_expr} < 18 THEN 'months_15_18'
        WHEN {length_of_stay_months_expr} < 21 THEN 'months_18_21'
        WHEN {length_of_stay_months_expr} < 24 THEN 'months_21_24'
        WHEN {length_of_stay_months_expr} < 36 THEN 'months_24_36'
        WHEN {length_of_stay_months_expr} < 48 THEN 'months_36_48'
        WHEN {length_of_stay_months_expr} <= 60 THEN 'months_48_60'
    END"""


def get_binned_time_period_months(
    date_expr: str, special_case_expr: Optional[str] = ""
) -> str:
    """Given a SQL expression that resolves to a date, assigns it to a bin representing
    various non-overlapping periods, looking back as far as the past 5 years.
    Will be NULL if the date is more than 5 years before the current date, or in the future."""

    return f"""CASE
        {special_case_expr}
        WHEN {date_expr} > CURRENT_DATE('US/Eastern') THEN NULL
        WHEN {date_expr} >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 6 MONTH) THEN "months_0_6"
        WHEN {date_expr} >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 12 MONTH) THEN "months_7_12"
        WHEN {date_expr} >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 24 MONTH) THEN "months_13_24"
        WHEN {date_expr} >= DATE_SUB(CURRENT_DATE('US/Eastern'), INTERVAL 60 MONTH) THEN "months_25_60"
    END"""


def get_person_full_name(name_expr: str) -> str:
    """Given a SQL expression that will resolve to a standard Recidiviz full_name JSON object,
    returns an expression that transforms it into a string in the format "Last, First"."""

    return f"""IF(
        {name_expr} IS NOT NULL, 
        CONCAT(
            JSON_VALUE({name_expr}, '$.surname'), 
            ", ",
            JSON_VALUE(
                {name_expr}, 
                '$.given_names'
            )
        ), 
        NULL
    )"""


def first_known_location(location_expr: str) -> str:
    """Given a sql expression that resolves to a string of packed location of the form
    location_1|location_2, will return location_1 unless it contains the string "UNKNOWN",
    in which case it will return location_2."""

    return f"""IF(CONTAINS_SUBSTR(SPLIT({location_expr},"|")[OFFSET(0)], "UNKNOWN"),
        SPLIT({location_expr},"|")[OFFSET(1)],
        SPLIT({location_expr},"|")[OFFSET(0)])"""


def create_buckets_with_cap(bucket_expr: str, max_value: int) -> str:
    """Given a positive integer value and an expression that evaluates to a positive integer,
    returns a case statement that returns the expression value cast to a string if less than
    the max value and returns f'{max_value}+' otherwise."""

    return f"IF({bucket_expr} < {max_value}, CAST({bucket_expr} AS STRING), '{max_value}+')"


def convert_days_to_years(day_expr: str) -> str:
    """Given a sql expression that resolves to a number of days, returns an expression
    that transforms it into an approximate number of years, rounding up to the nearest whole number."""

    # 365.256 days in a year
    return f"CAST(CEILING({day_expr}/365.256) AS INT64)"


def deduped_supervision_sessions(where_clause: Optional[str] = "") -> str:
    return f"""
        `{{project_id}}.{{sessions_dataset}}.dataflow_sessions_materialized` s,
        UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 5 YEAR), MONTH), 
            CURRENT_DATE('US/Eastern'), INTERVAL 1 MONTH)) as date_of_supervision,
        UNNEST (session_attributes) session_attributes
        LEFT JOIN `{{project_id}}.{{dashboards_dataset}}.pathways_supervision_location_name_map` name_map
            ON s.state_code = name_map.state_code
            AND session_attributes.supervision_office = name_map.location_id
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.compartment_level_2_dedup_priority` cl2_dedup
            ON "SUPERVISION" = cl2_dedup.compartment_level_1
            AND session_attributes.compartment_level_2=cl2_dedup.compartment_level_2
        LEFT JOIN `{{project_id}}.{{sessions_dataset}}.supervision_level_dedup_priority` sl_dedup
            ON session_attributes.correctional_level=sl_dedup.correctional_level
        WHERE session_attributes.compartment_level_1 = 'SUPERVISION' 
            AND date_of_supervision BETWEEN s.start_date AND COALESCE(s.end_date, CURRENT_DATE('US/Eastern'))
            {where_clause}
        QUALIFY ROW_NUMBER() OVER(PARTITION BY person_id, state_code, date_of_supervision
            ORDER BY COALESCE(cl2_dedup.priority, 999),
            COALESCE(sl_dedup.correctional_level_priority, 999),
            NULLIF(session_attributes.supervising_officer_external_id, 'EXTERNAL_UNKNOWN') NULLS LAST,
            NULLIF(session_attributes.case_type, 'EXTERNAL_UNKNOWN') NULLS LAST,
            NULLIF(session_attributes.judicial_district_code, 'EXTERNAL_UNKNOWN') NULLS LAST
        ) = 1
    """


def non_active_supervision_levels() -> str:
    return (
        '("EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN", "IN_CUSTODY", "WARRANT", "ABSCONDED")'
    )
