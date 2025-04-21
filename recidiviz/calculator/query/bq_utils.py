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

"""Helper functions for building BQ views."""
# pylint: disable=line-too-long


from typing import Iterable, List, Optional, Sequence, Set

MAGIC_END_DATE = "9999-12-31"
MAGIC_START_DATE = "1000-01-01"


def exclude_rows_with_missing_fields(required_columns: Set[str]) -> str:
    """Returns a WHERE clause to filter out rows that are missing values for any of the
    given columns"""
    conditions = [f"{column} IS NOT NULL" for column in sorted(required_columns)]
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
def filter_out_absconsions(
    dataflow_metric_table: str, include_state_pop: bool = False
) -> str:
    include_state_pop_string = (
        "AND metric.included_in_state_population" if include_state_pop else ""
    )
    return f"""
        SELECT
            metric.*,
            staff.external_id AS supervising_officer_external_id,
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.{dataflow_metric_table}` metric
        LEFT JOIN
            `{{project_id}}.sessions.state_staff_id_to_legacy_supervising_officer_external_id_materialized` staff
        ON
            metric.supervising_officer_staff_id = staff.staff_id
        INNER JOIN
            `{{project_id}}.{{normalized_state_dataset}}.state_supervision_period` supervision_period
        ON
            metric.person_id = supervision_period.person_id
            AND metric.date_of_supervision BETWEEN supervision_period.start_date AND {nonnull_end_date_clause("supervision_period.termination_date")}
        WHERE staff.external_id IS NOT NULL
            AND metric.supervision_level IS NOT NULL
            AND IFNULL(supervision_period.admission_reason, '') != 'ABSCONSION'
            {include_state_pop_string}
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


def filter_to_states(
    state_code_column: str,
    enabled_states: List[str],
) -> str:
    return f"""WHERE {state_code_column} in ({', '.join(f"'{state}'" for state in sorted(enabled_states))})"""


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
    date_expr: str,
    *,
    special_case_expr: Optional[str] = "",
    current_date_expr: Optional[str] = "CURRENT_DATE('US/Eastern')",
) -> str:
    """Given a SQL expression that resolves to a date, assigns it to a bin representing
    various non-overlapping periods, looking back as far as the past 5 years.
    Will be NULL if the date is more than 5 years before the current date, or in the future.
    """

    return f"""CASE
        {special_case_expr}
        WHEN {date_expr} > {current_date_expr} THEN NULL
        WHEN {date_expr} >= DATE_SUB({current_date_expr}, INTERVAL 6 MONTH) THEN "months_0_6"
        WHEN {date_expr} >= DATE_SUB({current_date_expr}, INTERVAL 12 MONTH) THEN "months_7_12"
        WHEN {date_expr} >= DATE_SUB({current_date_expr}, INTERVAL 24 MONTH) THEN "months_13_24"
        WHEN {date_expr} >= DATE_SUB({current_date_expr}, INTERVAL 60 MONTH) THEN "months_25_60"
    END"""


def get_person_full_name(name_expr: str) -> str:
    """Given a SQL expression that will resolve to a standard Recidiviz full_name JSON object,
    returns an expression that transforms it into a string in the format "Last, First".
    """

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
    that transforms it into an approximate number of years, rounding up to the nearest whole number.
    """

    # 365.256 days in a year
    return f"CAST(CEILING({day_expr}/365.256) AS INT64)"


def non_active_supervision_levels() -> str:
    return (
        '("EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN", "IN_CUSTODY", "WARRANT", "ABSCONDED")'
    )


def nonnull_current_date_clause(column_name: str) -> str:
    """Convert NULL end dates to current date to help with date logic"""
    return f'COALESCE({column_name}, CURRENT_DATE("US/Eastern"))'


def nonnull_current_date_exclusive_clause(column_name: str) -> str:
    """Convert NULL exclusive end dates to the day after current date to help with date logic.
    For use in views that use exclusive end dates, where we might want to calculate the number
    of days in a span."""
    return (
        f'COALESCE({column_name}, DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))'
    )


def nonnull_end_date_clause(column_name: str) -> str:
    """Convert NULL end dates to dates far in the future to help with the date logic"""
    return f'IFNULL({column_name}, "{MAGIC_END_DATE}")'


def nonnull_end_date_exclusive_clause(column_name: str) -> str:
    """Convert NULL end dates to dates far in the future to help with the date logic. For use in
    views that use exclusive end dates, like task eligibility."""
    return f'IFNULL(DATE_SUB({column_name}, INTERVAL 1 DAY), "{MAGIC_END_DATE}")'


def today_between_start_date_and_nullable_end_date_clause(
    start_date_column: str, end_date_column: str
) -> str:
    """Filter to spans where today falls after the start date and before the end date if it is not NULL."""
    return f'CURRENT_DATE("US/Pacific") BETWEEN {start_date_column} AND {nonnull_end_date_clause(end_date_column)}'


def today_between_start_date_and_nullable_end_date_exclusive_clause(
    start_date_column: str, end_date_column: str
) -> str:
    """Filter to spans where today falls after the start date and before the exclusive end date if it is not NULL."""
    return f'CURRENT_DATE("US/Pacific") BETWEEN {start_date_column} AND {nonnull_end_date_exclusive_clause(end_date_column)}'


def revert_nonnull_end_date_clause(column_name: str) -> str:
    """Convert end dates far in the future to NULL"""
    return f'IF({column_name} = "{MAGIC_END_DATE}", NULL, {column_name})'


def nonnull_start_date_clause(column_name: str) -> str:
    """Convert NULL start dates to dates far in the future to help with the date logic"""
    return f'COALESCE({column_name}, "{MAGIC_START_DATE}")'


def revert_nonnull_start_date_clause(column_name: str) -> str:
    """Convert start dates far in the future to NULL"""
    return f'IF({column_name} = "{MAGIC_START_DATE}", NULL, {column_name})'


def array_concat_with_null(arrays_to_concat: List[str]) -> str:
    """Concatenate arrays and handle null arrays"""
    array_expr = []
    for arr in arrays_to_concat:
        array_expr.append(f"IFNULL({arr}, [])")

    return f'ARRAY_CONCAT({", ".join(array_expr)})'


def list_to_query_string(
    string_list: Sequence[str], quoted: bool = False, table_prefix: Optional[str] = None
) -> str:
    """Combines a list or tuple of strings into a comma-separated string, with the
    option to maintain quoted strings and/or add a prefix"""
    string_list_modified = string_list
    if table_prefix is not None:
        string_list_modified = [f"{table_prefix}.{string}" for string in string_list]
    if quoted:
        string_list_modified = [f'"{string}"' for string in string_list_modified]
    return ", ".join(string_list_modified)


def columns_to_array(columns: Iterable[str]) -> str:
    """Aggregate values from multiple columns into an array, ignoring nulls"""
    return f"""
        (SELECT ARRAY_AGG(x IGNORE NULLS)
            FROM UNNEST([{", ".join(columns)}]) x)
    """


def join_on_columns_fragment(columns: Iterable[str], table1: str, table2: str) -> str:
    """Returns a string fragment that can be used in an "ON" join statement for columns shared between table1 and table2"""
    join_fragments = [f"{table1}.{col} = {table2}.{col}" for col in columns]
    return "\nAND ".join(join_fragments)


def date_diff_in_full_months(
    first_date_column: str,
    second_date_column: str,
    # time_zone: str = "US/Pacific",
) -> str:
    """Returns a string fragment to calculate the number of full months between a date
            and the current date.

    Args:
        first_date_column (str): The name of the date column to calculate the difference from.
        second_date_column (str, optional): The time zone to use for the current date.
            Defaults to "US/Pacific".
    """

    return f"""  DATE_DIFF({first_date_column}, {second_date_column}, MONTH)
          - IF(EXTRACT(DAY FROM {second_date_column}) > EXTRACT(DAY FROM {first_date_column}), 
                1, 0)"""


def get_pseudonymized_id_query_str(hash_value_query_str: str) -> str:
    """
    Returns a string fragment for the pseudonymized_id in the product.
    Note: this must be kept in sync with recidiviz.auth.helpers.generate_pseudonymized_id
    """
    return f"""SUBSTRING(
        # hashing external ID to base64url
            REPLACE(
                REPLACE(
                    TO_BASE64(SHA256({hash_value_query_str})), 
                    '+', 
                    '-'
                ),
                '/', 
                '_'
            ), 
            1, 
            16
        )"""


def merge_permissions_query_str(column_name: str, table_name: str) -> str:
    """
    Returns a string fragment to merge/reconcile permissions for users with multiple roles.
    Note: this must be kept in sync with recidiviz.auth.helpers.merge_permissions
    """

    if column_name == "routes":
        split_kv_pairs_query_str = """
            TRIM(REGEXP_EXTRACT(key_value, r'^\\s*"([^"]+)"'), '"') AS key,
            TRIM(REGEXP_EXTRACT(key_value, r':\\s*("[^"]*"|\\S*)'), '"') AS value
        """

        # For routes, select by priority: 1) true, 2) false
        prioritize_permissions_query_str = """
            MAX(value) AS value
        """
    elif column_name == "feature_variants":
        split_kv_pairs_query_str = """
            TRIM(REGEXP_EXTRACT(key_value, r'^\\s*"([^"]+)"'), '"') AS key,
            TRIM(REGEXP_EXTRACT(key_value, r':\\s*(false|{{.*}})'), '"') AS value
        """

        # For feature variants, select by priority: 1) always on (value of variant will be empty object/not have an activeDate attribute), 2) earliest active date, 3) false
        prioritize_permissions_query_str = """
            ARRAY_AGG(
                IFNULL(value, '{{}}')
                ORDER BY
                    CASE
                        WHEN JSON_EXTRACT(value, '$.activeDate') IS NULL AND value != 'false' THEN 1
                        WHEN JSON_EXTRACT(value, '$.activeDate') IS NOT NULL THEN 2
                        WHEN value = 'false' THEN 3
                        ELSE 4
                    END,
                    JSON_EXTRACT(value, '$.activeDate') ASC
            )[OFFSET(0)] AS value
        """
    else:
        raise ValueError(
            f"Merging is not currently supported for column: {column_name}"
        )

    return f"""
        -- Separate each permission into key and value columns
        {column_name}_kv_pairs AS (
            SELECT
                email_address,
                {split_kv_pairs_query_str}
            FROM (
                SELECT
                    email_address,
                    key_value
                FROM
                    {table_name},
                    UNNEST({column_name}) AS col_value,
                    UNNEST(SPLIT(REGEXP_REPLACE(col_value, r'^{{{{|}}}}$', ''), ',')) AS key_value
            )
        ),
        -- Select only one value for each column based on priority
        {column_name}_prioritized_permissions AS (
            SELECT
                email_address,
                key,
                {prioritize_permissions_query_str}
            FROM
                {column_name}_kv_pairs
            GROUP BY
                email_address, key
        ),
        -- Merge permissions into single object
        merged_{column_name} AS (
            SELECT
                email_address,
                COALESCE(
                    CONCAT(
                        '{{{{',
                        STRING_AGG(
                            CONCAT('"', key, '": ', value),
                            ', ' ORDER BY key
                        ),
                        '}}}}'
                    ),
                    '{{{{}}}}'
                ) AS {column_name}
            FROM
                {column_name}_prioritized_permissions
            GROUP BY
                email_address
        )
    """
