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


from typing import List, Set


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


def hack_us_id_supervising_officer_external_id(dataflow_metric_table: str) -> str:
    return f"""
        -- HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT
        --
        -- HACK ALERT HACK ALERT HACK ALERT HACK ALERT
        --
        -- HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT
        --
        -- TODO(#5943): We unfortunately have to pull straight from raw data from Idaho due to internal
        -- inconsistencies in Idaho's data. Our ingest pipeline assumed that the historical record
        -- was accurate, but unfortunately that no longer seems to be the case. The long-term solution
        -- involves fetching an updates one-off historical dump of the casemgr table, re-running ingest,
        -- and adding validation to ensure this doesn't happen, but the timescale of this is much
        -- slower than we want to move for Case Triage.
        --
        -- Hence, the decision to add this very verbose warning to encourage future readers to decide
        -- whether they should start trying to pay down this technical debt.
        --
        -- HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT
        --
        -- HACK ALERT HACK ALERT HACK ALERT HACK ALERT
        --
        -- HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT HACK ALERT
        WITH latest_periods AS (
          SELECT
            person_id,
            state_code
          FROM
            `{{project_id}}.state.state_supervision_period`
          WHERE
            termination_date IS NULL
            AND (state_code != 'US_ID' OR admission_reason != 'ABSCONSION')
        ),
        latest_ofndr_agnt AS (
          SELECT
            ofndr_num AS person_external_id,
            UPPER(agnt_id) AS agnt_id,
          FROM `{{project_id}}.us_id_raw_data_up_to_date_views.ofndr_agnt_latest`
          -- These filters limit the results to only currently assigned POs
          -- in the unlikely case where two POs are assigned to a single client,
          -- the query returns the one with the most recent start date
          WHERE end_dt IS NULL
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ofndr_num
            ORDER BY agnt_strt_dt DESC
          ) = 1
        )
        SELECT
          * EXCEPT (supervising_officer_external_id),
          IF(state_code != 'US_ID', supervising_officer_external_id, latest_ofndr_agnt.agnt_id) AS supervising_officer_external_id
        FROM `{{project_id}}.{{materialized_metrics_dataset}}.{dataflow_metric_table}` metric
        INNER JOIN latest_periods lp
        USING (person_id, state_code)
        LEFT OUTER JOIN
          latest_ofndr_agnt
        USING(person_external_id)
        WHERE IF(state_code != 'US_ID', supervising_officer_external_id, latest_ofndr_agnt.agnt_id) IS NOT NULL
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
            end as age_group,
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


def get_binned_time_period_months(date_expr: str) -> str:
    """Given a SQL expression that resolves to a date, assigns it to a bin representing
    various non-overlapping periods, looking back as far as the past 5 years.
    Will be NULL if the date is more than 5 years before the current date, or in the future."""

    return f"""CASE
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
