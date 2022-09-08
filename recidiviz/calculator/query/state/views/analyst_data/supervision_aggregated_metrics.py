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
"""View tracking supervision metrics aggregated to different levels of analysis"""
from typing import List, Tuple

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.calculator.query.state.views.analyst_data.supervision_unnested_metrics import (
    SUPERVISION_METRICS_START_DATE,
    SUPERVISION_METRICS_SUPPORTED_LEVELS,
    SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS,
    SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES,
    SUPERVISION_METRICS_SUPPORTED_WINDOW_DAYS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


# function for getting string with window days appended
def add_window_to_metric(
    aggregation: str,
    metric_name: str,
    window_lengths: List[int],
) -> str:
    """
    Takes
    - `aggregation` aggregation function,
    - `metric_name` column in data
    - `window_length` list of integer window lengths (e.g. days)
    Returns a (str) column to be selected in SQL.
    Example:
        append_window_days("SUM", "metric", [180, 365]) ->
        "SUM(metric_180) AS metric_180,
         SUM(metric_365) AS metric_365"
    """

    ret_arr = []
    for window in window_lengths:
        metric_windowed = f"{metric_name}_{str(window)}"
        ret_arr += [f"{aggregation}({metric_windowed}) AS {metric_windowed}"]

    # list -> string with newlines between columns
    return ",\n".join(ret_arr)


def add_window_to_weighted_avg(
    metric_name: str,
    metric_weight_name: str,
    window_lengths: List[int],
) -> str:
    """
    Takes
    - `metric_name` column in data
    - `metric_weight_name` column in data
    - `window_length` list of integer window lengths (e.g. days)
    Returns a (str) column to be selected in SQL.
    Example:
        append_window_days("metric", "weight", [180, 365]) ->
        "SAFE_DIVIDE(SUM(metric_180 * weight_180), SUM(weight_180)) AS metric_180,
         SAFE_DIVIDE(SUM(metric_365 * weight_365), SUM(weight_365)) AS metric_365"
    """

    ret_arr = []
    for window in window_lengths:
        metric_windowed = f"{metric_name}_{str(window)}"
        weight_windowed = f"{metric_weight_name}_{str(window)}"
        a = f"SUM({metric_windowed} * {weight_windowed})"
        b = f"SUM({weight_windowed})"

        ret_arr += [f"SAFE_DIVIDE({a}, {b}) AS {metric_windowed}"]

    # list -> string with newlines between columns
    return ",\n".join(ret_arr)


# function for getting view name, description, and query template
def get_supervision_aggregated_metrics_view_strings_by_level(
    level: str,
) -> Tuple[str, str, str]:
    """
    Takes as input the level of the metrics dataframe, i.e. one from
    SUPERVISION_METRICS_SUPPORTED_LEVELS.
    Returns a list with four strings:
    1. view_id
    2. view_description
    3. query_template
    """

    if level not in SUPERVISION_METRICS_SUPPORTED_LEVELS:
        raise ValueError(f"`level` must be in {SUPERVISION_METRICS_SUPPORTED_LEVELS}")

    level_name = SUPERVISION_METRICS_SUPPORTED_LEVELS_NAMES[level]
    view_id = f"supervision_{level_name}_metrics"
    index_cols = SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS[level]
    window_days = SUPERVISION_METRICS_SUPPORTED_WINDOW_DAYS

    view_description = f"""
Tracks {level_name}-level metrics aggregated over monthly, quarterly, and yearly periods.
Note that status metrics (e.g. days_incarcerated_365) are for all clients assigned
to the {level_name} in the time period between start_date and end_date and tracked
in the year following assignment.
"""

    # level-dependent columns/metrics
    if level_name == "officer":
        primary_officer_count = """
        SELECT
            state_code,
            officer_id,
            date AS start_date,
            district,
            office,
        FROM
            `{project_id}.{analyst_dataset}.supervision_officer_primary_office_materialized`"""
        level_dependent_metrics = """-- primary officer district and office
    MIN(district) AS primary_district,
    MIN(office) AS primary_office"""

    else:
        primary_officer_count = f"""
        SELECT
            {index_cols},
            date AS start_date,
            COUNT(DISTINCT officer_id) AS primary_officers,
        FROM
            `{{project_id}}.{{analyst_dataset}}.supervision_officer_primary_office_materialized`
        GROUP BY {index_cols}, start_date"""
        level_dependent_metrics = f"""-- number of officers in each {level_name}
    MIN(primary_officers) AS primary_officers"""

    # TODO(#15115): add back officer_tenure_days and avg_caseload size

    query_template = f"""
/*{{description}}*/

-- unnested date array starting at SUPERVISION_METRICS_START_DATE 
-- through the last day of the most recent complete month
WITH date_array AS (
    SELECT
        date,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "{SUPERVISION_METRICS_START_DATE}",
            DATE_TRUNC(DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY), MONTH),
            INTERVAL 1 DAY
        )) AS date
)
, truncated_dates AS (
    SELECT DISTINCT
        "MONTH" AS period,
        DATE_TRUNC(date, MONTH) AS start_date,
        DATE_SUB(DATE_ADD(DATE_TRUNC(date, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) AS end_date,
    FROM
        date_array 

    UNION ALL

    SELECT DISTINCT
        "QUARTER" AS period,
        DATE_TRUNC(date, QUARTER) AS start_date,
        DATE_SUB(DATE_ADD(DATE_TRUNC(date, QUARTER), INTERVAL 1 QUARTER), INTERVAL 1 DAY) AS end_date,
    FROM
        date_array 

    UNION ALL

    -- we repeat the year period quarterly to allow greater flexibility downstream
    SELECT DISTINCT
        "YEAR" AS period,
        DATE_TRUNC(date, QUARTER) AS start_date,
        DATE_ADD(DATE_TRUNC(date, QUARTER), INTERVAL 364 DAY) AS end_date,
    FROM
        date_array 
)
, primary_officer_count AS (
    {primary_officer_count}
)
-- aggregated to {level_name} (across dates within period)
SELECT
    -- index
    {index_cols},
    period,
    start_date,
    end_date,

    {level_dependent_metrics},

    ## average daily populations assigned to {level_name} across period
    -- these are all calculated using person_spans

    -- compartments
    AVG(daily_population) AS avg_daily_population,
    AVG(population_out_of_state) AS avg_population_out_of_state,
    AVG(population_parole) AS avg_population_parole,
    AVG(population_probation) AS avg_population_probation,
    AVG(population_community_confinement) AS avg_population_community_confinement,
    AVG(population_other_supervision_type) AS avg_population_other_supervision_type,

    -- person demographics
    AVG(population_female) AS avg_population_female,
    AVG(population_nonwhite) AS avg_population_nonwhite,

    -- case types
    AVG(population_general_case_type) AS avg_population_general_case_type,
    AVG(population_domestic_violence_case_type) AS 
        avg_population_domestic_violence_case_type,
    AVG(population_sex_offense_case_type) AS avg_population_sex_offense_case_type,
    AVG(population_drug_case_type) AS avg_population_drug_case_type,
    AVG(population_mental_health_case_type) AS avg_population_mental_health_case_type,
    AVG(population_other_case_type) AS avg_population_other_case_type,
    AVG(population_unknown_case_type) AS avg_population_unknown_case_type,

    -- assessments and risk levels
    AVG(population_no_lsir_score) AS avg_population_no_lsir_score,
    AVG(population_no_lsir_score_at_assignment) AS 
        avg_population_no_lsir_score_at_assignment,
    AVG(population_low_risk_level) AS avg_population_low_risk_level,
    AVG(population_high_risk_level) AS avg_population_high_risk_level,
    AVG(population_unknown_risk_level) AS avg_population_unknown_risk_level,

    -- employment
    AVG(population_is_employed) AS avg_population_is_employed,

    -- contacts
    AVG(population_no_completed_contact_past_1yr) AS 
        avg_population_no_completed_contact_past_1yr,


    ## average numeric attributes across period
    SAFE_DIVIDE(SUM(avg_age * daily_population), SUM(daily_population)) AS avg_age,
    SAFE_DIVIDE(SUM(avg_lsir_score * daily_population), SUM(daily_population))
        AS avg_lsir_score,
    SAFE_DIVIDE(SUM(avg_days_since_latest_lsir * daily_population),
        SUM(daily_population)) AS avg_days_since_latest_lsir,   
    -- avg LSIR at assignment for those with scores only
    SAFE_DIVIDE(SUM(avg_lsir_score_at_assignment * (
        daily_population - population_no_lsir_score_at_assignment)), 
        SUM(daily_population - population_no_lsir_score_at_assignment)
    ) AS avg_lsir_score_at_assignment,
    SAFE_DIVIDE(SUM(avg_days_since_latest_completed_contact * daily_population),
        SUM(daily_population)) AS avg_days_since_latest_completed_contact,   


    ## event-based metrics
    /*
        Here we list counts of events and average event attributes during period.
        To get rates, divide by `avg_daily_population`.
        Averages here are representative of the event that occurred.
    */

    -- compartment transitions
    SUM(successful_completions) AS successful_completions,
    SUM(incarcerations_all) AS incarcerations_all,
    SUM(incarcerations_temporary) AS incarcerations_temporary,
    SUM(absconsions_bench_warrants) AS absconsions_bench_warrants,
    SUM(early_discharge_requests) AS early_discharge_requests,
    SUM(supervision_downgrades) AS supervision_downgrades,
    SUM(supervision_upgrades) AS supervision_upgrades,
    SUM(supervision_downgrades_to_limited) AS supervision_downgrades_to_limited,

    -- violations
    SUM(violations) AS violations,
    SUM(violations_absconded) AS violations_absconded,
    SUM(violations_new_crime) AS violations_new_crime,
    SUM(violations_technical) AS violations_technical,

    -- violation responses
    SUM(violation_responses) AS violation_responses,
    SUM(violation_responses_absconded) AS violation_responses_absconded,
    SUM(violation_responses_new_crime) AS violation_responses_new_crime,
    SUM(violation_responses_technical) AS violation_responses_technical,

    -- drug screens
    SUM(drug_screens_all) AS drug_screens_all,
    SUM(drug_screens_positive) AS drug_screens_positive,

    -- assessments
    SUM(lsir_assessments_any_officer) AS lsir_assessments_any_officer,
    SUM(lsir_risk_increase_any_officer) AS lsir_risk_increase_any_officer,
    SUM(lsir_risk_decrease_any_officer) AS lsir_risk_decrease_any_officer,
    SAFE_DIVIDE(
        SUM(lsir_assessments_any_officer * avg_lsir_score_change_any_officer),
        SUM(lsir_assessments_any_officer)
    ) AS avg_lsir_score_change_any_officer,

    -- contacts
    SUM(contacts_completed) AS contacts_completed,
    SUM(contacts_attempted) AS contacts_attempted,
    SUM(contacts_home_visit) AS contacts_home_visit,
    SUM(contacts_face_to_face) AS contacts_face_to_face,

    -- employment
    SUM(employment_gained) AS employment_gained,
    SUM(employment_lost) AS employment_lost,


    ## window-based metrics
    /*
        Each of these metrics is calculated for a 'cohort' of clients assigned
        to {level_name} during the period (between start and end dates).

        It is important to note the two relevant time parameters for these metrics:
        1. period start and end dates: the time window during which clients assigned
         to {level_name} are included in the metric calculation
        2. window length: the number of days appended to each metric name is the amount
         of days following client assignment to {level_name} that events/spans can be
         included in the metric calculation.

        For example, `days_incarcerated_365` is the number of days incarcerated for 
        all clients assigned to each {level_name} between start and end dates over the 
        365 days following assignment.
    */
    -- number of clients assigned to each {level_name} - the 'cohort' size
    SUM(clients_assigned) AS clients_assigned,

    -- cumulative number of days observed since assignment up to end of window
    {add_window_to_metric("SUM", "days_since_assignment", window_days)},

    -- average attributes at assignment for the cohort
    -- these depend on period length but not window
    SUM(lsir_score_present_at_assignment_cohort) AS 
        lsir_score_present_at_assignment_cohort,
    SAFE_DIVIDE(SUM(avg_lsir_score_at_assignment_cohort * 
        lsir_score_present_at_assignment_cohort),
        SUM(lsir_score_present_at_assignment_cohort)
    ) AS avg_lsir_score_at_assignment_cohort,

    -- days until 'event' since assignment
    -- divide these by `days_since_assignment_X` to get percent of window until event
    -- divide these by `clients_assigned` to get average days until event per person
    {add_window_to_metric("SUM", "days_to_first_incarceration", window_days)},
    {add_window_to_metric("SUM", "days_to_first_absconsion_bench_warrant", window_days)},
    {add_window_to_metric("SUM", "days_to_first_absconsion_violation", window_days)},
    {add_window_to_metric("SUM", "days_to_first_new_crime_violation", window_days)},
    {add_window_to_metric("SUM", "days_to_first_technical_violation", window_days)},
    {add_window_to_metric("SUM", "days_to_first_absconsion_violation_response", window_days)},
    {add_window_to_metric("SUM", "days_to_first_new_crime_violation_response", window_days)},
    {add_window_to_metric("SUM", "days_to_first_technical_violation_response", window_days)},

    -- days in span since assignment
    -- divide these by `days_since_assignment_X` to get percent time in span
    -- divide these by `clients_assigned` to get average days in span per person
    {add_window_to_metric("SUM", "days_incarcerated", window_days)},
    {add_window_to_metric("SUM", "days_employed", window_days)},
    {add_window_to_metric("SUM", "max_days_stable_employment", window_days)},

    -- unique attribute count 
    -- divide by `clients_assigned` to get average attribute count per person
    {add_window_to_metric("SUM", "num_unique_employers", window_days)},

    -- metrics using value at start of assignment and end of window
    {add_window_to_metric("SUM", "lsir_clients_reassessed", window_days)},
    {add_window_to_weighted_avg("avg_lsir_score_change", "lsir_clients_reassessed", window_days)},

FROM
    `{{project_id}}.{{analyst_dataset}}.supervision_{level_name}_unnested_metrics_materialized` a
INNER JOIN
    truncated_dates b
ON
    a.date BETWEEN b.start_date AND b.end_date
LEFT JOIN
    primary_officer_count
USING
    ({index_cols}, start_date)
GROUP BY {index_cols}, period, start_date, end_date
"""

    return view_id, view_description, query_template


# init object to hold view builders
SUPERVISION_AGGREGATED_METRICS_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = []

# iteratively add each builder to list
for level_string in SUPERVISION_METRICS_SUPPORTED_LEVELS:
    (
        view_id_string,
        view_description_string,
        query_template_string,
    ) = get_supervision_aggregated_metrics_view_strings_by_level(level_string)

    clustering_fields = SUPERVISION_METRICS_SUPPORTED_LEVELS_INDEX_COLUMNS[
        level_string
    ].split(", ")

    SUPERVISION_AGGREGATED_METRICS_VIEW_BUILDERS.append(
        SimpleBigQueryViewBuilder(
            dataset_id=ANALYST_VIEWS_DATASET,
            view_id=view_id_string,
            view_query_template=query_template_string,
            description=view_description_string,
            analyst_dataset=ANALYST_VIEWS_DATASET,
            clustering_fields=clustering_fields,
            should_materialize=True,
        )
    )

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in SUPERVISION_AGGREGATED_METRICS_VIEW_BUILDERS:
            view_builder.build_and_print()
