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
Note that span metrics (e.g. days_incarcerated_365) are for all clients assigned
to the {level_name} in the time period between start_date and end_date (exclusive) and 
tracked in the `_N` days following assignment.

All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
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
    MIN(office) AS primary_office,
    DATE_DIFF(start_date, MIN(first_assignment_date), DAY) AS officer_tenure_days"""
        officer_level_metrics_join = ""

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
    MIN(primary_officers) AS primary_officers,
    AVG(avg_daily_caseload_officer) AS avg_daily_caseload_officer,
    AVG(clients_assigned_officer) AS avg_clients_assigned_officer,
    AVG(officer_tenure_days) AS avg_officer_tenure_days"""

        officer_level_metrics_join_vars = "start_date, end_date, state_code"
        if level_name in ["office", "district"]:
            level_specific_name = f"primary_{level_name}"
            officer_level_metrics_join = f"""LEFT JOIN (
    SELECT
        {officer_level_metrics_join_vars},
        {level_specific_name} AS {level_name},
        avg_daily_population AS avg_daily_caseload_officer,
        clients_assigned AS clients_assigned_officer,
        officer_tenure_days,
    FROM
        `{{project_id}}.{{analyst_dataset}}.supervision_officer_metrics`
) c
USING
    ({officer_level_metrics_join_vars}, {level_name})
        """
        else:
            officer_level_metrics_join = f"""LEFT JOIN (
    SELECT
        {officer_level_metrics_join_vars},
        avg_daily_population AS avg_daily_caseload_officer,
        clients_assigned AS clients_assigned_officer,
        officer_tenure_days,
    FROM
        `{{project_id}}.{{analyst_dataset}}.supervision_officer_metrics`
)
USING
    ({officer_level_metrics_join_vars})
        """

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

-- end_dates are exclusive
, truncated_dates AS (
    SELECT DISTINCT
        "MONTH" AS period,
        DATE_TRUNC(date, MONTH) AS start_date,
        DATE_ADD(DATE_TRUNC(date, MONTH), INTERVAL 1 MONTH) AS end_date,
    FROM
        date_array 

    UNION ALL

    SELECT DISTINCT
        "QUARTER" AS period,
        DATE_TRUNC(date, QUARTER) AS start_date,
        DATE_ADD(DATE_TRUNC(date, QUARTER), INTERVAL 1 QUARTER) AS end_date,
    FROM
        date_array 

    UNION ALL

    -- we repeat the year period quarterly to allow greater flexibility downstream
    SELECT DISTINCT
        "YEAR" AS period,
        DATE_TRUNC(date, QUARTER) AS start_date,
        DATE_ADD(DATE_TRUNC(date, QUARTER), INTERVAL 365 DAY) AS end_date,
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
    -- population metrics should only be null only where necessary fields are not 
    -- hydrated, e.g. employment periods in some states

    -- compartments
    AVG(daily_population) AS avg_daily_population,
    AVG(population_parole) AS avg_population_parole,
    AVG(population_probation) AS avg_population_probation,
    AVG(population_community_confinement) AS avg_population_community_confinement,

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

    ## summed person days in span across period
    IFNULL(SUM(person_days_supervision_level_downgrade_eligible), 0) AS 
        person_days_supervision_level_downgrade_eligible,
    IFNULL(SUM(person_days_early_discharge_from_supervision_eligible), 0) AS 
        person_days_early_discharge_from_supervision_eligible,
    IFNULL(SUM(person_days_full_term_discharge_from_supervision_eligible), 0) 
        AS person_days_full_term_discharge_from_supervision_eligible,

    ## event-based metrics
    /*
        Here we list counts of events and average event attributes during period.
        To get rates, divide by `avg_daily_population`.
        Averages here are representative of the event that occurred.
        
        For metrics that will be null if no event occurred, impute a true zero.
        Since entities must have at least one assigned client to be included in this
        table, all event count metrics should be not-null.
    */

    -- compartment transitions
    IFNULL(SUM(successful_completions), 0) AS successful_completions,
    IFNULL(SUM(incarcerations_all), 0) AS incarcerations_all,
    IFNULL(SUM(incarcerations_all), 0) AS incarcerations_technical,
    IFNULL(SUM(incarcerations_all), 0) AS incarcerations_absconded,
    IFNULL(SUM(incarcerations_all), 0) AS incarcerations_new_crime,
    IFNULL(SUM(incarcerations_unknown_violation), 0) AS incarcerations_unknown_violation,
    IFNULL(SUM(incarcerations_temporary), 0) AS incarcerations_temporary,
    IFNULL(SUM(pending_custody_starts), 0) AS pending_custody_starts,
    IFNULL(SUM(absconsions_bench_warrants), 0) AS absconsions_bench_warrants,
    IFNULL(SUM(incarcerations_all + pending_custody_starts + 
        absconsions_bench_warrants), 0) AS unsuccessful_terminations,
    IFNULL(SUM(early_discharge_requests), 0) AS early_discharge_requests,
    IFNULL(SUM(supervision_downgrades), 0) AS supervision_downgrades,
    IFNULL(SUM(supervision_upgrades), 0) AS supervision_upgrades,
    IFNULL(SUM(supervision_downgrades_to_limited), 0) AS supervision_downgrades_to_limited,

    -- violations
    IFNULL(SUM(violations), 0) AS violations,
    IFNULL(SUM(violations_absconded), 0) AS violations_absconded,
    IFNULL(SUM(violations_new_crime), 0) AS violations_new_crime,
    IFNULL(SUM(violations_technical), 0) AS violations_technical,

    -- violation responses
    IFNULL(SUM(violation_responses), 0) AS violation_responses,
    IFNULL(SUM(violation_responses_absconded), 0) AS violation_responses_absconded,
    IFNULL(SUM(violation_responses_new_crime), 0) AS violation_responses_new_crime,
    IFNULL(SUM(violation_responses_technical), 0) AS violation_responses_technical,

    -- drug screens
    IFNULL(SUM(drug_screens_all), 0) AS drug_screens_all,
    IFNULL(SUM(drug_screens_positive), 0) AS drug_screens_positive,

    -- assessments
    IFNULL(SUM(lsir_assessments_any_officer), 0) AS lsir_assessments_any_officer,
    IFNULL(SUM(lsir_risk_increase_any_officer), 0) AS lsir_risk_increase_any_officer,
    IFNULL(SUM(lsir_risk_decrease_any_officer), 0) AS lsir_risk_decrease_any_officer,
    -- can be null if no lsir assessments
    SAFE_DIVIDE(
        SUM(lsir_assessments_any_officer * avg_lsir_score_change_any_officer),
        SUM(lsir_assessments_any_officer)
    ) AS avg_lsir_score_change_any_officer,

    -- contacts
    IFNULL(SUM(contacts_completed), 0) AS contacts_completed,
    IFNULL(SUM(contacts_attempted), 0) AS contacts_attempted,
    IFNULL(SUM(contacts_home_visit), 0) AS contacts_home_visit,
    IFNULL(SUM(contacts_face_to_face), 0) AS contacts_face_to_face,

    -- employment
    IFNULL(SUM(employment_gained), 0) AS employment_gained,
    IFNULL(SUM(employment_lost), 0) AS employment_lost,

    -- treatment referrals
    IFNULL(SUM(treatment_referrals), 0) AS treatment_referrals,
    
    -- N days late responding to task
    IFNULL(SUM(late_opportunity_supervision_level_downgrade_7_days), 0) AS
        late_opportunity_supervision_level_downgrade_7_days,
    IFNULL(SUM(late_opportunity_supervision_level_downgrade_30_days), 0) AS
        late_opportunity_supervision_level_downgrade_30_days,
    IFNULL(SUM(late_opportunity_early_discharge_from_supervision_7_days), 0) AS
        late_opportunity_early_discharge_from_supervision_7_days,
    IFNULL(SUM(late_opportunity_early_discharge_from_supervision_30_days), 0) AS
        late_opportunity_early_discharge_from_supervision_30_days,
    IFNULL(SUM(late_opportunity_full_term_discharge_7_days), 0) AS
        late_opportunity_full_term_discharge_7_days,
    IFNULL(SUM(late_opportunity_full_term_discharge_30_days), 0) AS
        late_opportunity_full_term_discharge_30_days,    

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
        
        These metrics may be null if no clients were assigned during the period, though
        `clients_assigned` should never be null.
    */
    -- number of clients assigned to each {level_name} - the 'cohort' size
    IFNULL(SUM(clients_assigned), 0) AS clients_assigned,

    -- cumulative number of days observed since assignment up to end of window
    {add_window_to_metric("SUM", "days_since_assignment", window_days)},

    -- average attributes at assignment for the cohort
    -- these depend on period length but not window
    -- these can be null if no clients assigned during period
    SUM(lsir_score_present_at_assignment_cohort) AS 
        lsir_score_present_at_assignment_cohort,
    SAFE_DIVIDE(SUM(avg_lsir_score_at_assignment_cohort * 
        lsir_score_present_at_assignment_cohort),
        SUM(lsir_score_present_at_assignment_cohort)
    ) AS avg_lsir_score_at_assignment_cohort,

    -- days until 'event' since assignment
    -- divide these by `days_since_assignment_X` to get percent of window until event
    -- divide these by `clients_assigned` to get average days until event per person
    -- all can be null if no clients assigned during period
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
    `{{project_id}}.{{analyst_dataset}}.supervision_{level_name}_unnested_metrics` a
INNER JOIN
    truncated_dates b
ON
    a.date BETWEEN b.start_date AND b.end_date
LEFT JOIN
    primary_officer_count
USING
    ({index_cols}, start_date)
{officer_level_metrics_join}
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
            should_materialize=False,  # materialize in an unmanaged dataset instead
        )
    )

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in SUPERVISION_AGGREGATED_METRICS_VIEW_BUILDERS:
            view_builder.build_and_print()
