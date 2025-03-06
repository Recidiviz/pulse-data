#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
#  #
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  #
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  #
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Time series view of vitals metrics at the state- and district-level."""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    generate_district_id_from_district_name,
    list_to_query_string,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_summaries import (
    VITALS_ENTITIES,
    VITALS_METRICS,
    VITALS_METRICS_BY_STATE,
    calculate_rate_query_fragment,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string_formatting import fix_indent

VITALS_TIME_SERIES_VIEW_NAME = "vitals_time_series"

VITALS_TIME_SERIES_DESCRIPTION = """
Historical record of vitals metrics over the last 365 days
"""


def vitals_time_series_query() -> str:
    """Query representing vitals metrics over time, at the state, district, and PO level."""

    # Creates one subquery per entity (officer, district, state), filtering entities and metrics to
    # the states where they apply. Each metric is pulled at the "day" level to get the value as of
    # that start date and the "month" level to get a 30 day rate ending on the same day. In order
    # to create the value for the time series query output, divide the appropriate
    # numerator/denominator, subtract it from 1, and multiply by 100. For 30 day rates, round the
    # result to 1 decimal place.
    #
    # For example, if the "office" entity is only valid in US_ND, and some metrics are valid in
    # US_ND, others in US_IX, and others in both, the generated subquery will look like this:
    # SELECT
    #   today.state_code,
    #   today.start_date AS date,
    #   REPLACE(REGEXP_REPLACE(today.office_name, r"[',-]", ''), ' ' , '_') AS entity_id,
    #   IF(today.state_code IN ("US_ND"), 100 * (1 - IFNULL(SAFE_DIVIDE(today.avg_population_past_full_term_release_date, today.avg_daily_population), 0)), NULL) AS timely_discharge,
    #   IF(today.state_code IN ("US_ND", "US_IX"), 100 * (1 - IFNULL(SAFE_DIVIDE(today.avg_population_assessment_overdue, today.avg_population_assessment_required), 0)), NULL) AS timely_risk_assessment,
    #   IF(today.state_code IN ("US_ND", "US_IX"), 100 * (1 - IFNULL(SAFE_DIVIDE(today.avg_population_contact_overdue, today.avg_population_contact_required), 0)), NULL) AS timely_contact,
    #   IF(today.state_code IN ("US_IX"), 100 * (1 - IFNULL(SAFE_DIVIDE(today.avg_population_task_eligible_supervision_level_downgrade, today.avg_daily_population), 0)), NULL) AS timely_downgrade,
    #   ROUND(IF(today.state_code IN ("US_ND"), 100 * (1 - IFNULL(SAFE_DIVIDE(thirty_day.avg_population_past_full_term_release_date, thirty_day.avg_daily_population), 0)), NULL), 1) AS timely_discharge_30d,
    #   ROUND(IF(today.state_code IN ("US_ND", "US_IX"), 100 * (1 - IFNULL(SAFE_DIVIDE(thirty_day.avg_population_assessment_overdue, thirty_day.avg_population_assessment_required), 0)), NULL), 1) AS timely_risk_assessment_30d,
    #   ROUND(IF(today.state_code IN ("US_ND", "US_IX"), 100 * (1 - IFNULL(SAFE_DIVIDE(thirty_day.avg_population_contact_overdue, thirty_day.avg_population_contact_required), 0)), NULL), 1) AS timely_contact_30d,
    #   ROUND(IF(today.state_code IN ("US_IX"), 100 * (1 - IFNULL(SAFE_DIVIDE(thirty_day.avg_population_task_eligible_supervision_level_downgrade, thirty_day.avg_daily_population), 0)), NULL), 1) AS timely_downgrade_30d,
    # FROM
    #   -- call this "today" even though it represents many days to match what it's called in VITALS_ENTITIES
    #   (
    #     SELECT * FROM `recidiviz-staging.vitals_report_views.supervision_office_aggregated_metrics_materialized`
    #     WHERE period = "DAY"
    #   ) today
    # INNER JOIN
    #   (
    #     SELECT * FROM `recidiviz-staging.vitals_report_views.supervision_office_aggregated_metrics_materialized`
    #     WHERE period = "MONTH"
    #   ) thirty_day
    # USING (state_code, end_date, office)
    # WHERE today.state_code IN ("US_ND")
    #
    # and the output will have rows like this:
    # state_code | date       | entity_id | timely_discharge | timely_risk_assessment | timely_contact | timely_downgrade | timely_discharge_30d | timely_risk_assessment_30d | timely_contact_30d | timely_downgrade_30d
    # -----------|------------|-----------|------------------|------------------------|----------------|------------------|----------------------|----------------------------|--------------------|---------------------
    # US_ND      | 2025-01-30 | BEULAH    | 100.0            | 95.454545454545...     | 90.90909090... | null             | 100.0                | 97.5                       | 86.8               | null

    entity_subqueries = [
        f"""
  SELECT
    today.state_code,
    today.start_date AS date,
    {generate_district_id_from_district_name(vitals_entity.entity_id_field)} AS entity_id,
    {calculate_rate_query_fragment("today")}
    {calculate_rate_query_fragment("thirty_day", "30d", 1)}
  FROM
    -- call this "today" even though it represents many days to match what it's called in VITALS_ENTITIES
    (
      SELECT * FROM `{{project_id}}.vitals_report_views.supervision_{vitals_entity.aggregated_metrics_entity}_aggregated_metrics_materialized`
      WHERE period = "DAY"
    ) today
  INNER JOIN
    (
      SELECT * FROM `{{project_id}}.vitals_report_views.supervision_{vitals_entity.aggregated_metrics_entity}_aggregated_metrics_materialized`
      WHERE period = "MONTH"
    ) thirty_day
  USING (state_code, end_date, {vitals_entity.entity_id_join_field})
  {vitals_entity.join_query_fragment}
  {f"WHERE today.state_code IN ({list_to_query_string(vitals_entity.enabled_states, quoted=True)})"}
  {f'AND {vitals_entity.exclusion_query_fragment}' if vitals_entity.exclusion_query_fragment else ""}
"""
        for vitals_entity in VITALS_ENTITIES
    ]

    # UNION ALL each subquery together so that we have metrics for all entity types together in one
    # CTE.
    all_metrics = fix_indent("\nUNION ALL\n".join(entity_subqueries), indent_level=2)

    # Each entity also needs an "overall" metric and an "overall_30d" metric for each day. To
    # calculate this, we take the average of the calculated values from the above subqueries for all
    # the metrics enabled for that state and round it to 1 decimal point.
    #
    # The generated CTE here will look like this:
    # SELECT
    #   *,
    #   CASE state_code
    #     WHEN 'US_ND' THEN ROUND(SAFE_DIVIDE(timely_discharge+timely_risk_assessment+timely_contact, 3), 1)
    #     WHEN 'US_IX' THEN ROUND(SAFE_DIVIDE(timely_risk_assessment+timely_contact+timely_downgrade, 3), 1)
    #   END AS overall,
    #   CASE state_code
    #     WHEN 'US_ND' THEN ROUND(SAFE_DIVIDE(timely_discharge_30d+timely_risk_assessment_30d+timely_contact_30d, 3), 1)
    #     WHEN 'US_IX' THEN ROUND(SAFE_DIVIDE(timely_risk_assessment_30d+timely_contact_30d+timely_downgrade_30d, 3), 1)
    #   END AS overall_30d
    # FROM all_metrics
    #
    # and the result rows here will look like the ones from the previous section, but with two extra
    # columns: overall and overall_30d.
    overall_cases = "\n".join(
        f"    WHEN '{state}' THEN ROUND(SAFE_DIVIDE({'+'.join(vitals_metric_names)}, {len(vitals_metric_names)}), 1)"
        for state, vitals_metric_names in VITALS_METRICS_BY_STATE.items()
    )
    overall_cases_30d = "\n".join(
        f"    WHEN '{state}' THEN ROUND(SAFE_DIVIDE({'+'.join([f'{vitals_metric_name}_30d' for vitals_metric_name in vitals_metric_names])}, {len(vitals_metric_names)}), 1)"
        for state, vitals_metric_names in VITALS_METRICS_BY_STATE.items()
    )
    all_metrics_with_overall = fix_indent(
        f"""
SELECT
  *,
  CASE state_code
    {overall_cases}
  END AS overall,
  CASE state_code
    {overall_cases_30d}
  END AS overall_30d
FROM all_metrics
	""",
        indent_level=2,
    )

    # At this point, each metric is a column in the table, but we want the final output to contain
    # one row per metric per entity per date, with the formatted name of the metric in a column
    # called "metric", the value of the single day rate in a column called "value", and the value of
    # the 30 day rate in a column called "avg_30d". To accomplish this, use an UNPIVOT statement.
    #
    # The generated statement will look like:
    # UNPIVOT ((value, avg_30d) FOR metric IN ((timely_discharge, timely_discharge_30d) AS "DISCHARGE", (timely_risk_assessment, timely_risk_assessment_30d) AS "RISK_ASSESSMENT", (timely_contact, timely_contact_30d) AS "CONTACT", (timely_downgrade, timely_downgrade_30d) AS "DOWNGRADE", (overall, overall_30d) AS "OVERALL"))
    #
    # Once added to the query, the result rows will look like:
    # state_code | date       | entity_id | value        | avg_30d | metric
    # -----------|------------|-----------|--------------|---------|--------
    # US_ND      | 2024-09-10 | BEULAH    | 90.909090... | 86.8    | CONTACT
    metric_names_to_unpivot = [
        *[metric.vitals_metric_name for metric in VITALS_METRICS],
        "overall",
    ]
    unpivot_stmt = ", ".join(
        f'({metric_name}, {metric_name}_30d) AS "{metric_name.removeprefix("timely_").upper()}"'
        for metric_name in metric_names_to_unpivot
    )

    # Put it all together! At this point, also exclude certain entities that should not be in the
    # final output.
    query = f"""
WITH all_metrics AS (
{all_metrics}
)
, all_metrics_with_overall AS (
{all_metrics_with_overall}
)
SELECT * FROM all_metrics_with_overall
UNPIVOT ((value, avg_30d) FOR metric IN ({unpivot_stmt}))
WHERE date >= DATE_SUB(CURRENT_DATE("US/Eastern"), INTERVAL 180 DAY)
"""
    return query


VITALS_TIME_SERIES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=VITALS_TIME_SERIES_VIEW_NAME,
    description=VITALS_TIME_SERIES_DESCRIPTION,
    view_query_template=vitals_time_series_query(),
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VITALS_TIME_SERIES_VIEW_BUILDER.build_and_print()
