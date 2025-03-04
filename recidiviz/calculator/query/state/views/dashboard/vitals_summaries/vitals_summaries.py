# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Daily summaries of vitals metrics, at the state, district, and PO level."""

import attr

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.bq_utils import (
    generate_district_id_from_district_name,
    list_to_query_string,
    nonnull_end_date_exclusive_clause,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VITALS_SUMMARIES_VIEW_NAME = "vitals_summaries"

VITALS_SUMMARIES_DESCRIPTION = """
Daily summaries of vitals metrics, at the state, district, and PO level.
"""


@attr.define(frozen=True)
class VitalsEntity:
    # Entity type as shown in final output
    entity_type: str
    # Entity id as shown in final output
    entity_id_field: str
    # Field within the aggregated metrics table to join on for this entity
    entity_id_join_field: str
    # Entity name as shown in final output
    entity_name_field: str
    # Parent entity id as shown in final output
    parent_entity_id_field: str
    # Aggregated metrics unit of analysis name for this entity
    aggregated_metrics_entity: str
    # States to include metrics for this entity
    enabled_states: list[str]
    # Whether we need to join to a table of staff data to get additional information about this entity, for example to fill in the parent_entity_id_field
    include_staff_join: bool = False


@attr.define(frozen=True)
class VitalsMetric:
    # Name of the metric as we refer to it in vitals
    vitals_metric_name: str
    # Name of the metric to look up in aggregated metrics
    aggregated_metric_name: str
    # Name of the denominator metric to look up in aggregated metrics to calculate the rate
    aggregated_metric_denominator: str
    # States that have this metric enabled
    enabled_states: list[str]


VITALS_ENTITIES = [
    VitalsEntity(
        entity_type="po",
        entity_id_field="today.officer_id",
        entity_id_join_field="officer_id",
        entity_name_field="UPPER(today.officer_name)",
        parent_entity_id_field="staff.district",
        aggregated_metrics_entity="officer",
        enabled_states=["US_ND", "US_IX"],
        include_staff_join=True,
    ),
    VitalsEntity(
        entity_type="level_1_supervision_location",
        entity_id_field="today.office_name",
        entity_id_join_field="office",
        entity_name_field="today.office_name",
        parent_entity_id_field='"STATE_DOC"',
        aggregated_metrics_entity="office",
        enabled_states=["US_ND"],
    ),
    VitalsEntity(
        entity_type="level_2_supervision_location",
        entity_id_field="today.district",
        entity_id_join_field="district",
        entity_name_field="today.district",
        parent_entity_id_field='"STATE_DOC"',
        aggregated_metrics_entity="district",
        enabled_states=["US_IX"],
    ),
    VitalsEntity(
        entity_type="state",
        entity_id_field='"STATE_DOC"',
        entity_id_join_field="state_code",
        entity_name_field='"STATE DOC"',
        parent_entity_id_field='"STATE_DOC"',
        aggregated_metrics_entity="state",
        enabled_states=["US_ND", "US_IX"],
    ),
]

VITALS_METRICS = [
    VitalsMetric(
        vitals_metric_name="timely_discharge",
        aggregated_metric_name="avg_population_past_full_term_release_date",
        aggregated_metric_denominator="avg_daily_population",
        enabled_states=["US_ND"],
    ),
    VitalsMetric(
        vitals_metric_name="timely_risk_assessment",
        aggregated_metric_name="avg_population_assessment_overdue",
        aggregated_metric_denominator="avg_population_assessment_required",
        enabled_states=["US_ND", "US_IX"],
    ),
    VitalsMetric(
        vitals_metric_name="timely_contact",
        aggregated_metric_name="avg_population_contact_overdue",
        aggregated_metric_denominator="avg_population_contact_required",
        enabled_states=["US_ND", "US_IX"],
    ),
    VitalsMetric(
        vitals_metric_name="timely_downgrade",
        aggregated_metric_name="avg_population_task_eligible_supervision_level_downgrade",
        aggregated_metric_denominator="avg_daily_population",
        enabled_states=["US_IX"],
    ),
]

VITALS_METRICS_BY_STATE = {
    state: [m.vitals_metric_name for m in VITALS_METRICS if state in m.enabled_states]
    for metric in VITALS_METRICS
    for state in metric.enabled_states
}


def calculate_rate_for_metric(
    table_name: str,
    vitals_metric: VitalsMetric,
    suffix: str | None = None,
) -> str:
    rate_calc_string = f"100 * (1 - IFNULL(SAFE_DIVIDE({table_name}.{vitals_metric.aggregated_metric_name}, {table_name}.{vitals_metric.aggregated_metric_denominator}), 0))"
    return f"IF(today.state_code IN ({list_to_query_string(vitals_metric.enabled_states, quoted=True)}), {rate_calc_string}, NULL) AS {vitals_metric.vitals_metric_name}{f'_{suffix}' if suffix else ''},"


def calculate_rate_query_fragment(table_name: str, suffix: str | None = None) -> str:
    return "\n    ".join(
        [
            calculate_rate_for_metric(table_name, vitals_metric, suffix)
            for vitals_metric in VITALS_METRICS
        ]
    )


def vitals_summaries_query() -> str:
    """Query representing summaries of vitals metrics, at the state, district, and PO level."""

    staff_join_fragment = f"""
  INNER JOIN
    `{{project_id}}.reference_views.product_staff_materialized` staff
  ON
    today.state_code = staff.state_code
    AND today.officer_id = staff.external_id
    AND today.start_date BETWEEN staff.start_date AND {nonnull_end_date_exclusive_clause("end_date_exclusive")}
    AND staff.is_supervision_officer
    AND NOT (staff.state_code="US_ND" AND (IFNULL(staff.district, "") LIKE "%PRETRIAL" OR IFNULL(staff.district, "") = "CENTRAL OFFICE"))
    AND NOT (staff.state_code="US_IX" AND (staff.district IS NULL OR IFNULL(staff.specialized_caseload_type_primary, "") = "TRANSITIONAL" OR IFNULL(staff.is_supervision_officer_supervisor, FALSE)))
"""
    summary_ctes = "\n,".join(
        [
            f"""
{vitals_entity.aggregated_metrics_entity}_values AS (
  SELECT
    today.state_code,
    today.end_date AS most_recent_date_of_supervision,
    "{vitals_entity.entity_type}" AS entity_type,
    {generate_district_id_from_district_name(vitals_entity.entity_id_field)} AS entity_id,
    {vitals_entity.entity_name_field} AS entity_name,
    {generate_district_id_from_district_name(vitals_entity.parent_entity_id_field)} AS parent_entity_id,
    {calculate_rate_query_fragment("today")}
    {calculate_rate_query_fragment("thirty_days_ago", "30d")}
    {calculate_rate_query_fragment("ninety_days_ago", "90d")}
  FROM
    `{{project_id}}.vitals_report_views.supervision_{vitals_entity.aggregated_metrics_entity}_aggregated_metrics_materialized` today
  LEFT JOIN
    `{{project_id}}.vitals_report_views.supervision_{vitals_entity.aggregated_metrics_entity}_aggregated_metrics_materialized` thirty_days_ago
  ON
    today.state_code = thirty_days_ago.state_code
    AND today.{vitals_entity.entity_id_join_field} = thirty_days_ago.{vitals_entity.entity_id_join_field}
    AND DATE_SUB(today.end_date, INTERVAL 30 DAY) = thirty_days_ago.end_date
    AND today.period = thirty_days_ago.period
  LEFT JOIN
    `{{project_id}}.vitals_report_views.supervision_{vitals_entity.aggregated_metrics_entity}_aggregated_metrics_materialized` ninety_days_ago
  ON
    today.state_code = ninety_days_ago.state_code
    AND today.{vitals_entity.entity_id_join_field} = ninety_days_ago.{vitals_entity.entity_id_join_field}
    AND DATE_SUB(today.end_date, INTERVAL 90 DAY) = ninety_days_ago.end_date
    AND today.period = ninety_days_ago.period
  {staff_join_fragment if vitals_entity.include_staff_join else ''}
  WHERE
    today.end_date = CURRENT_DATE("US/Eastern")
    AND today.period = "DAY"
)
"""
            for vitals_entity in VITALS_ENTITIES
        ]
    )

    overall_cases = "\n".join(
        f"    WHEN '{state}' THEN ROUND(SAFE_DIVIDE({'+'.join(vitals_metric_name)}, {len(vitals_metric_name)}), 0)"
        for state, vitals_metric_name in VITALS_METRICS_BY_STATE.items()
    )

    def past(suffix: str) -> str:
        return "\n".join(
            f"    WHEN '{state}' THEN ROUND(SAFE_DIVIDE({'+'.join(vitals_metric_name)}, {len(vitals_metric_name)}), 0) - ROUND(SAFE_DIVIDE({'+'.join(f'{vital}_{suffix}' for vital in vitals_metric_name)}, {len(vitals_metric_name)}), 0)"
            for state, vitals_metric_name in VITALS_METRICS_BY_STATE.items()
        )

    summary_queries = "\nUNION ALL\n".join(
        [
            f"""
SELECT
  state_code,
  most_recent_date_of_supervision,
  entity_type,
  entity_id,
  entity_name,
  parent_entity_id,
  ROUND(timely_discharge, 0) AS timely_discharge,
  ROUND(timely_risk_assessment, 0) AS timely_risk_assessment,
  ROUND(timely_contact, 0) AS timely_contact,
  ROUND(timely_downgrade, 0) AS timely_downgrade,
  CASE state_code
{overall_cases}
  END AS overall,
  CASE state_code
{past("30d")}
  END AS overall_30d,
  CASE state_code
{past("90d")}
  END AS overall_90d
FROM {vitals_entity.aggregated_metrics_entity}_values
WHERE
    NOT (state_code="US_ND" AND (entity_id LIKE "%PRETRIAL%" OR entity_id IN ("CENTRAL_OFFICE", "EXTERNAL_UNKNOWN")))
    AND NOT (state_code="US_IX" AND entity_id IN ("PAROLE_COMMISSION_OFFICE", "DISTRICT_0", "EXTERNAL_UNKNOWN", "OUT_OF_STATE"))
{f"AND state_code IN ({list_to_query_string(vitals_entity.enabled_states, quoted=True)})"}
"""
            for vitals_entity in VITALS_ENTITIES
        ]
    )

    return f"""
WITH {summary_ctes}
{summary_queries}
"""


VITALS_SUMMARIES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=VITALS_SUMMARIES_VIEW_NAME,
    view_query_template=vitals_summaries_query(),
    description=VITALS_SUMMARIES_DESCRIPTION,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VITALS_SUMMARIES_VIEW_BUILDER.build_and_print()
