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
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.bq_utils import generate_district_id_from_district_name
from recidiviz.calculator.query.state.state_specific_query_strings import (
    VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VITALS_SUMMARIES_VIEW_NAME = "vitals_summaries"

VITALS_SUMMARIES_DESCRIPTION = """
Daily summaries of vitals metrics, at the state, district, and PO level.
"""


def generate_entity_summary_query(field: str, vitals_table: str) -> str:
    po_condition = "supervising_officer_external_id != 'ALL' AND district_id != 'ALL'"
    district_condition = (
        "supervising_officer_external_id = 'ALL' AND district_id != 'ALL'"
    )

    district_level_name = f"""
        IF(metric_table.state_code in {VITALS_LEVEL_1_SUPERVISION_LOCATION_OPTIONS}, 'level_1_supervision_location', 'level_2_supervision_location')
    """

    return f"""
    SELECT
      metric_table.state_code,
      most_recent_date_of_supervision,
      MAX(IF(date_of_supervision = most_recent_job_id.most_recent_date_of_supervision, {field}, null)) as most_recent_{field},
      MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 30 DAY), {field}, null)) as {field}_30_days_before,
      MAX(IF(date_of_supervision = DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 90 DAY), {field}, null)) as {field}_90_days_before,
      CASE
        WHEN {po_condition} THEN {bq_utils.clean_up_supervising_officer_external_id()}
        WHEN {district_condition} THEN {generate_district_id_from_district_name('district_name')}
        ELSE 'STATE_DOC'
      END as entity_id,
      CASE
        WHEN {po_condition} THEN supervising_officer_external_id
        WHEN {district_condition} THEN district_name
        ELSE 'STATE DOC'
      END as entity_name,
      IF( {po_condition}, {generate_district_id_from_district_name('district_name')}, 'STATE_DOC') as parent_entity_id,
      CASE 
        WHEN {po_condition} THEN 'po'
        WHEN {district_condition} THEN {district_level_name}
        ELSE 'state'
      END as entity_type,
    FROM
     `{{project_id}}.{{vitals_report_dataset}}.{vitals_table}` metric_table
    INNER JOIN
        most_recent_job_id
    ON
        metric_table.state_code = most_recent_job_id.state_code
    WHERE (supervising_officer_external_id = 'ALL' OR district_id <> 'ALL')
      AND date_of_supervision IN (
        most_recent_job_id.most_recent_date_of_supervision,
        DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 30 DAY),
        DATE_SUB(most_recent_job_id.most_recent_date_of_supervision, INTERVAL 90 DAY))
    GROUP BY state_code, most_recent_date_of_supervision, entity_id, entity_name, parent_entity_id, supervising_officer_external_id, district_id
    """


VITALS_SUMMARIES_QUERY_TEMPLATE = f"""
    /*{{description}}*/
    WITH most_recent_job_id AS (
    SELECT
        state_code,
        metric_date as most_recent_date_of_supervision,
        job_id,
        metric_type
      FROM
        `{{project_id}}.{{materialized_metrics_dataset}}.most_recent_daily_job_id_by_metric_and_state_code_materialized`
      WHERE metric_type = "SUPERVISION_POPULATION"
    ), 
    timely_discharge AS (
     {generate_entity_summary_query('timely_discharge', 'supervision_population_due_for_release_by_po_by_day')}
    ),
    timely_risk_assessment AS (
     {generate_entity_summary_query('timely_risk_assessment', 'overdue_lsir_by_po_by_day')}
    )

    SELECT 
      state_code,
      timely_discharge.most_recent_date_of_supervision,
      timely_discharge.entity_type,
      timely_discharge.entity_id,
      timely_discharge.entity_name,
      timely_discharge.parent_entity_id,
      ROUND((most_recent_timely_discharge + most_recent_timely_risk_assessment + 80) / 3, 0) as overall,
      ROUND(most_recent_timely_discharge) as timely_discharge,
      ROUND(most_recent_timely_risk_assessment) as timely_risk_assessment,
      # TODO(#6703): update once contact vitals are completed.
      80 as timely_contact,
      ROUND((most_recent_timely_discharge + most_recent_timely_risk_assessment + 80) / 3 - (timely_discharge_30_days_before + timely_risk_assessment_30_days_before  + 80) / 3, 0) as overall_30d,
      ROUND((most_recent_timely_discharge + most_recent_timely_risk_assessment + 80) / 3 - (timely_discharge_90_days_before + timely_risk_assessment_90_days_before + 80) / 3, 0) as overall_90d
    FROM timely_discharge
    LEFT JOIN timely_risk_assessment
      USING (state_code, entity_id, parent_entity_id)
    WHERE entity_id is not null
      AND entity_id != 'UNKNOWN'
      AND ROUND(most_recent_timely_discharge) != 0
      AND ROUND(most_recent_timely_risk_assessment) != 0
    ORDER BY most_recent_date_of_supervision DESC
"""

VITALS_SUMMARIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=VITALS_SUMMARIES_VIEW_NAME,
    view_query_template=VITALS_SUMMARIES_QUERY_TEMPLATE,
    dimensions=("entity_id", "entity_name", "parent_entity_id"),
    description=VITALS_SUMMARIES_DESCRIPTION,
    vitals_report_dataset=dataset_config.VITALS_REPORT_DATASET,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VITALS_SUMMARIES_VIEW_BUILDER.build_and_print()
