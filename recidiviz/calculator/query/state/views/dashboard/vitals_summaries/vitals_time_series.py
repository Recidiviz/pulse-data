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
# pylint: disable=trailing-whitespace,line-too-long
from recidiviz.calculator.query.bq_utils import (
    clean_up_supervising_officer_external_id,
    generate_district_id_from_district_name,
)
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def generate_time_series_query(metric_name: str, table_name: str) -> str:
    po_condition = "supervising_officer_external_id != 'ALL' AND district_id = 'ALL'"
    district_condition = (
        "supervising_officer_external_id = 'ALL' AND district_id != 'ALL'"
    )

    return f"""
    SELECT
      state_code,
      date_of_supervision as date,
      CASE
        WHEN {po_condition} THEN {clean_up_supervising_officer_external_id()}
        WHEN {district_condition} THEN {generate_district_id_from_district_name('district_name')}
        ELSE 'STATE_DOC'
      END as entity_id,
      "{metric_name.upper()}" as metric,
      ROUND(timely_{metric_name}) as value,
      ROUND(AVG(timely_{metric_name}) OVER (ORDER BY district_id, supervising_officer_external_id, date_of_supervision ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)) as avg_30d
    FROM `{{project_id}}.{{vitals_report_dataset}}.{table_name}`
    WHERE (supervising_officer_external_id = 'ALL' OR district_id = 'ALL')
      AND district_id <> "UNKNOWN"
      AND date_of_supervision >= DATE_SUB(CURRENT_DATE(), INTERVAL 210 DAY) -- Need to go an additional 30 days back for the avg
      AND state_code = 'US_ND'
    """


VITALS_TIME_SERIES_VIEW_NAME = "vitals_time_series"

VITALS_TIME_SERIES_DESCRIPTION = """
    Historical record of vitals metrics over the last 365 days
 """

VITALS_TIME_SERIES_TEMPLATE = f"""
  /*{{description}}*/
  WITH discharge AS (
    {generate_time_series_query("discharge", "supervision_population_due_for_release_by_po_by_day")}
  ), risk_assessment AS (
    {generate_time_series_query("risk_assessment", "overdue_lsir_by_po_by_day")}
  ), contact AS (
    {generate_time_series_query("contact", "timely_contact_by_po_by_day")}
  ), summary AS (
    SELECT
      state_code,
      date,
      entity_id,
      "OVERALL" as metric,
      ROUND((discharge.value + risk_assessment.value + contact.value)/3) as value,
      ROUND((discharge.avg_30d + risk_assessment.avg_30d + contact.avg_30d)/3) as avg_30d
    FROM discharge
      JOIN risk_assessment 
      USING (state_code, date, entity_id)
      JOIN contact
      USING (state_code, date, entity_id)
    ORDER BY entity_id, date
  )
  SELECT
   *
  FROM (
    SELECT * FROM discharge WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    UNION ALL
    SELECT * FROM risk_assessment WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    UNION ALL
    SELECT * FROM contact WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
    UNION ALL
    SELECT * FROM summary WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
  )
  WHERE value != 0
    OR metric = "contact"
  ORDER BY entity_id, date, metric
"""

VITALS_TIME_SERIES_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.DASHBOARD_VIEWS_DATASET,
    view_id=VITALS_TIME_SERIES_VIEW_NAME,
    description=VITALS_TIME_SERIES_DESCRIPTION,
    view_query_template=VITALS_TIME_SERIES_TEMPLATE,
    dimensions=("entity_id", "state_code"),
    vitals_report_dataset=dataset_config.VITALS_REPORT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VITALS_TIME_SERIES_VIEW_BUILDER.build_and_print()
