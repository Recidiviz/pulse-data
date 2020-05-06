# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Event Based Admissions."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import view_config
EVENT_BASED_ADMISSIONS_VIEW_NAME = 'event_based_admissions'

EVENT_BASED_ADMISSIONS_DESCRIPTION = """
 Admission data on the person level with admission district (county of residence), admission date, and admission reason.

 Expanded Dimensions: district
 """

EVENT_BASED_ADMISSIONS_QUERY_TEMPLATE = \
    """
    /*{description}*/
    SELECT
      person_id, state_code, year, month,
      district,
      admission_reason, admission_date
    FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`
    JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
      USING (state_code, job_id, year, month, metric_period_months),
    {district_dimension}
    WHERE methodology = 'EVENT'
      AND person_id IS NOT NULL
      AND metric_period_months = 1
      AND month IS NOT NULL
      AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
      AND job.metric_type = 'INCARCERATION_ADMISSION'
    """

EVENT_BASED_ADMISSIONS_VIEW = BigQueryView(
    dataset_id=view_config.REFERENCE_TABLES_DATASET,
    view_id=EVENT_BASED_ADMISSIONS_VIEW_NAME,
    view_query_template=EVENT_BASED_ADMISSIONS_QUERY_TEMPLATE,
    description=EVENT_BASED_ADMISSIONS_DESCRIPTION,
    metrics_dataset=view_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=view_config.REFERENCE_TABLES_DATASET,
    district_dimension=bq_utils.unnest_district(district_column='county_of_residence')
)

if __name__ == '__main__':
    print(EVENT_BASED_ADMISSIONS_VIEW.view_id)
    print(EVENT_BASED_ADMISSIONS_VIEW.view_query)
