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

from recidiviz.calculator.query import bqview, bq_utils
from recidiviz.calculator.query.state import view_config
from recidiviz.utils import metadata

PROJECT_ID = metadata.project_id()
METRICS_DATASET = view_config.DATAFLOW_METRICS_DATASET
REFERENCE_DATASET = view_config.REFERENCE_TABLES_DATASET

EVENT_BASED_ADMISSIONS_VIEW_NAME = 'event_based_admissions'

EVENT_BASED_ADMISSIONS_DESCRIPTION = """
 Admission data on the person level with admission district (county of residence), admission date, and admission reason.

 Expanded Dimensions: district
 """

EVENT_BASED_ADMISSIONS_QUERY = \
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
    """.format(
        description=EVENT_BASED_ADMISSIONS_DESCRIPTION,
        project_id=PROJECT_ID,
        metrics_dataset=METRICS_DATASET,
        reference_dataset=REFERENCE_DATASET,
        district_dimension=bq_utils.unnest_district(district_column='county_of_residence')
    )

EVENT_BASED_ADMISSIONS_VIEW = bqview.BigQueryView(
    view_id=EVENT_BASED_ADMISSIONS_VIEW_NAME,
    view_query=EVENT_BASED_ADMISSIONS_QUERY
)

if __name__ == '__main__':
    print(EVENT_BASED_ADMISSIONS_VIEW.view_id)
    print(EVENT_BASED_ADMISSIONS_VIEW.view_query)
