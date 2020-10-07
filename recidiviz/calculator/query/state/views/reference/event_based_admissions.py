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
"""Event Based Admissions."""
# pylint: disable=trailing-whitespace, line-too-long

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

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
      district, facility,
      admission_reason, admission_date
    FROM `{project_id}.{metrics_dataset}.incarceration_admission_metrics`
    {filter_to_most_recent_job_id_for_metric},
    {district_dimension}
    WHERE methodology = 'EVENT'
      AND person_id IS NOT NULL
      AND metric_period_months = 1
      AND month IS NOT NULL
      AND year >= EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR))
    """

EVENT_BASED_ADMISSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=EVENT_BASED_ADMISSIONS_VIEW_NAME,
    view_query_template=EVENT_BASED_ADMISSIONS_QUERY_TEMPLATE,
    description=EVENT_BASED_ADMISSIONS_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    district_dimension=bq_utils.unnest_district(
        district_column='county_of_residence'),
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        EVENT_BASED_ADMISSIONS_VIEW_BUILDER.build_and_print()
