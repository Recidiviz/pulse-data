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
"""Daily case counts for each unique facility ID."""
# pylint: disable=trailing-whitespace
from recidiviz.calculator.query.state import dataset_config
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

FACILITY_CASE_DATA_VIEW_NAME = "facility_case_data"

FACILITY_CASE_DATA_VIEW_DESCRIPTION = (
    """Daily case counts for each unique facility ID."""
)

FACILITY_CASE_DATA_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH cases AS (
    SELECT
        facility_id, c.*
      FROM `{project_id}.{covid_dashboard_reference_dataset}.covid_cases_by_facility` c
      INNER JOIN `{project_id}.{covid_dashboard_reference_dataset}.facility_alias` f
        ON c.state = f.state AND c.canonical_facility_name = f.facility_name
    )
    
    SELECT 
      facility_id,
      date,
      pop_tested,
      pop_tested_negative,
      pop_tested_positive,
      pop_deaths,
      staff_tested,
      staff_tested_negative,
      staff_tested_positive,
      staff_deaths,
    FROM cases
    ORDER BY date, facility_id
    """

FACILITY_CASE_DATA_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.COVID_DASHBOARD_DATASET,
    view_id=FACILITY_CASE_DATA_VIEW_NAME,
    view_query_template=FACILITY_CASE_DATA_VIEW_QUERY_TEMPLATE,
    description=FACILITY_CASE_DATA_VIEW_DESCRIPTION,
    dimensions=("facility_id", "date"),
    covid_dashboard_reference_dataset=dataset_config.COVID_DASHBOARD_REFERENCE_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        FACILITY_CASE_DATA_VIEW_BUILDER.build_and_print()
