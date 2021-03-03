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
"""Week dates by state for COVID-19 Report"""

# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

COVID_REPORT_WEEKS_VIEW_NAME = "covid_report_weeks"

COVID_REPORT_WEEKS_DESCRIPTION = """Week dates by state for COVID-19 Report"""

COVID_REPORT_WEEKS_QUERY_TEMPLATE = """
    /*{description}*/
    -- US_ID report starting 2020-05-02 --
    SELECT
      'US_ID' as state_code,
      (row_number() OVER ()) - 1 as week_num,
      start_date,
      DATE_ADD(start_date, INTERVAL 13 DAY) as end_date
    FROM
    (SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('2020-05-02', CURRENT_DATE('America/Los_Angeles'), INTERVAL 2 WEEK)) as start_date)

    UNION ALL

    -- US_ND report starting 2020-03-12 --
    SELECT
      'US_ND' as state_code,
      (row_number() OVER ()) - 1 as week_num,
      start_date,
      DATE_ADD(start_date, INTERVAL 13 DAY) as end_date
    FROM
    (SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('2020-03-12', CURRENT_DATE('America/Los_Angeles'), INTERVAL 2 WEEK)) as start_date)
"""

COVID_REPORT_WEEKS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.REFERENCE_VIEWS_DATASET,
    view_id=COVID_REPORT_WEEKS_VIEW_NAME,
    view_query_template=COVID_REPORT_WEEKS_QUERY_TEMPLATE,
    description=COVID_REPORT_WEEKS_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        COVID_REPORT_WEEKS_VIEW_BUILDER.build_and_print()
