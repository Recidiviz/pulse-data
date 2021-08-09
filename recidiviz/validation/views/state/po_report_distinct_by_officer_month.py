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

"""A view revealing when one officer has more than one row in the PO report table."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PO_REPORT_DISTINCT_BY_OFFICER_MONTH_VIEW_NAME = "po_report_distinct_by_officer_month"

PO_REPORT_DISTINCT_BY_OFFICER_MONTH_DESCRIPTION = """
  A list of officers that have more than one PO report row per recipient and review_month.
"""

PO_REPORT_DISTINCT_BY_OFFICER_MONTH_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
      state_code as region_code, review_month, email_address, count(*) AS total_rows
    FROM `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized` t1
    GROUP BY state_code, review_month, email_address
    HAVING count(*) > 1
    """

PO_REPORT_DISTINCT_BY_OFFICER_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PO_REPORT_DISTINCT_BY_OFFICER_MONTH_VIEW_NAME,
    view_query_template=PO_REPORT_DISTINCT_BY_OFFICER_MONTH_QUERY_TEMPLATE,
    description=PO_REPORT_DISTINCT_BY_OFFICER_MONTH_DESCRIPTION,
    po_report_dataset=state_dataset_config.PO_REPORT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_REPORT_DISTINCT_BY_OFFICER_MONTH_VIEW_BUILDER.build_and_print()
