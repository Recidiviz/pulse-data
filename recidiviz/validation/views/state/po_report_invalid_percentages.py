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
"""A view describing when the PO Monthly Report outputs invalid percentages"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

PO_REPORT_INVALID_PERCENTAGES_NAME = "po_report_invalid_percentages"

PO_REPORT_INVALID_PERCENTAGES_DESCRIPTION = """
  Outputs rows that have percentage values over 100%
"""

PO_REPORT_INVALID_PERCENTAGES_QUERY_TEMPLATE = """
  SELECT
    * EXCEPT (state_code),
    state_code AS region_code
  FROM
    `{project_id}.{po_report_dataset}.po_monthly_report_data_materialized`
  WHERE
    assessments_percent > 100.0
    OR facetoface_percent > 100.0
"""

PO_REPORT_INVALID_PERCENTAGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=PO_REPORT_INVALID_PERCENTAGES_NAME,
    view_query_template=PO_REPORT_INVALID_PERCENTAGES_QUERY_TEMPLATE,
    description=PO_REPORT_INVALID_PERCENTAGES_DESCRIPTION,
    po_report_dataset=state_dataset_config.PO_REPORT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PO_REPORT_INVALID_PERCENTAGES_VIEW_BUILDER.build_and_print()
