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

"""A view which provides a comparison of total case termination counts per month for views that count terminations
  by type."""

# pylint: disable=trailing-whitespace
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_NAME = 'case_termination_by_type_comparison'

CASE_TERMINATIONS_BY_TYPE_COMPARISON_DESCRIPTION = """Supervision case termination count comparison per month"""

CASE_TERMINATIONS_BY_TYPE_COMPARISON_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH by_month as (
      SELECT
        state_code as region_code, year, month,
        SUM(absconsion) as absconsion_count, SUM(discharge) as discharge_count
      FROM `{project_id}.{view_dataset}.case_terminations_by_type_by_month`
      WHERE district != 'ALL' AND supervision_type = 'ALL'
      GROUP BY region_code, year, month
    ),
    absconsions_by_officer as (
      SELECT state_code as region_code, year, month, SUM(absconsions) as absconsion_count
      FROM `{project_id}.{po_report_dataset}.supervision_absconsion_terminations_by_officer_by_month_materialized`
      GROUP BY region_code, year, month
    ),
    discharges_by_officer as (
      SELECT state_code as region_code, year, month, SUM(pos_discharges) as discharge_count
      FROM `{project_id}.{po_report_dataset}.supervision_discharges_by_officer_by_month_materialized`
      WHERE district != 'ALL' AND officer_external_id != 'ALL'
      GROUP BY region_code, year, month
    )
    SELECT region_code, year, month,
       by_month.absconsion_count as absconsions_by_month,
       absconsions_by_officer.absconsion_count as absconsions_by_officer,

       by_month.discharge_count as discharges_by_month,
       discharges_by_officer.discharge_count as discharges_by_officer
    FROM by_month
    FULL OUTER JOIN absconsions_by_officer USING (region_code, year, month)
    FULL OUTER JOIN discharges_by_officer USING (region_code, year, month)
    WHERE by_month.absconsion_count != absconsions_by_officer.absconsion_count
      OR by_month.discharge_count != discharges_by_officer.discharge_count
    ORDER BY region_code, year, month
"""

CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_NAME,
    view_query_template=CASE_TERMINATIONS_BY_TYPE_COMPARISON_QUERY_TEMPLATE,
    description=CASE_TERMINATIONS_BY_TYPE_COMPARISON_DESCRIPTION,
    view_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    po_report_dataset=state_dataset_config.PO_REPORT_DATASET
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER.build_and_print()
