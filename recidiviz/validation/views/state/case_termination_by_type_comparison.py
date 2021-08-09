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
"""
This view looks at case termination by type and compares the different ways of
computing them.

The PO monthly reports look at violations whereas case_terminations_by_type_by_month
look at terminations, so we want to confirm that (when computed) we see more
violations than terminations.
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_NAME = "case_termination_by_type_comparison"

CASE_TERMINATIONS_BY_TYPE_COMPARISON_DESCRIPTION = (
    """Supervision case termination count comparison per month"""
)

# TODO(#4948): This validation currently ignores rows that are NULL. We should later fix this
# to ensure that the only rows that are NULL are the ones that we expect (i.e. ones that don't
# feed into the table just yet).
CASE_TERMINATIONS_BY_TYPE_COMPARISON_QUERY_TEMPLATE = """
    /*{description}*/
    WITH by_month as (
      SELECT
        state_code as region_code, year, month,
        SUM(absconsion) as absconsion_count, SUM(discharge) as discharge_count
      FROM `{project_id}.{view_dataset}.case_terminations_by_type_by_month`
      WHERE (district != 'ALL' OR district IS NULL) AND supervision_type = 'ALL'
      GROUP BY region_code, year, month
    ),
    from_po_report as (
      SELECT state_code as region_code, year, month,
      COUNT(DISTINCT IF(successful_completion_date IS NOT NULL, person_id, NULL)) as discharge_count,
      COUNT(DISTINCT IF(absconsion_report_date IS NOT NULL, person_id, NULL)) as absconsion_count
      FROM `{project_id}.{po_report_dataset}.report_data_by_person_by_month_materialized`
      GROUP BY region_code, year, month
    )
    SELECT region_code, year, month,
       by_month.absconsion_count as absconsions_by_month,
       from_po_report.absconsion_count as absconsions_from_po_report,

       by_month.discharge_count as discharges_by_month,
       from_po_report.discharge_count as discharges_from_po_report
    FROM by_month
    FULL OUTER JOIN from_po_report USING (region_code, year, month)
    /*
    The by_month table counts terminations and the from_po_report table counts violations.
    There should always be more violations than terminations, so validate that we don't see any
    instances where that isn't the case.

    We are not coalescing when the relevant counts are NULL because as of right now we are not
    computing these metrics across all states.
    */
    WHERE by_month.absconsion_count > from_po_report.absconsion_count
      OR by_month.discharge_count > from_po_report.discharge_count
    ORDER BY region_code, year, month
"""

CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_NAME,
    view_query_template=CASE_TERMINATIONS_BY_TYPE_COMPARISON_QUERY_TEMPLATE,
    description=CASE_TERMINATIONS_BY_TYPE_COMPARISON_DESCRIPTION,
    view_dataset=state_dataset_config.DASHBOARD_VIEWS_DATASET,
    po_report_dataset=state_dataset_config.PO_REPORT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        CASE_TERMINATIONS_BY_TYPE_COMPARISON_VIEW_BUILDER.build_and_print()
