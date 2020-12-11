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
"""Absconsion report recommendations by person by month."""
# pylint: disable=trailing-whitespace

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.calculator.query.state.views.po_report.violation_reports_query import violation_reports_query
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_VIEW_NAME = \
    'absconsion_reports_by_person_by_month'

ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_DESCRIPTION = """
    Absconsion report recommendations by person by month. 
"""

ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH violation_reports AS (
      {violation_reports_query}
    )
    SELECT DISTINCT
        state_code, year, month, person_id, officer_external_id,
        response_date AS absconsion_report_date
    FROM violation_reports
    WHERE violation_type = 'ABSCONDED'
    """

ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PO_REPORT_DATASET,
    view_id=ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_VIEW_NAME,
    should_materialize=True,
    view_query_template=ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_QUERY_TEMPLATE,
    description=ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_DESCRIPTION,
    violation_reports_query=violation_reports_query(
        state_dataset=dataset_config.STATE_BASE_DATASET,
        reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET
    )
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        ABSCONSION_REPORTS_BY_PERSON_BY_MONTH_VIEW_BUILDER.build_and_print()
