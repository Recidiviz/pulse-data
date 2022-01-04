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
"""Violation data

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.po_report.supervision_compliance_by_person_by_month
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIOLATIONS_VIEW_NAME = "violations"

VIOLATIONS_DESCRIPTION = """
"""

VIOLATIONS_QUERY_TEMPLATE = """
SELECT state_code,
 person_external_id,
 revocation_violation_type,
 revocation_report_date
 FROM `{project_id}.po_report_views.report_data_by_person_by_month_materialized`
WHERE DATE(year, month, 1) = DATE_SUB(
    DATE(EXTRACT(YEAR FROM CURRENT_DATE('US/Eastern')), EXTRACT(MONTH FROM CURRENT_DATE('US/Eastern')), 1),
    INTERVAL 1 MONTH
);
"""

VIOLATIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=VIOLATIONS_VIEW_NAME,
    should_materialize=True,
    view_query_template=VIOLATIONS_QUERY_TEMPLATE,
    description=VIOLATIONS_DESCRIPTION,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIOLATIONS_VIEW_BUILDER.build_and_print()
