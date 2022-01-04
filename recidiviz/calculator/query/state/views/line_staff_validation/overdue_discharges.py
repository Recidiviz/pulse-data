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
"""Overdue discharges

To generate the BQ view, run:
    python -m recidiviz.calculator.query.state.views.line_staff_validation.overdue_discharges
"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

OVERDUE_DISCHARGES_VIEW_NAME = "overdue_discharges"

OVERDUE_DISCHARGES_DESCRIPTION = """
"""

OVERDUE_DISCHARGES_QUERY_TEMPLATE = """
SELECT
    projected_discharges.state_code,
    projected_discharges.person_external_id,
    projected_discharges.projected_end_date,
    projected_discharges.projected_end_date <= CURRENT_DATE('US/Eastern') AS is_overdue
FROM `{project_id}.{analyst_dataset}.projected_discharges_materialized` projected_discharges
ORDER BY person_external_id, projected_end_date;
"""

OVERDUE_DISCHARGES_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.LINESTAFF_DATA_VALIDATION,
    view_id=OVERDUE_DISCHARGES_VIEW_NAME,
    should_materialize=True,
    view_query_template=OVERDUE_DISCHARGES_QUERY_TEMPLATE,
    description=OVERDUE_DISCHARGES_DESCRIPTION,
    analyst_dataset=dataset_config.ANALYST_VIEWS_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        OVERDUE_DISCHARGES_VIEW_BUILDER.build_and_print()
