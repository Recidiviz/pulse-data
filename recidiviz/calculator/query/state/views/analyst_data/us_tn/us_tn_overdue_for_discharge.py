# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Creates a view to identify individuals overdue for discharge"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

US_TN_OVERDUE_FOR_DISCHARGE_VIEW_NAME = "us_tn_overdue_for_discharge"

US_TN_OVERDUE_FOR_DISCHARGE_VIEW_DESCRIPTION = (
    """Creates a view to identify individuals eligible overdue for discharge"""
)

US_TN_OVERDUE_FOR_DISCHARGE_QUERY_TEMPLATE = """
    SELECT person_name,
        person_external_id,
        officer_id,
        supervision_type,
        judicial_district,
        district,
        expiration_date AS exp_date,
    FROM `{project_id}.{analyst_dataset}.us_tn_compliant_reporting_logic_materialized`
    WHERE overdue_for_discharge_no_case_closure = 1 OR overdue_for_discharge_within_180 = 1 OR expiration_date IS NULL
"""

US_TN_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ANALYST_VIEWS_DATASET,
    view_id=US_TN_OVERDUE_FOR_DISCHARGE_VIEW_NAME,
    description=US_TN_OVERDUE_FOR_DISCHARGE_VIEW_DESCRIPTION,
    view_query_template=US_TN_OVERDUE_FOR_DISCHARGE_QUERY_TEMPLATE,
    project_id=GCP_PROJECT_STAGING,
    analyst_dataset=ANALYST_VIEWS_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        US_TN_OVERDUE_FOR_DISCHARGE_VIEW_BUILDER.build_and_print()
