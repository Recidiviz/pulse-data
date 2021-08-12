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
"""Supervisees due for release by PO by day."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#8389) Templatize vital base-view generation

SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_NAME = (
    "supervision_population_due_for_release_by_po_by_day"
)

SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_DESCRIPTION = """
Supervision population due for release by PO by day
"""

SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT 
        state_code,
        date_of_supervision,
        IFNULL(supervising_officer_external_id, 'UNKNOWN') as supervising_officer_external_id,
        district_id,
        district_name,
        due_for_release_count,
        people_under_supervision AS total_under_supervision,
        SAFE_DIVIDE((people_under_supervision - due_for_release_count), people_under_supervision) * 100 AS timely_discharge,
    FROM `{project_id}.{vitals_views_dataset}.supervision_population_by_po_by_day_materialized`    
    """

SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_DESCRIPTION,
    vitals_views_dataset=dataset_config.VITALS_REPORT_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_DUE_FOR_RELEASE_BY_PO_BY_DAY_VIEW_BUILDER.build_and_print()
