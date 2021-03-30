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
"""Supervision population by PO and day"""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_NAME = "supervision_population_by_po_by_day"

SUPERVISION_POPULATION_BY_PO_BY_DAY_DESCRIPTION = """
    Supervision population by PO by day
 """

SUPERVISION_POPULATION_BY_PO_BY_DAY_QUERY_TEMPLATE = """
    /*{description}*/
   SELECT
        state_code,
        date_of_supervision,
        supervising_district_external_id,
        supervising_officer_external_id,
        COUNT(DISTINCT(person_id)) AS people_under_supervision
    FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_population_metrics_materialized`,
    UNNEST ([supervising_district_external_id, 'ALL']) AS supervising_district_external_id,
    UNNEST ([supervising_officer_external_id, 'ALL']) AS supervising_officer_external_id
    # TODO(#6614): Generalize to be state agnostic.
    WHERE state_code = "US_ND"
    GROUP BY state_code, date_of_supervision, supervising_district_external_id, supervising_officer_external_id
    """

SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VITALS_REPORT_DATASET,
    view_id=SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_BY_PO_BY_DAY_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_BY_PO_BY_DAY_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    should_materialize=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_BY_PO_BY_DAY_VIEW_BUILDER.build_and_print()
