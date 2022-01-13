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
"""A view revealing when entries in the commitment from supervision metrics are not
also included in the admission metrics."""

from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_VIEW_NAME = (
    "incarceration_commitments_subset_of_admissions"
)

INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_DESCRIPTION = """A view revealing when entries in the commitment from supervision metrics are not
also included in the admission metrics."""

INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_QUERY_TEMPLATE = """
    /*{description}*/
    (SELECT DISTINCT person_id, admission_date, admission_reason, included_in_state_population
    FROM `{project_id}.{metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized`
    EXCEPT DISTINCT
    (
      SELECT DISTINCT person_id, admission_date, admission_reason, included_in_state_population
      FROM `{project_id}.{metrics_dataset}.most_recent_incarceration_admission_metrics_included_in_state_population_materialized`  
    ))
    UNION ALL
    (SELECT DISTINCT person_id, admission_date, admission_reason, included_in_state_population
    FROM `{project_id}.{metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_not_included_in_state_population_materialized`
    EXCEPT DISTINCT
    (
      SELECT DISTINCT person_id, admission_date, admission_reason, included_in_state_population
      FROM `{project_id}.{metrics_dataset}.most_recent_incarceration_admission_metrics_not_included_in_state_population_materialized`  
    ))
"""

INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_VIEW_NAME,
    view_query_template=INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_QUERY_TEMPLATE,
    description=INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_DESCRIPTION,
    metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_COMMITMENTS_SUBSET_OF_ADMISSIONS_VIEW_BUILDER.build_and_print()
