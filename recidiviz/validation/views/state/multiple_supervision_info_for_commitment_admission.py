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
"""A view revealing when a commitment from supervision admission is associated with
more than one supervising_officer_external_id,
level_1_supervision_location_external_id,
or level_2_supervision_location_external_id.

A failure indicates a bug in the commitment from supervision identification
calculation logic.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state import dataset_config as state_dataset_config
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.validation.views import dataset_config

MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_VIEW_NAME = (
    "multiple_supervision_info_for_commitment_admission"
)

MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_DESCRIPTION = """A view revealing when a commitment from supervision admission is associated with
more than one supervising_officer_external_id,
level_1_supervision_location_external_id,
or level_2_supervision_location_external_id.

A failure indicates a bug in the commitment from supervision identification 
calculation logic."""

MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_QUERY_TEMPLATE = """
    /*{description}*/
SELECT
    state_code as region_code,
    person_id,
    admission_date,
    COUNT(DISTINCT(supervising_officer_external_id)) as num_officers,
    COUNT(DISTINCT(level_1_supervision_location_external_id)) as num_level_1,
    COUNT(DISTINCT(level_2_supervision_location_external_id)) as num_level_2
FROM `{project_id}.{metrics_dataset}.most_recent_incarceration_commitment_from_supervision_metrics_included_in_state_population_materialized`
GROUP BY 1,2,3
HAVING num_officers > 1
OR num_level_1 > 1
OR num_level_2 > 1
"""

MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.VIEWS_DATASET,
    view_id=MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_VIEW_NAME,
    view_query_template=MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_QUERY_TEMPLATE,
    description=MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_DESCRIPTION,
    metrics_dataset=state_dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        MULTIPLE_SUPERVISION_INFO_FOR_COMMITMENT_ADMISSION_VIEW_BUILDER.build_and_print()
