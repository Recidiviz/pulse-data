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
"""Creates the view builder that copies most_recent_program_participation_metrics_materialized."""

from typing import List

from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query.externally_shared_views.dataset_config import (
    EXTERNALLY_SHARED_VIEWS_DATASET,
    USDR_FTR_DATASET,
    USDR_STATE_PERMISSIONS,
)
from recidiviz.calculator.query.state.dataset_config import (
    DATAFLOW_METRICS_MATERIALIZED_DATASET,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# name of destination table
PROGRAM_PARTICIPATION_VIEW_NAME = "ftr_participation"

# description of the view
PROGRAM_PARTICIPATION_VIEW_DESCRIPTION = "Participation data for ND's FTR program"

# query template
PROGRAM_PARTICIPATION_QUERY_TEMPLATE = """
    /*{description}*/
    SELECT
        state_code,
        person_id,
        date_of_participation,
        metric_type,
        program_id,
        program_location_id,
        supervision_type,
        is_first_day_in_program,
    FROM
        `{staging_only_project}.{origin_dataset_id}.most_recent_program_participation_metrics_materialized`
    WHERE
        state_code IN ({allowed_states})
    ORDER BY
        state_code, 
        person_id,
        date_of_participation DESC
"""

# Iteratively construct builders that reference the same view. Each unique materialized
# table needs its own `view_id`, `dataset_id` (inside `materialized_address_override`),
# and `allowed_states`.

# batch destination-specific parameters:
# view_prefix, destination_dataset_id, allowed_states
EXTERNAL_ID_VIEW_CONFIG = [
    ("usdr_", USDR_FTR_DATASET, USDR_STATE_PERMISSIONS),
]

# init object to hold view builders
FTR_PROGRAM_PARTICIPATION_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = []

# iteratively add each builder to list
for view_prefix, destination_dataset_id, allowed_states in EXTERNAL_ID_VIEW_CONFIG:
    FTR_PROGRAM_PARTICIPATION_VIEW_BUILDERS.append(
        SimpleBigQueryViewBuilder(
            dataset_id=EXTERNALLY_SHARED_VIEWS_DATASET,
            view_id=view_prefix + PROGRAM_PARTICIPATION_VIEW_NAME,
            view_query_template=PROGRAM_PARTICIPATION_QUERY_TEMPLATE,
            description=PROGRAM_PARTICIPATION_VIEW_DESCRIPTION,
            staging_only_project=GCP_PROJECT_STAGING,
            origin_dataset_id=DATAFLOW_METRICS_MATERIALIZED_DATASET,
            allowed_states=str(allowed_states)[1:-1],
            should_materialize=True,
            materialized_address_override=BigQueryAddress(
                dataset_id=destination_dataset_id,
                table_id=PROGRAM_PARTICIPATION_VIEW_NAME,
            ),
        )
    )

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in FTR_PROGRAM_PARTICIPATION_VIEW_BUILDERS:
            view_builder.build_and_print()
