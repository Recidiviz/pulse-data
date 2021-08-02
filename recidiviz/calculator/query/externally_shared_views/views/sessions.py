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
"""Creates the view builders for specified Sessions tables in analyst_data."""

from typing import List, Tuple

from recidiviz.big_query.big_query_view import (
    BigQueryAddress,
    SimpleBigQueryViewBuilder,
)
from recidiviz.calculator.query.externally_shared_views.dataset_config import (
    CSG_CONFIG,
    EXTERNALLY_SHARED_VIEWS_DATASET,
)
from recidiviz.calculator.query.state.views.analyst_data.compartment_sentences import (
    COMPARTMENT_SENTENCES_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.compartment_sessions import (
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.reincarceration_sessions_from_sessions import (
    REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.revocation_sessions import (
    REVOCATION_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.supervision_super_sessions import (
    SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER,
)
from recidiviz.calculator.query.state.views.analyst_data.violations_sessions import (
    VIOLATIONS_SESSIONS_VIEW_BUILDER,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# list of source view builders, which will provide view names and descriptions
SESSIONS_SOURCE_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    COMPARTMENT_SENTENCES_VIEW_BUILDER,
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
    REINCARCERATION_SESSIONS_FROM_SESSIONS_VIEW_BUILDER,
    REVOCATION_SESSIONS_VIEW_BUILDER,
    SUPERVISION_SUPER_SESSIONS_VIEW_BUILDER,
    VIOLATIONS_SESSIONS_VIEW_BUILDER,
]

# query template
SESSIONS_QUERY_TEMPLATE: str = """
    /*{description}*/
    SELECT *
    FROM
        `{project_id}.{origin_dataset_id}.{origin_table_id}`
    WHERE
        state_code IN {allowed_states}
"""

# Iteratively construct builders that reference the same views. Each unique materialized
# table needs its own `view_id`, `dataset_id` (inside `materialized_address_override`),
# and `allowed_states`.

# batch partner-specific parameters
# view_prefix, destination_dataset_id, allowed_states
PARTNER_SHARED_SESSIONS_CONFIG: List[Tuple[str, str, Tuple[str, ...]]] = [
    CSG_CONFIG,
]

# init object to hold view builders
PARTNER_SHARED_SESSIONS_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = []

# iteratively add each builder to list
# first, iterate over dataset destination/state permissions
for (
    view_prefix,
    destination_dataset_id,
    allowed_states,
) in PARTNER_SHARED_SESSIONS_CONFIG:

    # second, iterate over Sessions tables
    for view_builder in SESSIONS_SOURCE_VIEW_BUILDERS:

        # ensure view_builder has a defined materialized address
        if view_builder.materialized_address is None:
            raise ValueError(
                f"Materialized address not defined for view: {view_builder.view_id}"
            )

        # append each unique view to the builder
        PARTNER_SHARED_SESSIONS_VIEW_BUILDERS.append(
            SimpleBigQueryViewBuilder(
                dataset_id=EXTERNALLY_SHARED_VIEWS_DATASET,
                view_id=view_prefix + view_builder.view_id,
                view_query_template=SESSIONS_QUERY_TEMPLATE,
                description=view_builder.description,
                origin_dataset_id=view_builder.dataset_id,
                origin_table_id=view_builder.materialized_address.table_id,
                allowed_states=str(allowed_states),
                should_materialize=True,
                materialized_address_override=BigQueryAddress(
                    dataset_id=destination_dataset_id,
                    table_id=view_builder.materialized_address.table_id,
                ),
            )
        )

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for view_builder in PARTNER_SHARED_SESSIONS_VIEW_BUILDERS:
            view_builder.build_and_print()
