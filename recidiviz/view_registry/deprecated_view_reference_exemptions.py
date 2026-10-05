# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Lists views that are currently exempted from checks preventing references to
deprecated views.
"""

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.views.unioned_normalized_state_views import (
    normalized_state_view_address_for_entity_table,
)
from recidiviz.ingest.views.view_config import (
    get_view_builders_for_views_to_update as get_ingest_infra_view_builders,
)
from recidiviz.monitoring.platform_kpis.dataset_config import PLATFORM_KPIS_DATASET
from recidiviz.monitoring.platform_kpis.velocity.normalized_state_hydration_live_snapshot import (
    NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID,
)
from recidiviz.persistence.entity.activity.entities import (
    StateCharge,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.pipelines.ingest.activity.dataset_config import (
    normalized_state_dataset_for_state_code,
)
from recidiviz.utils.types import assert_subclass
from recidiviz.validation.views.state.primary_keys_unique_across_all_states import (
    PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER,
)


def _get_ingest_metadata_addresses_table(
    state_table_id: str,
) -> list[BigQueryAddress]:
    """Returns addresses for all ingest_metadata views that reference the given table."""
    addresses = []
    table_prefix = f"ingest_state_metadata__{state_table_id}__"
    for builder in get_ingest_infra_view_builders():
        if builder.address.table_id.startswith(table_prefix):
            addresses.append(builder.address)
    return addresses


_ALL_SCHEMA_TABLE_VIEWS = [
    BigQueryAddress(
        dataset_id=PLATFORM_KPIS_DATASET,
        table_id=NORMALIZED_STATE_HYDRATION_LIVE_SNAPSHOT_VIEW_ID,
    ),
    PRIMARY_KEYS_UNIQUE_ACROSS_ALL_STATES_VIEW_BUILDER.address,
]

# Maps deprecated view addresses to a dict of views that are allowed to reference
# them. The inner dict maps each exempted view address to a reason string explaining
# why the reference still exists.
SENTENCES_V1_DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {
    # TODO(#33402): Delete StateIncarcerationSentence once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(
        StateIncarcerationSentence.get_table_id()
    ): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when "
                "state_incarceration_sentence is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(
                    StateIncarcerationSentence.get_table_id()
                )
            )
        },
    },
    # TODO(#33402): Delete StateSupervisionSentence once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(
        StateSupervisionSentence.get_table_id()
    ): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when "
                "state_supervision_sentence is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(
                    StateSupervisionSentence.get_table_id()
                )
            )
        },
    },
    # TODO(#33402): Delete StateCharge once all states are migrated
    # to v2 infra
    normalized_state_view_address_for_entity_table(StateCharge.get_table_id()): {
        # Boilerplate views that will be deleted automatically
        **{
            view_address: (
                "TODO(#33402): This view will be deleted when state_charge is deleted"
            )
            for view_address in (
                _ALL_SCHEMA_TABLE_VIEWS
                + _get_ingest_metadata_addresses_table(StateCharge.get_table_id())
            )
        },
    },
    # State-specific v1 sentences table reference exemptions
    **{
        BigQueryAddress(
            dataset_id=normalized_state_dataset_for_state_code(state_code),
            table_id=assert_subclass(entity_cls, Entity).get_table_id(),
        ): (
            {
                normalized_state_view_address_for_entity_table(
                    assert_subclass(entity_cls, Entity).get_table_id()
                ): (
                    f"TODO(#33402): This view will be deleted when "
                    f"{assert_subclass(entity_cls, Entity).get_table_id()} is deleted"
                ),
            }
        )
        for state_code in get_existing_direct_ingest_states()
        if state_code != StateCode.US_OZ
        for entity_cls in [
            StateIncarcerationSentence,
            StateSupervisionSentence,
            StateCharge,
        ]
    },
}


# Maps deprecated view addresses to a dict of views that are allowed to reference
# them. The inner dict maps each exempted view address to a reason string explaining
# why the reference still exists.
DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS: dict[
    BigQueryAddress, dict[BigQueryAddress, str]
] = {**SENTENCES_V1_DEPRECATED_VIEWS_AND_USAGE_EXEMPTIONS}
