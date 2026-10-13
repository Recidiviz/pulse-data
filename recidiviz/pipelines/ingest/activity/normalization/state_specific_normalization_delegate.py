# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Interface for state-specific decisions about running normalization logic in ingest.
"""
import abc

from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.external_id_selection import (
    has_shared_selection_strategy,
    select_display_external_id,
    select_stable_external_id,
)
from recidiviz.persistence.entity.activity.entities import (
    StatePersonExternalId,
    StateStaffExternalId,
)
from recidiviz.persistence.entity.activity.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.base_entity import Entity, StateExternalIdEntityT
from recidiviz.pipelines.ingest.activity.normalization.normalize_external_ids_helpers import (
    select_single_external_id_with_is_current_display_id,
    select_single_external_id_with_is_stable_id,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)


class StateSpecificNormalizationDelegate(abc.ABC, StateSpecificDelegate):
    """Interface for state-specific decisions about running normalization logic in
    ingest.
    """

    def select_display_id_for_person_external_ids_of_type(
        self,
        state_code: StateCode,
        id_type: str,
        person_external_ids_of_type: list[StatePersonExternalId],
    ) -> StatePersonExternalId:
        """Given a list of external_ids of the given |id_type|, returns the one that
        should be used as the "display" external id in products that display person
        external_ids of this type.

        States whose selection depends on id-active dates or ingest-set flags override
        this; every other state defers to the shared selection module.
        """
        return self._select_display_external_id_of_type(
            state_code=state_code,
            id_type=id_type,
            external_ids_of_type=person_external_ids_of_type,
        )

    def select_stable_id_for_person_external_ids_of_type(
        self,
        state_code: StateCode,
        id_type: str,
        person_external_ids_of_type: list[StatePersonExternalId],
    ) -> StatePersonExternalId:
        """Given a list of external_ids of the given |id_type|, returns the one that
        should be used as the "stable" external id in products that need to persist an
        external id for a person across deploy cycles.

        States whose selection depends on id-active dates or ingest-set flags override
        this; every other state defers to the shared selection module.
        """
        return self._select_stable_external_id_of_type(
            state_code=state_code,
            id_type=id_type,
            external_ids_of_type=person_external_ids_of_type,
        )

    def select_display_id_for_staff_external_ids_of_type(
        self,
        state_code: StateCode,
        id_type: str,
        staff_external_ids_of_type: list[StateStaffExternalId],
    ) -> StateStaffExternalId:
        """Given a list of external_ids of the given |id_type|, returns the one that
        should be used as the "display" external id in products that display staff
        external_ids of this type.

        States whose selection depends on id-active dates or ingest-set flags override
        this; every other state defers to the shared selection module.
        """
        return self._select_display_external_id_of_type(
            state_code=state_code,
            id_type=id_type,
            external_ids_of_type=staff_external_ids_of_type,
        )

    def select_stable_id_for_staff_external_ids_of_type(
        self,
        state_code: StateCode,
        id_type: str,
        staff_external_ids_of_type: list[StateStaffExternalId],
    ) -> StateStaffExternalId:
        """Given a list of external_ids of the given |id_type|, returns the one that
        should be used as the "stable" external id in products that need to persist an
        external id for a staff member across deploy cycles.

        States whose selection depends on id-active dates or ingest-set flags override
        this; every other state defers to the shared selection module.
        """
        return self._select_stable_external_id_of_type(
            state_code=state_code,
            id_type=id_type,
            external_ids_of_type=staff_external_ids_of_type,
        )

    def _select_display_external_id_of_type(
        self,
        *,
        state_code: StateCode,
        id_type: str,
        external_ids_of_type: list[StateExternalIdEntityT],
    ) -> StateExternalIdEntityT:
        """Returns the external id of the given type to use as the display id.

        When ingest hydrated a display flag, uses the id it marked; otherwise defers
        to the shared (tenant, id_type)-keyed selection in
        recidiviz.common.external_id_selection, matching the chosen id string back to
        its entity.
        """
        if len(external_ids_of_type) <= 1:
            raise ValueError(
                f"We do not expect external id selection to be called if there are fewer "
                f"than 2 ids of a given type. Found [{len(external_ids_of_type)}] ids."
            )
        tenant = Tenant.from_state_code(state_code)
        if any(
            ei.is_current_display_id_for_type is not None for ei in external_ids_of_type
        ):
            _raise_if_shared_strategy_also_exists(tenant=tenant, id_type=id_type)
            return select_single_external_id_with_is_current_display_id(
                external_ids_of_type
            )
        selected_external_id = select_display_external_id(
            tenant=tenant,
            id_type=id_type,
            external_ids=[ei.external_id for ei in external_ids_of_type],
        )
        return one(
            ei for ei in external_ids_of_type if ei.external_id == selected_external_id
        )

    def _select_stable_external_id_of_type(
        self,
        *,
        state_code: StateCode,
        id_type: str,
        external_ids_of_type: list[StateExternalIdEntityT],
    ) -> StateExternalIdEntityT:
        """Returns the external id of the given type to use as the stable id.

        When ingest hydrated a stable flag, uses the id it marked; otherwise defers
        to the shared (tenant, id_type)-keyed selection in
        recidiviz.common.external_id_selection, matching the chosen id string back to
        its entity.
        """
        if len(external_ids_of_type) <= 1:
            raise ValueError(
                f"We do not expect external id selection to be called if there are fewer "
                f"than 2 ids of a given type. Found [{len(external_ids_of_type)}] ids."
            )
        tenant = Tenant.from_state_code(state_code)
        if any(ei.is_stable_id_for_type is not None for ei in external_ids_of_type):
            _raise_if_shared_strategy_also_exists(tenant=tenant, id_type=id_type)
            return select_single_external_id_with_is_stable_id(external_ids_of_type)
        selected_external_id = select_stable_external_id(
            tenant=tenant,
            id_type=id_type,
            external_ids=[ei.external_id for ei in external_ids_of_type],
        )
        return one(
            ei for ei in external_ids_of_type if ei.external_id == selected_external_id
        )

    def extra_entities_generated_via_normalization(
        self,
        normalization_input_types: set[type[Entity]],  # pylint: disable=unused-argument
    ) -> set[type[NormalizedStateEntity]]:
        """Returns a list of Normalized* entity types that are *only* produced by
        the normalization step of ingest pipelines and are not produced directly by
        any ingest views / mappings.

        Args:
            normalization_input_types: The entity types that are produced by the
              ingest mappings step and are inputs to the normalization step.
        """
        return set()


def _raise_if_shared_strategy_also_exists(*, tenant: Tenant, id_type: str) -> None:
    """Raises if (tenant, id_type) is selected both by an ingest-set flag and by the shared
    selection module. The two must be mutually exclusive, or the activity pipeline (which
    honors the flag) and the Identity Service (which runs the shared strategy) could pick
    different ids.
    """
    if has_shared_selection_strategy(tenant=tenant, id_type=id_type):
        raise ValueError(
            f"id_type [{id_type}] for tenant [{tenant.value}] has both an ingest-set "
            f"display/stable flag and a shared selection strategy. These are mutually "
            f"exclusive: remove the entry from _SELECTION_STRATEGY_BY_TENANT_AND_ID_TYPE "
            f"in recidiviz.common.external_id_selection, or stop hydrating the flag at "
            f"ingest."
        )
