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
"""Contains US_CA implementation of the StateSpecificNormalizationDelegate."""
from recidiviz.common.constants.state.external_id_types import US_CA_CDCNO, US_CA_DOC
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_external_ids_helpers import (
    select_most_recently_active_person_external_id,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)


class UsCaNormalizationDelegate(StateSpecificNormalizationDelegate):
    """US_CA implementation of the StateSpecificNormalizationDelegate."""

    def select_display_id_for_person_external_ids_of_type(
        self,
        state_code: StateCode,
        person_id: int,
        id_type: str,
        person_external_ids_of_type: list[StatePersonExternalId],
    ) -> StatePersonExternalId:
        if id_type == US_CA_DOC:
            return select_most_recently_active_person_external_id(
                person_external_ids_of_type, enforce_nonnull_id_active_from=False
            )
        if id_type == US_CA_CDCNO:
            return select_most_recently_active_person_external_id(
                person_external_ids_of_type, enforce_nonnull_id_active_from=False
            )

        raise ValueError(
            f"Unexpected id type {id_type} with multiple ids per person and no "
            f"is_current_display_id_for_type set at ingest time: "
            f"{person_external_ids_of_type}"
        )

    def extra_entities_generated_via_normalization(
        self, normalization_input_types: set[type[Entity]]
    ) -> set[type[NormalizedStateEntity]]:
        if entities.StateSupervisionPeriod in normalization_input_types:
            return {
                # We infer temporary custody incarceration periods from
                # incarceration periods
                normalized_entities.NormalizedStateIncarcerationPeriod
            }
        return set()
