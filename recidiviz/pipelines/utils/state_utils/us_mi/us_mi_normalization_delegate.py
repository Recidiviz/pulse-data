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
"""Contains US_MI implementation of the StateSpecificNormalizationDelegate."""
from recidiviz.common.constants.state.external_id_types import US_MI_DOC_BOOK
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.pipelines.ingest.state.normalization.normalize_external_ids_helpers import (
    select_alphabetically_highest_person_external_id,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)


class UsMiNormalizationDelegate(StateSpecificNormalizationDelegate):
    """US_MI implementation of the StateSpecificNormalizationDelegate."""

    def select_display_id_for_person_external_ids_of_type(
        self,
        state_code: StateCode,
        person_id: int,
        id_type: str,
        person_external_ids_of_type: list[StatePersonExternalId],
    ) -> StatePersonExternalId:
        if id_type == US_MI_DOC_BOOK:
            return select_alphabetically_highest_person_external_id(
                person_external_ids_of_type
            )

        raise ValueError(
            f"Unexpected id type {id_type} with multiple ids per person and no "
            f"is_current_display_id_for_type set at ingest time: "
            f"{person_external_ids_of_type}"
        )
