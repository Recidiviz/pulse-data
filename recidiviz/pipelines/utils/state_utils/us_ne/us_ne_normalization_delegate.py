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
"""Contains US_NE implementation of the StateSpecificNormalizationDelegate."""
from datetime import datetime

from recidiviz.common.constants.state.external_id_types import US_NE_ID_NBR
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)
from recidiviz.utils.types import assert_type


class UsNeNormalizationDelegate(StateSpecificNormalizationDelegate):
    """US_NE implementation of the StateSpecificNormalizationDelegate."""

    def select_display_id_for_person_external_ids_of_type(
        self,
        state_code: StateCode,
        person_id: int,
        id_type: str,
        person_external_ids_of_type: list[StatePersonExternalId],
    ) -> StatePersonExternalId:
        if id_type == US_NE_ID_NBR:
            # In NE we do our best to pick a single is_current_display_id_for_type for
            # each person, but we can't always do this reasonably before we make it to
            # normalization because there may be inmate number links that span
            # internalId values and entity merging ends up merging two clusters of ids
            # together, each with one id that is_current_display_id_for_type=True. Here
            # we pick the id where is_current_display_id_for_type=True that was active
            # most recently.
            ids_marked_as_current_display = [
                pei
                for pei in person_external_ids_of_type
                if pei.is_current_display_id_for_type
            ]

            return list(
                reversed(
                    sorted(
                        ids_marked_as_current_display,
                        key=lambda pei: assert_type(
                            pei.id_active_from_datetime, datetime
                        ),
                    )
                )
            )[0]

        raise ValueError(
            f"Unexpected id type {id_type} with multiple ids per person and no "
            f"is_current_display_id_for_type set at ingest time: "
            f"{person_external_ids_of_type}"
        )
