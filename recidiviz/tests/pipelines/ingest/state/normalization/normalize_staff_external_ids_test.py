# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for normalize_staff_external_ids.py"""

import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateStaffExternalId
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateStaffExternalId,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_staff_external_ids import (
    get_normalized_staff_external_ids,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)


class DefaultDelegate(StateSpecificNormalizationDelegate):
    pass


# TODO(#60442): Add a PickAlphabeticallyHighestAndLowestDelegate (or equivalent) once delegate methods are implemented


class TestNormalizeStaffExternalIds(unittest.TestCase):
    """Tests for normalize_staff_external_ids.py"""

    def _make_unnormalized_external_id(
        self,
        *,
        external_id: str,
        id_type: str = "US_XX_ID_TYPE",
        is_current_display_id_for_type: bool | None = None,
        is_stable_id_for_type: bool | None = None,
    ) -> StateStaffExternalId:
        return StateStaffExternalId(
            staff_external_id_id=1,
            state_code=StateCode.US_XX.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=is_current_display_id_for_type,
            is_stable_id_for_type=is_stable_id_for_type,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

    def _make_normalized_external_id(
        self,
        *,
        external_id: str,
        id_type: str = "US_XX_ID_TYPE",
        # TODO(#60442): Change types of variables below to bool once fields are made non-nullable
        is_current_display_id_for_type: bool | None,
        is_stable_id_for_type: bool | None,
    ) -> NormalizedStateStaffExternalId:
        return NormalizedStateStaffExternalId(
            staff_external_id_id=1,
            state_code=StateCode.US_XX.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=is_current_display_id_for_type,
            is_stable_id_for_type=is_stable_id_for_type,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

    def test_single_id_sets_display_and_stable_true(self) -> None:
        sei = self._make_unnormalized_external_id(external_id="ID1")
        result = get_normalized_staff_external_ids(
            state_code=StateCode.US_XX,
            staff_id=12345,
            external_ids=[sei],
            # TODO(#60442): Replace with a real delegate once implemented
            delegate=DefaultDelegate(),
        )

        expected_result = [
            self._make_normalized_external_id(
                external_id="ID1",
                is_current_display_id_for_type=True,
                is_stable_id_for_type=True,
            )
        ]
        self.assertEqual(expected_result, result)

    def test_multiple_id_types_single_id_each(self) -> None:
        ids = [
            self._make_unnormalized_external_id(
                external_id="A", id_type="US_XX_ID_TYPE"
            ),
            self._make_unnormalized_external_id(
                external_id="B", id_type="US_XX_ID_TYPE_2"
            ),
        ]
        result = get_normalized_staff_external_ids(
            state_code=StateCode.US_XX,
            staff_id=12345,
            external_ids=ids,
            # TODO(#60442): Replace with a real delegate once implemented
            delegate=DefaultDelegate(),
        )
        expected_result = [
            self._make_normalized_external_id(
                external_id="A",
                id_type="US_XX_ID_TYPE",
                is_current_display_id_for_type=True,
                is_stable_id_for_type=True,
            ),
            self._make_normalized_external_id(
                external_id="B",
                id_type="US_XX_ID_TYPE_2",
                is_current_display_id_for_type=True,
                is_stable_id_for_type=True,
            ),
        ]
        self.assertEqual(expected_result, result)

    # TODO(#60442): Add test_multiple_ids_delegate_selects_display_and_stable once
    #   delegate methods are implemented.

    # TODO(#60442): Add test_multiple_id_types_delegates_each once delegate methods
    #   are implemented.

    # TODO(#60442): Add test_raises_if_some_ids_have_display_flag_and_others_do_not
    #   once delegate methods are implemented.

    # TODO(#60442): Add test_raises_if_some_ids_have_stable_flag_and_others_do_not
    #   once delegate methods are implemented.

    # TODO(#60442): Add test_all_ids_have_flags_preserved_default_delegate once
    #   delegate methods are implemented.
