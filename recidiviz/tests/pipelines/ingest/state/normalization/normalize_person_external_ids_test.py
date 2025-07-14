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
"""Tests for normalize_person_external_ids.py"""

import unittest
from unittest.mock import patch

import mock

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StatePersonExternalId
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePersonExternalId,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_external_ids_helpers import (
    select_alphabetically_highest_person_external_id,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_person_external_ids import (
    get_normalized_person_external_ids,
)
from recidiviz.pipelines.ingest.state.normalization.state_specific_normalization_delegate import (
    StateSpecificNormalizationDelegate,
)


class DefaultDelegate(StateSpecificNormalizationDelegate):
    pass


class PickAlphabeticallyHighestDelegate(StateSpecificNormalizationDelegate):
    def select_display_id_for_person_external_ids_of_type(
        self,
        state_code: StateCode,
        person_id: int,
        id_type: str,
        person_external_ids_of_type: list[StatePersonExternalId],
    ) -> StatePersonExternalId:
        return select_alphabetically_highest_person_external_id(
            person_external_ids_of_type
        )


class TestNormalizePersonExternalIds(unittest.TestCase):
    """Tests for normalize_person_external_ids.py"""

    def _make_unnormalized_external_id(
        self,
        *,
        external_id: str,
        id_type: str = "US_XX_ID_TYPE",
        is_current_display_id_for_type: bool | None = None,
    ) -> StatePersonExternalId:
        return StatePersonExternalId(
            person_external_id_id=1,
            state_code=StateCode.US_XX.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=is_current_display_id_for_type,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

    def _make_normalized_external_id(
        self,
        *,
        external_id: str,
        id_type: str = "US_XX_ID_TYPE",
        is_current_display_id_for_type: bool,
        # TODO(#45291): Make non-optional and remove default once we expect
        #  is_stable_id_for_type to always be hydrated.
        is_stable_id_for_type: bool | None = None,
    ) -> NormalizedStatePersonExternalId:
        return NormalizedStatePersonExternalId(
            person_external_id_id=1,
            state_code=StateCode.US_XX.value,
            external_id=external_id,
            id_type=id_type,
            is_current_display_id_for_type=is_current_display_id_for_type,
            is_stable_id_for_type=is_stable_id_for_type,
            id_active_from_datetime=None,
            id_active_to_datetime=None,
        )

    def test_single_id_sets_is_current_display_true(self) -> None:
        pei = self._make_unnormalized_external_id(external_id="ID1")
        result = get_normalized_person_external_ids(
            state_code=StateCode.US_XX,
            person_id=12345,
            external_ids=[pei],
            delegate=PickAlphabeticallyHighestDelegate(),
        )

        expected_result = [
            self._make_normalized_external_id(
                external_id="ID1", is_current_display_id_for_type=True
            )
        ]
        self.assertEqual(expected_result, result)

    def test_multiple_ids_delegate_selects_display(self) -> None:
        ids = [
            self._make_unnormalized_external_id(external_id="ID1"),
            self._make_unnormalized_external_id(external_id="ID3"),
            self._make_unnormalized_external_id(external_id="ID2"),
        ]
        result = get_normalized_person_external_ids(
            state_code=StateCode.US_XX,
            person_id=12345,
            external_ids=ids,
            delegate=PickAlphabeticallyHighestDelegate(),
        )
        expected_result = [
            self._make_normalized_external_id(
                external_id="ID1", is_current_display_id_for_type=False
            ),
            self._make_normalized_external_id(
                external_id="ID2", is_current_display_id_for_type=False
            ),
            # This ID selected
            self._make_normalized_external_id(
                external_id="ID3", is_current_display_id_for_type=True
            ),
        ]
        self.assertEqual(expected_result, result)

    def test_multiple_id_types_delegates_each(self) -> None:
        ids = [
            self._make_unnormalized_external_id(
                external_id="A", id_type="US_XX_ID_TYPE"
            ),
            self._make_unnormalized_external_id(
                external_id="Y", id_type="US_XX_ID_TYPE_2"
            ),
            self._make_unnormalized_external_id(
                external_id="B", id_type="US_XX_ID_TYPE"
            ),
            self._make_unnormalized_external_id(
                external_id="X", id_type="US_XX_ID_TYPE_2"
            ),
        ]
        result = get_normalized_person_external_ids(
            state_code=StateCode.US_XX,
            person_id=12345,
            external_ids=ids,
            delegate=PickAlphabeticallyHighestDelegate(),
        )
        expected_result = [
            self._make_normalized_external_id(
                external_id="A",
                id_type="US_XX_ID_TYPE",
                is_current_display_id_for_type=False,
            ),
            self._make_normalized_external_id(
                external_id="B",
                id_type="US_XX_ID_TYPE",
                is_current_display_id_for_type=True,
            ),
            self._make_normalized_external_id(
                external_id="X",
                id_type="US_XX_ID_TYPE_2",
                is_current_display_id_for_type=False,
            ),
            self._make_normalized_external_id(
                external_id="Y",
                id_type="US_XX_ID_TYPE_2",
                is_current_display_id_for_type=True,
            ),
        ]
        self.assertEqual(expected_result, result)

    @patch(
        "recidiviz.pipelines.ingest.state.normalization."
        "state_specific_normalization_delegate."
        "person_external_id_types_with_allowed_multiples_per_person"
    )
    def test_raises_if_some_ids_have_display_flag_and_others_do_not_with_default_delegate(
        self, mock_allowed_types_with_multiples: mock.MagicMock
    ) -> None:
        mock_allowed_types_with_multiples.return_value = {"US_XX_ID_TYPE"}
        ids = [
            self._make_unnormalized_external_id(
                external_id="ID1", is_current_display_id_for_type=True
            ),
            self._make_unnormalized_external_id(
                external_id="ID2", is_current_display_id_for_type=None
            ),
        ]
        with self.assertRaisesRegex(
            ValueError,
            r"If you are going to rely on directly hydrated "
            r"is_current_display_id_for_type values, you must hydrate it for ALL "
            r"external ids of this type \(US_XX_ID_TYPE\).",
        ):
            get_normalized_person_external_ids(
                state_code=StateCode.US_XX,
                person_id=12345,
                external_ids=ids,
                delegate=DefaultDelegate(),
            )

    @patch(
        "recidiviz.pipelines.ingest.state.normalization."
        "state_specific_normalization_delegate."
        "person_external_id_types_with_allowed_multiples_per_person"
    )
    def test_all_ids_have_display_flags_preserved_default_delegate(
        self, mock_allowed_types_with_multiples: mock.MagicMock
    ) -> None:
        mock_allowed_types_with_multiples.return_value = {"US_XX_ID_TYPE"}

        ids = [
            self._make_unnormalized_external_id(
                external_id="ID1", is_current_display_id_for_type=True
            ),
            self._make_unnormalized_external_id(
                external_id="ID2", is_current_display_id_for_type=False
            ),
        ]
        result = get_normalized_person_external_ids(
            state_code=StateCode.US_XX,
            person_id=12345,
            external_ids=ids,
            delegate=DefaultDelegate(),
        )
        expected_result = [
            self._make_normalized_external_id(
                external_id="ID1", is_current_display_id_for_type=True
            ),
            self._make_normalized_external_id(
                external_id="ID2", is_current_display_id_for_type=False
            ),
        ]
        self.assertEqual(expected_result, result)
