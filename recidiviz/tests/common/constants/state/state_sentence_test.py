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
"""Tests for the constants defined in state_sentence"""

from recidiviz.common.constants.state.state_sentence import (
    _STATE_SENTENCE_STATUS_VALUE_TERMINATES,
    StateSentenceStatus,
)


def test_all_sentence_status_termination() -> None:
    """Tests that we have defined if a status terminates a sentence."""
    for status in StateSentenceStatus:
        assert status in _STATE_SENTENCE_STATUS_VALUE_TERMINATES
        _ = status.is_terminating_status
