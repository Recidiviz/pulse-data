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
"""Test gating ingest-related features."""
import re

from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.gating import (
    MANUAL_RAW_DATA_PRUNING_STATES,
    automatic_raw_data_pruning_enabled_for_state_and_instance,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

PRUNING_SCRIPT = "recidiviz/tools/multi_state_raw_data_pruning.sh"


def test_manual_raw_data_pruning_states() -> None:
    """Test that the manual raw data pruning states are correct."""
    with open(PRUNING_SCRIPT, encoding="utf-8") as f:
        state_declaration = one(
            l for l in f.readlines() if l.startswith("declare -a states")
        )
    assert MANUAL_RAW_DATA_PRUNING_STATES == set(
        map(StateCode, re.findall(r"US_\w\w", state_declaration))
    )

    for state_code in MANUAL_RAW_DATA_PRUNING_STATES:
        assert (
            automatic_raw_data_pruning_enabled_for_state_and_instance(
                state_code, DirectIngestInstance.PRIMARY
            )
            is False
        )
