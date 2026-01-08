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
    FILES_EXEMPT_FROM_AUTOMATIC_RAW_DATA_PRUNING_BY_STATE,
    FILES_EXEMPT_FROM_MANUAL_RAW_DATA_PRUNING_BY_STATE,
    MANUAL_RAW_DATA_PRUNING_STATES,
    ManualRawDataPruningExemptionReason,
    file_tag_exempt_from_automatic_raw_data_pruning,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

PRUNING_SCRIPT = "recidiviz/tools/multi_state_raw_data_pruning.sh"


def test_manual_raw_data_pruning_states() -> None:
    """Test that the manual raw data pruning states are correct."""
    with open(PRUNING_SCRIPT, encoding="utf-8") as f:
        state_declaration = one(
            line for line in f.readlines() if line.startswith("declare -a states")
        )
    assert MANUAL_RAW_DATA_PRUNING_STATES == set(
        map(StateCode, re.findall(r"US_\w\w", state_declaration))
    )

    def _file_tag_is_manually_pruned(state_code: StateCode, file_tag: str) -> bool:
        return state_code in MANUAL_RAW_DATA_PRUNING_STATES and (
            state_code not in FILES_EXEMPT_FROM_MANUAL_RAW_DATA_PRUNING_BY_STATE
            or file_tag
            not in FILES_EXEMPT_FROM_MANUAL_RAW_DATA_PRUNING_BY_STATE[state_code]
        )

    for state_code in MANUAL_RAW_DATA_PRUNING_STATES:
        region_raw_file_config = get_region_raw_file_config(state_code.value)
        manually_pruned_fts_not_exempt_from_automatic_pruning = {
            file_tag
            for file_tag in region_raw_file_config.raw_file_tags
            if _file_tag_is_manually_pruned(state_code, file_tag)
            and not file_tag_exempt_from_automatic_raw_data_pruning(
                state_code,
                DirectIngestInstance.PRIMARY,
                file_tag,
            )
        }
        assert not manually_pruned_fts_not_exempt_from_automatic_pruning


def test_exempted_file_tags_valid() -> None:
    # Ensure deleted file tags are removed from exemption list
    for (
        state_code,
        manual_exemptions,
    ) in FILES_EXEMPT_FROM_MANUAL_RAW_DATA_PRUNING_BY_STATE.items():
        region_raw_file_config = get_region_raw_file_config(state_code.value)
        invalid_fts = manual_exemptions.keys() - region_raw_file_config.raw_file_tags
        assert not invalid_fts

    for (
        state_code,
        automatic_exemptions,
    ) in FILES_EXEMPT_FROM_AUTOMATIC_RAW_DATA_PRUNING_BY_STATE.items():
        region_raw_file_config = get_region_raw_file_config(state_code.value)
        invalid_fts = automatic_exemptions.keys() - region_raw_file_config.raw_file_tags
        assert not invalid_fts


def test_mixed_file_transfers_not_enabled_for_automatic_pruning() -> None:
    for (
        state_code,
        exemptions,
    ) in FILES_EXEMPT_FROM_MANUAL_RAW_DATA_PRUNING_BY_STATE.items():
        for file_tag, exemption_reason in exemptions.items():
            file_has_mixed_historical_and_incremental_transfer_history = (
                exemption_reason
                == ManualRawDataPruningExemptionReason.MIXED_INCREMENTAL_AND_HISTORICAL
            )
            file_is_not_exempt_from_automatic_pruning = (
                state_code not in FILES_EXEMPT_FROM_AUTOMATIC_RAW_DATA_PRUNING_BY_STATE
                or file_tag
                not in FILES_EXEMPT_FROM_AUTOMATIC_RAW_DATA_PRUNING_BY_STATE[state_code]
            )
            assert (
                file_has_mixed_historical_and_incremental_transfer_history
                and file_is_not_exempt_from_automatic_pruning
            ) is False, (
                f"File tag [{file_tag}] cannot be enabled for automatic raw data pruning while containing mixed historical and incremental transfers. "
                "Please mark file as historical and fully deprecate any incremental transfers "
                "or mark file as incremental."
            )
