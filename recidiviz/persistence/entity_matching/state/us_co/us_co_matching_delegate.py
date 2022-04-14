# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Contains logic for US_CO specific entity matching overrides."""
from recidiviz.common.constants.states import StateCode
from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.entity_matching.state.base_state_matching_delegate import (
    BaseStateMatchingDelegate,
)


class UsCoMatchingDelegate(BaseStateMatchingDelegate):
    """Class that contains matching logic specific to US_CO."""

    def __init__(self, ingest_metadata: IngestMetadata):
        super().__init__(StateCode.US_CO.value.lower(), ingest_metadata)
