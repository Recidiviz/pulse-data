# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Dataset configuration for case triage experimentation platform."""

CEMENTING_THE_CHANGE_DATASET: str = "cementing_the_change"
EXTERNALLY_SHARED_VIEWS_DATASET: str = "externally_shared_views"
USDR_FTR_DATASET: str = "usdr_ftr"

# state access permissions
all_ingested_states = ["US_ID", "US_MO", "US_ND", "US_PA"]
RISC_STATE_PERMISSIONS: list = all_ingested_states
USDR_STATE_PERMISSIONS: list = all_ingested_states
