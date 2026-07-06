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
"""Configuration of states that may report course completions through the Edovo API.

This is the single source of truth for which states the endpoint accepts and the
external-id type each state's ``person_external_id`` values are expected to match.
Onboarding a new state is a one-line addition here.
"""
from recidiviz.common.constants.state.external_id_types import US_CO_ADCNUMBER
from recidiviz.common.constants.states import StateCode

# Maps each supported state to the external-id type Edovo sends for it. For
# Colorado this is the DOC-facing ADC number (their export's
# ``facility_assigned_id`` / ``DOCNO``), not the internal EOMIS ``OFFENDERID``.
#
# Invariant: only admit a state whose id_type is stable and single-per-person.
# The capture-time no-double-credit guard dedups on (state, external id, course)
# (see EdovoCourseCompletion), so a state whose external ids are reassigned or
# duplicated per person would weaken that guard. See TODO(OBT-23210) for the
# authoritative downstream check.
SUPPORTED_STATES: dict[StateCode, str] = {
    StateCode.US_CO: US_CO_ADCNUMBER,
}
