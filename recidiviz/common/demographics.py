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
"""Person demographic enums shared across pipelines and tenants.

TODO(#73351): Replace these re-exports with real enum definitions once
StateGender and StateRace are migrated out of state_person.py.
"""
from recidiviz.common.constants.state.state_person import StateGender as Gender
from recidiviz.common.constants.state.state_person import StateRace as Race

__all__ = ["Gender", "Race"]
