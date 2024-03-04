# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Listing out exemptions for the state ingest pipeline."""

from recidiviz.common.constants.states import StateCode

INGEST_VIEW_TREE_MERGER_ERROR_EXEMPTIONS = {
    # TODO(#24299) Remove this exemption once conflicts no longer appear.
    StateCode.US_PA: {"sci_incarceration_period"},
}
