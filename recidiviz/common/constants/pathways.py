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
"""Constants shared by the Pathways export queries and the Pathways API."""

# Joins the parts of a filter option value that only means something together,
# e.g. a custody type and a movement type. The export writes values with it, the
# API splits filter values on it, and the frontend uses the same character:
# COMPOUND_FILTER_VALUE_DELIMITER in recidiviz-dashboards libs/shared-pathways.
PATHWAYS_COMPOUND_FILTER_VALUE_DELIMITER = "|"
