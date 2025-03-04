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
"""Class to store information about the authorized user requesting Outliers information."""
from typing import Optional

import attr


@attr.define
class UserContext:
    """Stores information about the current user."""

    # Use string to allow storing "CSG" as a value
    state_code_str: str
    user_external_id: str
    email_address: str
    pseudonymized_id: Optional[str]
    can_access_all_supervisors: bool
    can_access_supervision_workflows: bool
    feature_variants: dict
