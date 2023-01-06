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
"""A factory for generating a RootEntityEntityMatchingDelegate."""

from recidiviz.persistence.entity_matching.state.root_entity_entity_matching_delegate import (
    RootEntityEntityMatchingDelegate,
)
from recidiviz.persistence.entity_matching.state.root_state_person_entity_matching_delegate import (
    RootStatePersonEntityMatchingDelegate,
)


class RootEntityEntityMatchingDelegateFactory:
    """A factory for generating a RootEntityEntityMatchingDelegate."""

    @staticmethod
    def build() -> RootEntityEntityMatchingDelegate:
        # TODO(#17471): Update to return either a RootStatePersonEntityMatchingDelegate
        #  or a RootStateStaffEntityMatchingDelegate based on what type of root entity
        #  we are
        #  matching.
        return RootStatePersonEntityMatchingDelegate()
