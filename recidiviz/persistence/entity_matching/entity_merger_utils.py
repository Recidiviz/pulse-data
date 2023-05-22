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
"""Utils to help with entity merging."""
from typing import Set

from recidiviz.persistence.persistence_utils import RootEntityT


def root_entity_external_id_keys(root_entity: RootEntityT) -> Set[str]:
    """Generates a set of unique string keys for a root entity's external id objects."""
    return {f"{type(e)}#{e.external_id}|{e.id_type}" for e in root_entity.external_ids}
