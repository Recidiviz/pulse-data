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
"""Class responsible for merging a root entity tree with new / updated child entities
into an existing version of that root entity.
"""
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity_matching.entity_merger_utils import (
    root_entity_external_id_keys,
)
from recidiviz.persistence.persistence_utils import RootEntityT


class RootEntityUpdateMerger:
    """Class responsible for merging a root entity tree with new / updated child
    entities into an existing version of that root entity.
    """

    def __init__(self, field_index: CoreEntityFieldIndex) -> None:
        self.field_index = field_index

    def merge_root_entity_trees(
        self, old_root_entity: RootEntityT, root_entity_updates: RootEntityT
    ) -> RootEntityT:
        if not self._can_merge_root_entity_trees(old_root_entity, root_entity_updates):
            raise ValueError(
                f"Attempting to merge updates for root entity with external ids "
                f"{root_entity_updates.external_ids} into entity with non-overlapping "
                f"external ids {old_root_entity.external_ids}."
            )

        # TODO(#20936): Update to actually merge and return the merged entity.
        return root_entity_updates

    @staticmethod
    def _can_merge_root_entity_trees(
        old_root_entity: RootEntityT, new_state: RootEntityT
    ) -> bool:
        """Determines whether two entity trees have overlapping external ids and can
        be merged.
        """
        previous_external_ids = root_entity_external_id_keys(old_root_entity)
        new_external_ids = root_entity_external_id_keys(new_state)

        return bool(previous_external_ids.intersection(new_external_ids))
