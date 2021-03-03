# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""State schema-specific implementation of BaseHistoricalSnapshotUpdater"""
from types import ModuleType
from typing import Any, List

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.persistence.database.history.base_historical_snapshot_updater import (
    BaseHistoricalSnapshotUpdater,
    _SnapshotContextRegistry,
    _SnapshotContext,
)
from recidiviz.persistence.database.schema.schema_person_type import SchemaPersonType
from recidiviz.persistence.database.schema.state import schema


class StateHistoricalSnapshotUpdater(BaseHistoricalSnapshotUpdater[schema.StatePerson]):
    """State schema-specific implementation of BaseHistoricalSnapshotUpdater"""

    def get_system_level(self) -> SystemLevel:
        return SystemLevel.STATE

    def get_schema_module(self) -> ModuleType:
        return schema

    def set_provided_start_and_end_times(
        self,
        root_people: List[SchemaPersonType],
        context_registry: _SnapshotContextRegistry,
    ) -> None:
        pass

    def post_process_initial_snapshot(
        self, snapshot_context: _SnapshotContext, initial_snapshot: Any
    ) -> None:
        pass
