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

"""
Method to update historical snapshots for all entities in all record trees
rooted in a list of Person or StatePerson schema objects.

See BaseHistoricalSnapshotUpdater for further documentation.
"""

from typing import List

from sqlalchemy.orm import Session

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.history.county.historical_snapshot_updater \
    import CountyHistoricalSnapshotUpdater
from recidiviz.persistence.database.history.state.historical_snapshot_updater \
    import StateHistoricalSnapshotUpdater
from recidiviz.persistence.database.schema.schema_person_type import \
    SchemaPersonType

from recidiviz.persistence.database.schema.county import schema as county_schema
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.utils import trace


@trace.span
def update_historical_snapshots(session: Session,
                                root_people: List[SchemaPersonType],
                                orphaned_schema_objects: List[DatabaseEntity],
                                ingest_metadata: IngestMetadata) -> None:
    """For all entities in all record trees rooted at |root_people| and all
    entities in |orphaned_schema_objects|, performs any required historical
    snapshot updates.

    If any entity has no existing historical snapshots, an initial snapshot will
    be created for it.

    If any column of an entity differs from its current snapshot, the current
    snapshot will be closed with period end time of |snapshot_time| and a new
    snapshot will be opened corresponding to the updated entity with period
    start time of |snapshot_time|.

    If neither of these cases applies, no action will be taken on the entity.
    """
    if all(isinstance(person, county_schema.Person) for person in root_people):
        CountyHistoricalSnapshotUpdater().update_historical_snapshots(
            session, root_people, orphaned_schema_objects, ingest_metadata)
    elif all(isinstance(person,
                        state_schema.StatePerson) for person in root_people):
        StateHistoricalSnapshotUpdater().update_historical_snapshots(
            session, root_people, orphaned_schema_objects, ingest_metadata)
    else:
        raise ValueError(f'Expected all types to be the same type, and one of '
                         f'[{county_schema.Person}] or '
                         f'[{state_schema.StatePerson}]')
