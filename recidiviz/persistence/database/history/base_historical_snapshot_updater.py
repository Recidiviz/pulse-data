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
Module with base class for updating historical snapshots for all entities in all
record trees rooted in a list of Person or StatePerson schema objects.

See BaseHistoricalSnapshotUpdater for further documentation.
"""

import abc
import logging
from datetime import datetime
from collections import defaultdict
from types import ModuleType
from typing import Any, List, Generic, Type, Set, Callable, Optional, Dict

import attr

from sqlalchemy import text

from recidiviz.persistence.database.session import Session
from recidiviz.common.ingest_metadata import IngestMetadata, SystemLevel
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.history_table_shared_columns_mixin import (
    HistoryTableSharedColumns,
)
from recidiviz.persistence.database.schema.schema_person_type import SchemaPersonType
from recidiviz.persistence.database.schema_utils import HISTORICAL_TABLE_CLASS_SUFFIX
from recidiviz.persistence.entity.entity_utils import SchemaEdgeDirectionChecker


class BaseHistoricalSnapshotUpdater(Generic[SchemaPersonType]):
    """
    Base class for updating historical snapshots for all entities in all record
    trees rooted in a list of Person or StatePerson schema objects.

    NOTE: All code in this class makes the following two assumptions:
    1) The table ORM class names are: <ENTITY> and <ENTITY>History (e.g.
        Person and PersonHistory)
    2) The primary key column of the master table and the foreign key column
        on the historical table pointing to the master table have the same
        name (e.g. table 'person_history' has a foreign key column 'person_id'
        referencing the primary key column 'person_id' on table 'person')
    If either of these assumptions are broken, this module will not behave
    as expected.
    """

    @abc.abstractmethod
    def get_system_level(self) -> SystemLevel:
        """Returns the system level for this snapshot updater."""

    @abc.abstractmethod
    def get_schema_module(self) -> ModuleType:
        """
        Returns the module that defines the schema for this snapshot update
        """

    @abc.abstractmethod
    def set_provided_start_and_end_times(
        self,
        root_people: List[SchemaPersonType],
        context_registry: "_SnapshotContextRegistry",
    ) -> None:
        """For every entity in all record trees rooted at |root_people|,
        determines if the entity has a provided start time (i.e. the time the
        entity was created or otherwise began) and/or a provided end time (i.e.
        the time the entity was completed or closed) and sets it on the
        |context_registry|
        """

    @abc.abstractmethod
    def post_process_initial_snapshot(
        self, snapshot_context: "_SnapshotContext", initial_snapshot: DatabaseEntity
    ) -> None:
        """Perform any additional processing on the initial closed snapshot,
        i.e.the snapshot representing the state of the entity before its current
        completed state."""

    def update_historical_snapshots(
        self,
        session: Session,
        root_people: List[SchemaPersonType],
        orphaned_entities: List[DatabaseEntity],
        ingest_metadata: IngestMetadata,
    ) -> None:
        """For all entities in all record trees rooted at |root_people| and all
        entities in |orphaned_entities|, performs any required historical
        snapshot updates.

        If any entity has no existing historical snapshots, an initial snapshot
        will be created for it.

        If any column of an entity differs from its current snapshot, the
        current snapshot will be closed with period end time of |snapshot_time|
        and a new snapshot will be opened corresponding to the updated entity
        with period start time of |snapshot_time|.

        If neither of these cases applies, no action will be taken on the
        entity.
        """
        logging.info(
            "Beginning historical snapshot updates for %s record tree(s) and %s"
            " orphaned entities",
            len(root_people),
            len(orphaned_entities),
        )

        schema: ModuleType = self.get_schema_module()

        root_entities: List[DatabaseEntity] = list()
        root_entities.extend(root_people)
        root_entities.extend(orphaned_entities)

        self._assert_all_root_entities_unique(root_entities)

        context_registry = _SnapshotContextRegistry()

        self._execute_action_for_all_entities(
            root_entities, context_registry.register_entity
        )

        logging.info(
            "%s master entities registered for snapshot check",
            len(context_registry.all_contexts()),
        )

        most_recent_snapshots = self._fetch_most_recent_snapshots_for_all_entities(
            session, root_entities, schema
        )
        for snapshot in most_recent_snapshots:
            context_registry.add_snapshot(snapshot, schema)

        logging.info(
            "%s registered entities with existing snapshots", len(most_recent_snapshots)
        )

        # Provided start and end times only need to be set for root_people, not
        # orphaned entities. Provided start and end times are only relevant for
        # new entities with no existing snapshots, and orphaned entities by
        # definition are already present in the database and therefore already
        # have existing snapshots.
        self.set_provided_start_and_end_times(root_people, context_registry)

        logging.info("Provided start and end times set for registered entities")

        for snapshot_context in context_registry.all_contexts():
            self._write_snapshots(
                session, snapshot_context, ingest_metadata.ingest_time, schema
            )

        logging.info("Flushing snapshots")
        session.flush()
        logging.info("All historical snapshots written")

    def _fetch_most_recent_snapshots_for_all_entities(
        self, session: Session, root_entities: List[DatabaseEntity], schema: ModuleType
    ) -> List[DatabaseEntity]:
        """Returns a list containing the most recent snapshot for each entity in
        all graphs reachable from |root_entities|, if one exists.
        """

        # Consolidate all master entity IDs for each type, so that each
        # historical table only needs to be queried once
        ids_by_entity_type_name: Dict[str, Set[int]] = defaultdict(set)
        self._execute_action_for_all_entities(
            root_entities,
            lambda entity: ids_by_entity_type_name[type(entity).__name__].add(
                entity.get_primary_key()
            ),
        )

        snapshots: List[DatabaseEntity] = []
        for type_name, ids in ids_by_entity_type_name.items():
            master_class = getattr(schema, type_name)
            snapshots.extend(
                self._fetch_most_recent_snapshots_for_entity_type(
                    session, master_class, ids, schema
                )
            )
        return snapshots

    def _fetch_most_recent_snapshots_for_entity_type(
        self,
        session: Session,
        master_class: Type,
        entity_ids: Set[int],
        schema: ModuleType,
    ) -> List[DatabaseEntity]:
        """Returns a list containing the most recent snapshot for each ID in
        |entity_ids| with type |master_class|
        """

        # Get name of historical table in database (as distinct from name of ORM
        # class representing historical table in code)
        history_table_class = _get_historical_class(master_class, schema)
        history_table_name = history_table_class.__table__.name
        history_table_primary_key_col_name = (
            history_table_class.get_primary_key_column_name()
        )
        # See module assumption #2
        master_table_primary_key_col_name = master_class.get_primary_key_column_name()
        ids_list = ", ".join([str(id) for id in entity_ids])

        # Get snapshot IDs in a separate query. The subquery logic here is ugly
        # and easier to do as a raw string query than through the ORM query, but
        # the return type of a raw string query is just a collection of values
        # rather than an ORM model. Doing this step as a separate query enables
        # passing just the IDs to the second request, which allows proper ORM
        # models to be returned as a result.
        snapshot_ids_query = f"""
        SELECT
          history.{history_table_primary_key_col_name},
          history.{master_table_primary_key_col_name},
          history.valid_to
        FROM {history_table_name} history
        JOIN (
          SELECT 
            {master_table_primary_key_col_name}, 
            MAX(valid_from) AS valid_from
          FROM {history_table_name}
          WHERE {master_table_primary_key_col_name} IN ({ids_list})
          GROUP BY {master_table_primary_key_col_name}
        ) AS most_recent_valid_from
        ON history.{master_table_primary_key_col_name} = 
            most_recent_valid_from.{master_table_primary_key_col_name}
        WHERE history.valid_from = most_recent_valid_from.valid_from;
        """

        results = session.execute(text(snapshot_ids_query)).fetchall()

        # Use only results where valid_to is None to exclude any overlapping
        # non-open snapshots
        snapshot_ids = [
            snapshot_id
            for snapshot_id, master_id, valid_to in results
            if valid_to is None
        ]

        # Removing the below early return will pass in tests but fail in
        # production, because SQLite allows "IN ()" but Postgres does not
        if not snapshot_ids:
            return []

        filter_statement = (
            "{historical_table}.{primary_key_column} IN ({ids_list})".format(
                historical_table=history_table_name,
                primary_key_column=history_table_class.get_primary_key_column_name(),
                ids_list=", ".join([str(id) for id in snapshot_ids]),
            )
        )

        return session.query(history_table_class).filter(text(filter_statement)).all()

    def _write_snapshots(
        self,
        session: Session,
        context: "_SnapshotContext",
        snapshot_time: datetime,
        schema: ModuleType,
    ) -> None:
        """
        Writes snapshots for any new entities and any entities that have
        changes.

        If an entity has no existing snapshots and has a provided start time
        earlier than |snapshot_time|, will backdate the snapshot to the provided
        start time.

        If an entity has no existing snapshots and has a provided end time
        earlier than |snapshot_time|, will backdate the snapshot to the provided
        end time. (This assumes the entity has already been properly updated to
        reflect that it is completed, e.g. by marking a booking as released.)

        If an entity has no existing snapshots and has both a provided start
        time and provided end time earlier than |snapshot_time|, will create one
        snapshot from start time to end time reflecting the entity's state while
        it was active, and one open snapshot from end time reflecting the
        entity's current state (again assuming the entity has already been
        properly updated to reflect that it is completed).

        If there is a conflict between provided start and end time (i.e. start
        time is after end time), end time will govern.
        """

        # Historical table does not need to be updated if entity is not new and
        # its current state matches its most recent historical snapshot
        if (
            context.most_recent_snapshot is not None
            and self._does_entity_match_historical_snapshot(
                context.schema_object, context.most_recent_snapshot
            )
        ):
            return

        if context.most_recent_snapshot is None:
            self._write_snapshots_for_new_entities(
                session, context, snapshot_time, schema
            )
        else:
            self._write_snapshots_for_existing_entities(
                session, context, snapshot_time, schema
            )

    def _write_snapshots_for_new_entities(
        self,
        session: Session,
        context: "_SnapshotContext",
        snapshot_time: datetime,
        schema: ModuleType,
    ) -> None:
        """Writes snapshots for any new entities, including any required manual
        adjustments based on provided start and end times
        """
        historical_class = _get_historical_class(type(context.schema_object), schema)
        new_historical_snapshot = historical_class()
        self._copy_entity_fields_to_historical_snapshot(
            context.schema_object, new_historical_snapshot
        )

        provided_start_time = None
        provided_end_time = None

        # Validate provided start and end times
        if (
            context.provided_start_time
            and context.provided_start_time.date() < snapshot_time.date()
        ):
            provided_start_time = context.provided_start_time

        if (
            context.provided_end_time
            and context.provided_end_time.date() < snapshot_time.date()
        ):
            provided_end_time = context.provided_end_time

        if (
            provided_start_time
            and provided_end_time
            and provided_start_time >= provided_end_time
        ):
            provided_start_time = None

        if provided_start_time is not None and provided_end_time is None:
            new_historical_snapshot.valid_from = provided_start_time
        elif provided_end_time is not None:
            new_historical_snapshot.valid_from = provided_end_time
        else:
            new_historical_snapshot.valid_from = snapshot_time

        # Snapshot must be merged separately from record tree, as they are not
        # included in the ORM model relationships (to avoid needing to load
        # the entire snapshot chain at once)
        session.merge(new_historical_snapshot)

        # If both start and end time were provided, an earlier snapshot needs to
        # be created, reflecting the state of the entity before its current
        # completed state
        if provided_start_time and provided_end_time:
            initial_snapshot = historical_class()
            self._copy_entity_fields_to_historical_snapshot(
                context.schema_object, initial_snapshot
            )
            initial_snapshot.valid_from = provided_start_time
            initial_snapshot.valid_to = provided_end_time

            self.post_process_initial_snapshot(context, initial_snapshot)

            session.merge(initial_snapshot)

    def _write_snapshots_for_existing_entities(
        self,
        session: Session,
        context: "_SnapshotContext",
        snapshot_time: datetime,
        schema: ModuleType,
    ) -> None:
        """Writes snapshot updates for entities that already have snapshots
        present in the database
        """
        historical_class = _get_historical_class(type(context.schema_object), schema)
        new_historical_snapshot = historical_class()
        self._copy_entity_fields_to_historical_snapshot(
            context.schema_object, new_historical_snapshot
        )
        new_historical_snapshot.valid_from = snapshot_time

        # Snapshot must be merged separately from record tree, as they are not
        # included in the ORM model relationships (to avoid needing to load
        # the entire snapshot chain at once)
        session.merge(new_historical_snapshot)

        # Close last snapshot if one is present
        if context.most_recent_snapshot is not None:
            if not isinstance(context.most_recent_snapshot, HistoryTableSharedColumns):
                raise ValueError(
                    f"Snapshot class [{type(context.most_recent_snapshot)}] "
                    f"must be a subclass of "
                    f"[{HistoryTableSharedColumns.__name__}]"
                )

            context.most_recent_snapshot.valid_to = snapshot_time
            session.merge(context.most_recent_snapshot)

    def _assert_all_root_entities_unique(
        self, root_schema_objects: List[DatabaseEntity]
    ) -> None:
        """Raises (AssertionError) if any schema_object in |root_schema_objects|
        does not have a unique primary key for its type.
        """
        keys_by_type: Dict[str, Set[int]] = defaultdict(set)
        for schema_object in root_schema_objects:
            type_name = type(schema_object).__name__
            key = schema_object.get_primary_key()
            if key is None:
                raise ValueError(
                    f"Primary key not set for object of type [{type_name}]"
                )
            if key in keys_by_type[type_name]:
                raise AssertionError(
                    "Duplicate entities passed of type {} with ID {}".format(
                        type_name, key
                    )
                )
            keys_by_type[type_name].add(key)

    def _execute_action_for_all_entities(
        self, start_schema_objects: List[DatabaseEntity], action: Callable, *args: Any
    ) -> None:
        """For every entity in every graph reachable from
        |start_schema_objects|, invokes |action|, passing the entity and
        |*args| as arguments.
        """

        unprocessed = list(start_schema_objects)
        unprocessed_ids = {id(start_object) for start_object in start_schema_objects}
        processed_ids = set()

        while unprocessed:
            entity = unprocessed.pop()
            action(entity, *args)
            unprocessed_ids.remove(id(entity))
            processed_ids.add(id(entity))

            for related_entity in self._get_related_entities(entity):
                related_id = id(related_entity)
                if (
                    related_id not in unprocessed_ids
                    and related_id not in processed_ids
                ):
                    unprocessed.append(related_entity)
                    unprocessed_ids.add(related_id)

    def _get_related_entities(self, entity: DatabaseEntity) -> List[DatabaseEntity]:
        """Returns list of all entities related to |entity|"""

        related_entities = []
        for relationship_name in entity.get_relationship_property_names():
            # TODO(#1145): For County schema, fix direction checker to gracefully
            # handle the fact that SentenceRelationship exists in the schema
            # but not in the entity layer.
            if self.get_system_level() == SystemLevel.STATE:
                # Skip back edges
                direction_checker = SchemaEdgeDirectionChecker.state_direction_checker()
                if direction_checker.is_back_edge(entity, relationship_name):
                    continue

            related = getattr(entity, relationship_name)

            # Relationship can return either a list or a single item
            if isinstance(related, list):
                related_entities.extend(related)
            elif related is not None:
                related_entities.append(related)
        return related_entities

    def _does_entity_match_historical_snapshot(
        self, schema_object: DatabaseEntity, historical_snapshot: DatabaseEntity
    ) -> bool:
        """Returns (True) if all fields on |entity| are equal to the
        corresponding fields on |historical_snapshot|.

        NOTE: This method *only* compares properties which are present on both
        the master and historical tables. Any property that is only present on
        one table will be ignored.
        """

        for column_property_name in self._get_shared_column_property_names(
            type(schema_object), type(historical_snapshot)
        ):
            entity_value = getattr(schema_object, column_property_name)
            historical_value = getattr(historical_snapshot, column_property_name)
            if entity_value != historical_value:
                return False

        return True

    def _copy_entity_fields_to_historical_snapshot(
        self, entity: DatabaseEntity, historical_snapshot: DatabaseEntity
    ) -> None:
        """
        Copies all column values present on |entity| to |historical_snapshot|.

        NOTE: This method *only* copies values for properties which are present
        on both the master and historical tables. Any property that is only
        present on one table will be ignored. The only exception is the master
        key column, which is copied over regardless of property name (only based
        on *column* name), following module assumption #2.
        """
        for column_property_name in self._get_shared_column_property_names(
            type(entity), type(historical_snapshot)
        ):
            entity_value = getattr(entity, column_property_name)
            setattr(historical_snapshot, column_property_name, entity_value)

        # See module assumption #2
        key_column_name = entity.get_primary_key_column_name()  # type: ignore
        historical_master_key_property_name = type(
            historical_snapshot
        ).get_property_name_by_column_name(key_column_name)
        setattr(
            historical_snapshot,
            historical_master_key_property_name,
            entity.get_primary_key(),
        )  # type: ignore

    @staticmethod
    def _get_shared_column_property_names(
        entity_class_a: Type, entity_class_b: Type
    ) -> List[str]:
        """Returns a set of all column property names shared between
        |entity_class_a| and |entity_class_b|.
        """
        return entity_class_a.get_column_property_names().intersection(
            entity_class_b.get_column_property_names()
        )


def _get_historical_class(
    master_class: Type[DatabaseEntity], schema: ModuleType
) -> Type:
    """Returns ORM class of historical table associated with the master table of
    |master_class|
    """
    # See module assumption #1
    historical_class_name = "{}{}".format(
        master_class.__name__, HISTORICAL_TABLE_CLASS_SUFFIX
    )
    return getattr(schema, historical_class_name)


def _get_master_class(
    historical_class: Type[DatabaseEntity], schema: ModuleType
) -> Type:
    """Returns ORM class of master table associated with the historical table of
    |historical_class|
    """
    # See module assumption #1
    master_class_name = historical_class.__name__.replace(
        HISTORICAL_TABLE_CLASS_SUFFIX, ""
    )
    return getattr(schema, master_class_name)


@attr.s
class _SnapshotContext:
    """Container for all data required for snapshot operations for a single
    entity
    """

    schema_object: DatabaseEntity = attr.ib(default=None)
    most_recent_snapshot: Optional[DatabaseEntity] = attr.ib(default=None)
    provided_start_time: Optional[datetime] = attr.ib(default=None)
    provided_end_time: Optional[datetime] = attr.ib(default=None)


class _SnapshotContextRegistry:
    """Container for all snapshot contexts for all entities"""

    def __init__(self) -> None:
        # Nested map:
        # (master entity type name string) -> ((primary key) -> (context))
        self.snapshot_contexts: Dict[str, Dict[int, _SnapshotContext]] = {}

    def snapshot_context(self, entity: DatabaseEntity) -> _SnapshotContext:
        """Returns (_SnapshotContext) for |entity|"""
        context_map = self.snapshot_contexts[type(entity).__name__]
        primary_key = entity.get_primary_key()
        if primary_key is None:
            raise ValueError("primary key should not be None")
        return context_map[primary_key]

    def all_contexts(self) -> List[_SnapshotContext]:
        """Returns all (_SnapshotContext) objects present in registry"""
        contexts: List[_SnapshotContext] = []
        for nested_registry in self.snapshot_contexts.values():
            contexts.extend(nested_registry.values())
        return contexts

    def register_entity(self, schema_object: DatabaseEntity) -> None:
        """Creates (_SnapshotContext) for |entity| and adds it to registry

        Raises (ValueError) if |entity| has already been registered
        """
        type_name = type(schema_object).__name__
        if type_name not in self.snapshot_contexts:
            self.snapshot_contexts[type_name] = {}

        entity_id = schema_object.get_primary_key()
        if entity_id in self.snapshot_contexts[type_name]:
            raise ValueError(
                "Entity already registered with type {type} and primary key "
                "{primary_key}".format(type=type_name, primary_key=entity_id)
            )
        if entity_id is None:
            raise ValueError("primary key should not be None")

        self.snapshot_contexts[type_name][entity_id] = _SnapshotContext(
            schema_object=schema_object
        )

    def add_snapshot(self, snapshot: DatabaseEntity, schema: ModuleType) -> None:
        """Registers |snapshot| to the appropriate (_SnapshotContext) of the
        master entity corresponding to |snapshot|

        Raises (ValueError) if a snapshot has already been registered for the
        corresponding master entity
        """
        master_class = _get_master_class(type(snapshot), schema)
        master_type_name = master_class.__name__

        key_column_name = master_class.get_primary_key_column_name()
        # See module assumption #2
        master_key_property_name = type(snapshot).get_property_name_by_column_name(
            key_column_name
        )
        master_entity_id = getattr(snapshot, master_key_property_name)

        if (
            self.snapshot_contexts[master_type_name][
                master_entity_id
            ].most_recent_snapshot
            is not None
        ):
            raise ValueError(
                "Snapshot already registered for master entity with type "
                "{type} and primary key {primary_key}".format(
                    type=master_type_name, primary_key=master_entity_id
                )
            )

        self.snapshot_contexts[master_type_name][
            master_entity_id
        ].most_recent_snapshot = snapshot
