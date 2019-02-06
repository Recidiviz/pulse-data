"""Method to update historical snapshots for all entities in all record trees
rooted in a list of Person schema objects

NOTE: All code in this module makes the following two assumptions:
1) The table ORM class names are: <ENTITY> and <ENTITY>History (e.g.
    Person and PersonHistory)
2) The primary key column of the master table and the foreign key column
    on the historical table pointing to the master table have the same
    name (e.g. table 'person_history' has a foreign key column 'person_id'
    referencing the primary key column 'person_id' on table 'person')
If either of these assumptions are broken, this module will not behave
as expected.
"""

from datetime import datetime
import logging
from typing import Any, Callable, List, Optional, Type

import attr
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from recidiviz.persistence.database import schema


_HISTORICAL_TABLE_CLASS_SUFFIX = 'History'


def update_historical_snapshots(session: Session,
                                root_people: List[schema.Person],
                                snapshot_time: datetime) -> None:
    """For all entities in all record trees rooted at |root_people|, performs
    any required historical snapshot updates.

    If any entity has no existing historical snapshots, an initial snapshot will
    be created for it.

    If any column of an entity differs from its current snapshot, the current
    snapshot will be closed with period end time of |snapshot_time| and a new
    snapshot will be opened corresponding to the updated entity with period
    start time of |snapshot_time|.

    If neither of these cases applies, no action will be taken on the entity.
    """
    logging.info(
        "Beginning historical snapshot updates for %s record tree(s)",
        len(root_people))

    context_registry = _SnapshotContextRegistry()

    _execute_action_for_all_entities(
        root_people, context_registry.register_entity)

    most_recent_snapshots = _fetch_most_recent_snapshots_for_all_entities(
        session, root_people)
    for snapshot in most_recent_snapshots:
        context_registry.add_snapshot(snapshot)

    for snapshot_context in context_registry.all_contexts():
        _update_snapshots_from_context(session, snapshot_context, snapshot_time)


def _fetch_most_recent_snapshots_for_all_entities(
        session: Session, root_people: List[schema.Person]) -> List[Any]:
    """Returns a list containing the most recent snapshot for each entity in
    all record trees rooted at |root_people|, if one exists.
    """
    snapshots: List[Any] = []

    # TODO: replace with pre-collecting IDs and making 1 query per table
    _execute_action_for_all_entities(
        root_people, _fetch_snapshot, session, snapshots)

    return snapshots


# TODO: replace with method to fetch all snapshots at once
# pylint: disable=missing-docstring
def _fetch_snapshot(entity, session, snapshots):
    # Get name of historical table in database (as distinct from name of ORM
    # class representing historical table in code)
    historical_class = _get_historical_class(type(entity))
    historical_table_name = historical_class.__table__.name

    filter_statement = \
        '{historical_table}.{master_foreign_key} = {master_entity_id}'.format(
            historical_table=historical_table_name,
            # See module assumption #2
            master_foreign_key=type(entity).get_primary_key_column_name(),
            master_entity_id=entity.get_primary_key())

    snapshot = session.query(historical_class) \
        .filter(text(filter_statement)) \
        .order_by(historical_class.valid_from.desc()) \
        .first()

    if snapshot is not None:
        snapshots.append(snapshot)


# TODO: replace with method that takes into account provided period values
# pylint: disable=missing-docstring
def _update_snapshots_from_context(session, context, snapshot_time):
    # Historical table does not need to be updated if entity is not new and
    # its current state matches its most recent historical snapshot
    if context.most_recent_snapshot is not None and \
            _does_entity_match_historical_snapshot(
                    context.entity, context.most_recent_snapshot):
        return

    historical_class = _get_historical_class(type(context.entity))
    new_historical_snapshot = historical_class()
    _copy_entity_fields_to_historical_snapshot(
        context.entity, new_historical_snapshot)
    new_historical_snapshot.valid_from = snapshot_time

    # See module assumption #2
    key_column_name = context.entity.get_primary_key_column_name()
    historical_master_key_property_name = \
        historical_class.get_property_name_by_column_name(key_column_name)
    setattr(new_historical_snapshot, historical_master_key_property_name,
            context.entity.get_primary_key())

    # Snapshot must be merged separately from record tree, as they are not
    # included in the ORM model relationships (to avoid needing to load
    # the entire snapshot chain at once)
    session.merge(new_historical_snapshot)

    # Close last snapshot if one is present
    if context.most_recent_snapshot is not None:
        context.most_recent_snapshot.valid_to = snapshot_time
        session.merge(context.most_recent_snapshot)


def _execute_action_for_all_entities(root_people: List[schema.Person],
                                     action: Callable, *args) -> None:
    """For every entity in every record tree rooted at one of |root_people|,
    invokes |action|, passing the entity and |*args| as arguments"""

    unprocessed = list(root_people)
    processed = []
    while unprocessed:
        entity = unprocessed.pop()
        action(entity, *args)
        processed.append(entity)

        unprocessed.extend([related_entity for related_entity
                            in _get_related_entities(entity)
                            if related_entity not in processed
                            and related_entity not in unprocessed])


def _get_related_entities(entity: Any) -> List[Any]:
    """Returns list of all entities related to |entity|"""

    related_entities = []

    for relationship_name in entity.get_relationship_property_names():
        related = getattr(entity, relationship_name)

        # Relationship can return either a list or a single item
        if isinstance(related, list):
            related_entities.extend(related)
        elif related is not None:
            related_entities.append(related)

    return related_entities


def _does_entity_match_historical_snapshot(entity: Any,
                                           historical_snapshot: Any) -> bool:
    """Returns (True) if all fields on |entity| are equal to the corresponding
    fields on |historical_snapshot|.

    NOTE: This method *only* compares columns which are present on both the
    master and historical tables. Any column that is only present on one table
    will be ignored.
    """

    for column_property_name in _get_shared_column_property_names(
            type(entity), type(historical_snapshot)):
        entity_value = getattr(entity, column_property_name)
        historical_value = getattr(historical_snapshot, column_property_name)
        if entity_value != historical_value:
            return False

    return True


def _copy_entity_fields_to_historical_snapshot(
        entity: Any, historical_snapshot: Any) -> None:
    """Copies all column values present on |entity| to |historical_snapshot|.

    NOTE: This method *only* copies values for columns which are present on
    both the master and historical tables. Any column that is only present on
    one table will be ignored.
    """

    for column_property_name in _get_shared_column_property_names(
            type(entity), type(historical_snapshot)):
        entity_value = getattr(entity, column_property_name)
        setattr(historical_snapshot, column_property_name, entity_value)


def _get_shared_column_property_names(entity_class_a: Type,
                                      entity_class_b: Type) -> List[str]:
    """Returns a set of all column property names shared between
    |entity_class_a| and |entity_class_b|.
    """
    return entity_class_a.get_column_property_names().intersection(
        entity_class_b.get_column_property_names())


def _get_historical_class(master_class: Type) -> Type:
    """Returns ORM class of historical table associated with the master table of
    |master_class|
    """
    # See module assumption #1
    historical_class_name = '{}{}'.format(
        master_class.__name__, _HISTORICAL_TABLE_CLASS_SUFFIX)
    return getattr(schema, historical_class_name)


def _get_master_class(historical_class: Type) -> Type:
    """Returns ORM class of master table associated with the historical table of
    |historical_class|
    """
    # See module assumption #1
    master_class_name = historical_class.__name__.replace(
        _HISTORICAL_TABLE_CLASS_SUFFIX, '')
    return getattr(schema, master_class_name)


@attr.s
class _SnapshotContext:
    """Container for all data required for snapshot operations for a single
    entity
    """
    entity: Optional[Any] = attr.ib(default=None)
    most_recent_snapshot: Optional[Any] = attr.ib(default=None)
    provided_valid_from: Optional[datetime] = attr.ib(default=None)
    provided_valid_to: Optional[datetime] = attr.ib(default=None)


class _SnapshotContextRegistry:
    """Container for all snapshot contexts for all entities"""

    def __init__(self):
        # Nested map:
        # (master entity type name string) -> ((primary key) -> (context))
        self.snapshot_contexts = {}

    def snapshot_context(self, entity: Any) -> _SnapshotContext:
        """Returns (_SnapshotContext) for |entity|"""
        return self.snapshot_contexts[type(entity).__name__] \
            [entity.get_primary_key()]

    def all_contexts(self) -> List[_SnapshotContext]:
        """Returns all (_SnapshotContext) objects present in registry"""
        contexts: List[_SnapshotContext] = []
        for nested_registry in self.snapshot_contexts.values():
            contexts.extend(nested_registry.values())
        return contexts

    def register_entity(self, entity: Any) -> None:
        """Creates (_SnapshotContext) for |entity| and adds it to registry

        Raises (ValueError) if |entity| has already been registered
        """
        type_name = type(entity).__name__
        if type_name not in self.snapshot_contexts:
            self.snapshot_contexts[type_name] = {}

        entity_id = entity.get_primary_key()
        if entity_id in self.snapshot_contexts[type_name]:
            raise ValueError(
                'Entity already registered with type {type} and primary key '
                '{primary_key}'.format(type=type_name, primary_key=entity_id))

        self.snapshot_contexts[type_name][entity_id] = \
            _SnapshotContext(entity=entity)

    def add_snapshot(self, snapshot: Any) -> None:
        """Registers |snapshot| to the appropriate (_SnapshotContext) of the
        master entity corresponding to |snapshot|

        Raises (ValueError) if a snapshot has already been registered for the
        corresponding master entity
        """
        master_class = _get_master_class(type(snapshot))
        master_type_name = master_class.__name__

        key_column_name = master_class.get_primary_key_column_name()
        # See module assumption #2
        master_key_property_name = type(snapshot) \
            .get_property_name_by_column_name(key_column_name)
        master_entity_id = getattr(snapshot, master_key_property_name)

        if self.snapshot_contexts[master_type_name][master_entity_id] \
                .most_recent_snapshot is not None:
            raise ValueError(
                'Snapshot already registered for master entity with type '
                '{type} and primary key {primary_key}'.format(
                    type=master_type_name, primary_key=master_entity_id))

        self.snapshot_contexts[master_type_name][master_entity_id] \
            .most_recent_snapshot = snapshot
