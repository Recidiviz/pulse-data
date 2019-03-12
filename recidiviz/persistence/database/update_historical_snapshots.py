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

from collections import defaultdict
from datetime import datetime
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Type

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

    _assert_all_record_trees_well_formed(root_people)

    _assert_all_record_trees_unique(root_people)

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

    # Consolidate all master entity IDs for each type, so that each historical
    # table only needs to be queried once
    ids_by_entity_type_name: Dict[str, Set[int]] = defaultdict(set)
    _execute_action_for_all_entities(
        root_people,
        lambda entity: ids_by_entity_type_name[type(entity).__name__] \
            .add(entity.get_primary_key()))

    snapshots: List[Any] = []
    for type_name, ids in ids_by_entity_type_name.items():
        master_class = getattr(schema, type_name)
        snapshots.extend(_fetch_most_recent_snapshots_for_entity_type(
            session, master_class, ids))
    return snapshots


def _fetch_most_recent_snapshots_for_entity_type(
        session: Session, master_class: Type,
        entity_ids: Set[int]) -> List[Any]:
    """Returns a list containing the most recent snapshot for each ID in
    |entity_ids| with type |master_class|
    """

    # Get name of historical table in database (as distinct from name of ORM
    # class representing historical table in code)
    historical_class = _get_historical_class(master_class)
    historical_table_name = historical_class.__table__.name

    # Get snapshot IDs in a separate query. The subquery logic here is ugly and
    # easier to do as a raw string query than through the ORM query, but the
    # return type of a raw string query is just a collection of values rather
    # than an ORM model. Doing this step as a separate query enables passing
    # just the IDs to the second request, which allows proper ORM models to be
    # returned as a result.
    snapshot_ids_query = '''
    SELECT
      history.{primary_key_column},
      history.{master_key_column},
      history.valid_to
    FROM {historical_table} history
    JOIN (
      SELECT {master_key_column}, MAX(valid_from) AS valid_from
      FROM {historical_table}
      WHERE {master_key_column} IN ({ids_list})
      GROUP BY {master_key_column}
    ) AS most_recent_valid_from
    ON history.{master_key_column} = most_recent_valid_from.{master_key_column}
    WHERE history.valid_from = most_recent_valid_from.valid_from;
    '''.format(
        primary_key_column=historical_class.get_primary_key_column_name(),
        historical_table=historical_table_name,
        # See module assumption #2
        master_key_column=master_class.get_primary_key_column_name(),
        ids_list=', '.join([str(id) for id in entity_ids]))

    results = session.execute(text(snapshot_ids_query)).fetchall()

    # TODO(899): remove this once zero-duration snapshots stop being written
    results_by_master_key: Dict[int, List] = defaultdict(list)
    for result in results:
        results_by_master_key[result[1]].append(result)
    overlapping_snapshot_entities = [str(master_key) for master_key, results
                                     in results_by_master_key.items()
                                     if len(results) > 1]
    if overlapping_snapshot_entities:
        logging.error('Overlapping historical snapshots found in table %s '
                      'for master entity IDs %s', historical_table_name,
                      ', '.join(overlapping_snapshot_entities))

    # Use only results where valid_to is None to exclude any overlapping
    # non-open snapshots
    snapshot_ids = [snapshot_id for snapshot_id, master_id, valid_to
                    in results if valid_to is None]

    # Removing the below early return will pass in tests but fail in production,
    # because SQLite allows "IN ()" but Postgres does not
    if not snapshot_ids:
        return []

    filter_statement = \
        '{historical_table}.{primary_key_column} IN ({ids_list})'.format(
            historical_table=historical_table_name,
            primary_key_column=historical_class.get_primary_key_column_name(),
            ids_list=', '.join([str(id) for id in snapshot_ids]))

    return session.query(historical_class) \
        .filter(text(filter_statement)) \
        .all()


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


def _assert_all_record_trees_unique(root_people: List[schema.Person]) -> None:
    """Raises (AssertionError) if any person in |root_people| does not have a
    unique primary key
    """
    keys: Set[int] = set()
    for person in root_people:
        key = person.get_primary_key()
        if key in keys:
            raise AssertionError(
                'Multiple record trees passed for person ID: {}'.format(key))
        keys.add(key)


# TODO(1125): either delete this or clean this up after #1125 is debugged
def _assert_all_record_trees_well_formed(
        root_people: List[schema.Person]) -> None:
    """Raises (AssertionError) if there are any duplicate entities within any
    record tree or between any record trees.
    """
    person_ids: Set[int] = set()
    for person in root_people:
        person_id = person.get_primary_key()
        if person_id in person_ids:
            raise AssertionError(
                'Duplicate people with person ID: {}'.format(person_id))
        person_ids.add(person_id)

    booking_ids_with_ancestors: Dict[int, List[int]] = {}
    for person in root_people:
        for booking in person.bookings:
            booking_id = booking.get_primary_key()
            if booking_id in booking_ids_with_ancestors.keys():
                person_id_a = booking_ids_with_ancestors[booking_id][0]
                person_id_b = person.get_primary_key()
                raise AssertionError(
                    'Duplicate bookings with booking ID: {}, one with '
                    'parent person ID {} and one with parent person ID {}' \
                    .format(booking_id, person_id_a, person_id_b))
            booking_ids_with_ancestors[booking_id] = [person.get_primary_key()]

    charge_ids_with_ancestors: Dict[int, List[int]] = {}
    for person in root_people:
        for booking in person.bookings:
            for charge in booking.charges:
                charge_id = charge.get_primary_key()
                if charge_id in charge_ids_with_ancestors.keys():
                    person_id_a = charge_ids_with_ancestors[charge_id][0]
                    booking_id_a = charge_ids_with_ancestors[charge_id][1]
                    person_id_b = person.get_primary_key()
                    booking_id_b = booking.get_primary_key()
                    raise AssertionError(
                        'Duplicate charges with charge ID: {}, one with '
                        'parent person ID {} and parent booking ID {}, and '
                        'one with parent person ID {} and parent booking ID '
                        '{}'.format(charge_id, person_id_a, booking_id_a,
                                    person_id_b, booking_id_b))
                charge_ids_with_ancestors[charge_id] = \
                    [person.get_primary_key(), booking.get_primary_key()]

    bond_ids_with_ancestors: Dict[int, List[int]] = {}
    for person in root_people:
        for booking in person.bookings:
            for charge in booking.charges:
                if charge.bond is not None:
                    bond = charge.bond
                    bond_id = bond.get_primary_key()
                    if bond_id in bond_ids_with_ancestors.keys():
                        person_id_a = bond_ids_with_ancestors[bond_id][0]
                        booking_id_a = bond_ids_with_ancestors[bond_id][1]
                        charge_id_a = bond_ids_with_ancestors[bond_id][2]
                        person_id_b = person.get_primary_key()
                        booking_id_b = booking.get_primary_key()
                        charge_id_b = charge.get_primary_key()
                        raise AssertionError(
                            'Duplicate bonds with bond ID: {}, one with '
                            'parent person ID {}, parent booking ID {}, and '
                            'parent charge ID {}, and one with parent person '
                            'ID {}, parent booking ID {}, and parent charge '
                            'ID {}'.format(bond_id, person_id_a, booking_id_a,
                                           charge_id_a, person_id_b,
                                           booking_id_b, charge_id_b))
                    bond_ids_with_ancestors[bond_id] = \
                        [person.get_primary_key(),
                         booking.get_primary_key(),
                         charge.get_primary_key()]

    sentence_ids_with_ancestors: Dict[int, List[int]] = {}
    for person in root_people:
        for booking in person.bookings:
            for charge in booking.charges:
                if charge.sentence is not None:
                    sentence = charge.sentence
                    sentence_id = sentence.get_primary_key()
                    if sentence_id in sentence_ids_with_ancestors.keys():
                        person_id_a = \
                            sentence_ids_with_ancestors[sentence_id][0]
                        booking_id_a = \
                            sentence_ids_with_ancestors[sentence_id][1]
                        charge_id_a = \
                            sentence_ids_with_ancestors[sentence_id][2]
                        person_id_b = person.get_primary_key()
                        booking_id_b = booking.get_primary_key()
                        charge_id_b = charge.get_primary_key()
                        raise AssertionError(
                            'Duplicate sentences with sentence ID: {}, one '
                            'with parent person ID {}, parent booking ID {}, '
                            'and parent charge ID {}, and one with parent '
                            'person ID {}, parent booking ID {}, and '
                            'parent charge ID {}'.format(
                                sentence_id, person_id_a, booking_id_a,
                                charge_id_a, person_id_b, booking_id_b,
                                charge_id_b))
                    sentence_ids_with_ancestors[sentence_id] = \
                        [person.get_primary_key(),
                         booking.get_primary_key(),
                         charge.get_primary_key()]


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
