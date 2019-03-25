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
from datetime import date, datetime
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Type

import attr
from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.common.constants.sentence import SentenceStatus
from recidiviz.persistence.database import schema


_HISTORICAL_TABLE_CLASS_SUFFIX = 'History'


_BOOKING_DESCENDANT_START_DATE_FIELD = {
    schema.Sentence.__name__: 'date_imposed'
}


_BOOKING_DESCENDANT_END_DATE_FIELD = {
    schema.Sentence.__name__: 'completion_date'
}


def update_historical_snapshots(session: Session,
                                root_people: List[schema.Person],
                                orphaned_entities: List[schema.DatabaseEntity],
                                snapshot_time: datetime) -> None:
    """For all entities in all record trees rooted at |root_people| and all
    entities in |orphaned_entities|, performs any required historical snapshot
    updates.

    If any entity has no existing historical snapshots, an initial snapshot will
    be created for it.

    If any column of an entity differs from its current snapshot, the current
    snapshot will be closed with period end time of |snapshot_time| and a new
    snapshot will be opened corresponding to the updated entity with period
    start time of |snapshot_time|.

    If neither of these cases applies, no action will be taken on the entity.
    """
    logging.info(
        "Beginning historical snapshot updates for %s record tree(s) and %s "
        "orphaned entities",
        len(root_people),
        len(orphaned_entities))

    root_entities: List[Any] = root_people + orphaned_entities # type: ignore

    _assert_all_root_entities_unique(root_entities)

    context_registry = _SnapshotContextRegistry()

    _execute_action_for_all_entities(
        root_entities, context_registry.register_entity)

    logging.info("%s master entities registered for snapshot check",
                 len(context_registry.all_contexts()))

    most_recent_snapshots = _fetch_most_recent_snapshots_for_all_entities(
        session, root_entities)
    for snapshot in most_recent_snapshots:
        context_registry.add_snapshot(snapshot)

    logging.info("%s registered entities with existing snapshots",
                 len(most_recent_snapshots))

    # Provided start and end times only need to be set for root_people, not
    # orphaned entities. Provided start and end times are only relevant for
    # new entities with no existing snapshots, and orphaned entities by
    # definition are already present in the database and therefore already have
    # existing snapshots.
    _set_provided_start_and_end_times(root_people, context_registry)

    logging.info("Provided start and end times set for registered entities")

    for snapshot_context in context_registry.all_contexts():
        _write_snapshots(session, snapshot_context, snapshot_time)

    logging.info("All historical snapshots written")


def _fetch_most_recent_snapshots_for_all_entities(
        session: Session, root_entities: List[Any]) -> List[Any]:
    """Returns a list containing the most recent snapshot for each entity in
    all graphs reachable from |root_entities|, if one exists.
    """

    # Consolidate all master entity IDs for each type, so that each historical
    # table only needs to be queried once
    ids_by_entity_type_name: Dict[str, Set[int]] = defaultdict(set)
    _execute_action_for_all_entities(
        root_entities,
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


def _set_provided_start_and_end_times(
        root_people: List[schema.Person],
        context_registry: '_SnapshotContextRegistry') -> None:
    """For every entity in all record trees rooted at |root_people|, determines
    if the entity has a provided start time (i.e. the time the entity was
    created or otherwise began) and/or a provided end time (i.e. the time the
    entity was completed or closed) and sets it on the |context_registry|
    """
    for person in root_people:
        earliest_booking_date = None

        for booking in person.bookings:
            admission_date = None
            release_date = None
            # Don't include case where admission_date_inferred is None
            if booking.admission_date and \
                    booking.admission_date_inferred is False:
                admission_date = booking.admission_date
                context_registry.snapshot_context(booking) \
                    .provided_start_time = _date_to_datetime(admission_date)
                if earliest_booking_date is None \
                        or admission_date < earliest_booking_date:
                    earliest_booking_date = admission_date

            # Don't include case where release_date_inferred is None
            if booking.release_date and \
                    booking.release_date_inferred is False:
                release_date = booking.release_date
                context_registry.snapshot_context(booking) \
                    .provided_end_time = _date_to_datetime(release_date)
                if earliest_booking_date is None \
                        or release_date < earliest_booking_date:
                    earliest_booking_date = release_date

            # If no booking start date is provided or if start date is after
            # end date, end date should be taken as booking start date instead
            booking_start_date = None
            if admission_date is not None:
                booking_start_date = admission_date
            if release_date is not None and \
                    (booking_start_date is None or
                     booking_start_date > release_date):
                booking_start_date = release_date

            _execute_action_for_all_entities(
                _get_related_entities(booking),
                _set_provided_start_and_end_time_for_booking_descendant,
                booking_start_date,
                context_registry)

        if earliest_booking_date:
            context_registry.snapshot_context(person).provided_start_time = \
                _date_to_datetime(earliest_booking_date)


def _set_provided_start_and_end_time_for_booking_descendant(
        entity: Any, parent_booking_start_date: date,
        context_registry: '_SnapshotContextRegistry') -> None:
    """Sets |entity| provided start time and/or provided end time on
    |context_registry| according to type of |entity|
    """

    # If this method is called during graph exploration on parent booking or
    # parent person, do nothing.
    if isinstance(entity, (schema.Booking, schema.Person)):
        return

    start_time = None
    end_time = None

    # Unless a more specific start time is provided, booking descendants should
    # be treated as having the same start time as the parent booking
    #
    # Note the inverse of this does NOT apply to end time. We cannot assume
    # child entities end when the parent booking ends.
    if parent_booking_start_date:
        start_time = _date_to_datetime(parent_booking_start_date)

    start_date_field_name = \
        _get_booking_descendant_start_date_field_name(entity)
    if start_date_field_name:
        start_date = getattr(entity, start_date_field_name)
        if start_date:
            start_time = _date_to_datetime(start_date)

    end_date_field_name = _get_booking_descendant_end_date_field_name(entity)
    if end_date_field_name:
        end_date = getattr(entity, end_date_field_name)
        if end_date:
            end_time = _date_to_datetime(end_date)

    if start_time:
        context_registry.snapshot_context(entity).provided_start_time = \
            start_time

    if end_time:
        context_registry.snapshot_context(entity).provided_end_time = \
            end_time


def _get_booking_descendant_start_date_field_name(entity: Any) -> Optional[str]:
    """Returns field name, if one exists, on schema object that represents the
    entity's start date
    """
    return _BOOKING_DESCENDANT_START_DATE_FIELD.get(type(entity).__name__, None)


def _get_booking_descendant_end_date_field_name(entity: Any) -> Optional[str]:
    """Returns field name, if one exists, on schema object that represents the
    entity's end date
    """
    return _BOOKING_DESCENDANT_END_DATE_FIELD.get(type(entity).__name__, None)


def _write_snapshots(session: Session, context: '_SnapshotContext',
                     snapshot_time: datetime) -> None:
    """Writes snapshots for any new entities and any entities that have changes.

    If an entity has no existing snapshots and has a provided start time earlier
    than |snapshot_time|, will backdate the snapshot to the provided start time.

    If an entity has no existing snapshots and has a provided end time earlier
    than |snapshot_time|, will backdate the snapshot to the provided end time.
    (This assumes the entity has already been already been properly updated
    to reflect that it is completed, e.g. by marking a booking as released.)

    If an entity has no existing snapshots and has both a provided start time
    and provided end time earlier than |snapshot_time|, will create one
    snapshot from start time to end time reflecting the entity's state while
    it was active, and one open snapshot from end time reflecting the entity's
    current state (again assuming the entity has already been properly updated
    to reflect that it is completed).

    If there is a conflict between provided start and end time (i.e. start time
    is after end time), end time will govern.
    """

    # Historical table does not need to be updated if entity is not new and
    # its current state matches its most recent historical snapshot
    if context.most_recent_snapshot is not None and \
            _does_entity_match_historical_snapshot(
                    context.entity, context.most_recent_snapshot):
        return

    if context.most_recent_snapshot is None:
        _write_snapshots_for_new_entities(session, context, snapshot_time)
    else:
        _write_snapshots_for_existing_entities(session, context, snapshot_time)


def _write_snapshots_for_new_entities(
        session: Session, context: '_SnapshotContext',
        snapshot_time: datetime) -> None:
    """Writes snapshots for any new entities, including any required manual
    adjustments based on provided start and end times
    """
    historical_class = _get_historical_class(type(context.entity))
    new_historical_snapshot = historical_class()
    _copy_entity_fields_to_historical_snapshot(
        context.entity, new_historical_snapshot)

    provided_start_time = None
    provided_end_time = None

    # Validate provided start and end times
    if context.provided_start_time and \
            context.provided_start_time.date() < snapshot_time.date():
        provided_start_time = context.provided_start_time

    if context.provided_end_time and \
            context.provided_end_time.date() < snapshot_time.date():
        provided_end_time = context.provided_end_time

    if provided_start_time and provided_end_time and \
            provided_start_time >= provided_end_time:
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
        _copy_entity_fields_to_historical_snapshot(
            context.entity, initial_snapshot)
        initial_snapshot.valid_from = provided_start_time
        initial_snapshot.valid_to = provided_end_time

        if isinstance(context.entity, schema.Booking):
            initial_snapshot.custody_status = CustodyStatus.IN_CUSTODY.value
        elif isinstance(context.entity, schema.Sentence):
            initial_snapshot.status = SentenceStatus.PRESENT_WITHOUT_INFO.value
        else:
            raise NotImplementedError("Snapshot backdating not supported for "
                                      "type {}".format(type(context.entity)))

        session.merge(initial_snapshot)


def _write_snapshots_for_existing_entities(
        session: Session, context: '_SnapshotContext',
        snapshot_time: datetime) -> None:
    """Writes snapshot updates for entities that already have snapshots
    present in the database
    """
    historical_class = _get_historical_class(type(context.entity))
    new_historical_snapshot = historical_class()
    _copy_entity_fields_to_historical_snapshot(
        context.entity, new_historical_snapshot)
    new_historical_snapshot.valid_from = snapshot_time

    # Snapshot must be merged separately from record tree, as they are not
    # included in the ORM model relationships (to avoid needing to load
    # the entire snapshot chain at once)
    session.merge(new_historical_snapshot)

    # Close last snapshot if one is present
    if context.most_recent_snapshot is not None:
        context.most_recent_snapshot.valid_to = snapshot_time
        session.merge(context.most_recent_snapshot)


def _assert_all_root_entities_unique(
        root_entities: List[schema.DatabaseEntity]) -> None:
    """Raises (AssertionError) if any entity in |root_entities| does not
    have a unique primary key for its type
    """
    keys_by_type: Dict[str, Set[int]] = defaultdict(set)
    for entity in root_entities:
        type_name = type(entity).__name__
        key = entity.get_primary_key()
        if key in keys_by_type[type_name]:
            raise AssertionError(
                "Duplicate entities passed of type {} with ID {}".format(
                    type_name, key))
        keys_by_type[type_name].add(key)


def _execute_action_for_all_entities(start_entities: List[Any],
                                     action: Callable, *args) -> None:
    """For every entity in every graph reachable from |start_entities|,
    invokes |action|, passing the entity and |*args| as arguments"""

    unprocessed = list(start_entities)
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

    NOTE: This method *only* compares properties which are present on both the
    master and historical tables. Any property that is only present on one table
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

    NOTE: This method *only* copies values for properties which are present on
    both the master and historical tables. Any property that is only present on
    one table will be ignored. The only exception is the master key column,
    which is copied over regardless of property name (only based on *column*
    name), following module assumption #2.
    """
    for column_property_name in _get_shared_column_property_names(
            type(entity), type(historical_snapshot)):
        entity_value = getattr(entity, column_property_name)
        setattr(historical_snapshot, column_property_name, entity_value)

    # See module assumption #2
    key_column_name = entity.get_primary_key_column_name() # type: ignore
    historical_master_key_property_name = \
        type(historical_snapshot).get_property_name_by_column_name(
            key_column_name)
    setattr(historical_snapshot, historical_master_key_property_name,
            entity.get_primary_key()) # type: ignore


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


def _date_to_datetime(d: date) -> datetime:
    return datetime(d.year, d.month, d.day)


@attr.s
class _SnapshotContext:
    """Container for all data required for snapshot operations for a single
    entity
    """
    entity: Optional[Any] = attr.ib(default=None)
    most_recent_snapshot: Optional[Any] = attr.ib(default=None)
    provided_start_time: Optional[datetime] = attr.ib(default=None)
    provided_end_time: Optional[datetime] = attr.ib(default=None)


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
                "Entity already registered with type {type} and primary key "
                "{primary_key}".format(type=type_name, primary_key=entity_id))

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
                "Snapshot already registered for master entity with type "
                "{type} and primary key {primary_key}".format(
                    type=master_type_name, primary_key=master_entity_id))

        self.snapshot_contexts[master_type_name][master_entity_id] \
            .most_recent_snapshot = snapshot
