"""Method to update historical snapshots for a database entity"""

from sqlalchemy.sql import text

from recidiviz.persistence.database import schema


def update_historical_snapshots(session, entity, snapshot_time):
    """If any columns of |entity| differ from the most recent historical
    snapshot, closes existing snapshot and adds new snapshot incorporating those
    changes.

    NOTE: This method makes 2 assumptions about the historical tables:
    1) The table ORM class names are: <ENTITY> and <ENTITY>History (e.g. Person
        and PersonHistory)
    2) The ORM class for the master entity, the ORM class for the historical
        snapshot, the table for the master entity, and the table for the
        historical snapshot all use the same name to refer to the persistent
        stable identifier for an object over time (e.g. all these locations
        will use "person_id" to refer to the stable identifier representing
        a single person over time).
    If either of these assumptions are broken, this method will not behave as
    expected.

    Args:
        session: (Session)
        entity: Schema object
        snapshot_time: (datetime) to close outdated snapshots and open new
            snapshots
    """

    master_table_class_name = type(entity).__name__
    historical_table_class_name = '{}History'.format(master_table_class_name)
    historical_table_class = getattr(schema, historical_table_class_name)
    key_column_name = _get_primary_key_column_name(entity.__table__)
    master_entity_id = getattr(entity, key_column_name)

    most_recent_historical_snapshot = _get_most_recent_historical_snapshot(
        session, entity, historical_table_class)

    # Historical table does not need to be updated if entity is not new and
    # its current state matches its most recent historical snapshot
    if most_recent_historical_snapshot is not None and \
            _does_entity_match_historical_snapshot(
                    entity, most_recent_historical_snapshot):
        return

    new_historical_snapshot = historical_table_class()
    _copy_entity_fields_to_historical_snapshot(
        entity, new_historical_snapshot)
    new_historical_snapshot.valid_from = snapshot_time
    setattr(new_historical_snapshot, key_column_name, master_entity_id)

    # Snapshot must be merged separately from record tree, as they are not
    # included in the ORM model relationships (to avoid needing to load
    # the entire snapshot chain at once)
    session.merge(new_historical_snapshot)

    # Close last snapshot if one is present
    if most_recent_historical_snapshot is not None:
        most_recent_historical_snapshot.valid_to = snapshot_time
        session.merge(most_recent_historical_snapshot)


def _get_most_recent_historical_snapshot(session, entity,
                                         historical_table_class):
    """Returns the most recent historical snapshot corresponding to |entity| if
    one exists, otherwise returns (None).
    """

    key_column_name = _get_primary_key_column_name(entity.__table__)
    master_entity_id = getattr(entity, key_column_name)

    # Get name of historical table in database (as distinct from name of ORM
    # class representing historical table in code)
    historical_table_name = historical_table_class.__table__.name

    filter_statement = \
        '{historical_table}.{master_foreign_key} = {master_entity_id}'.format(
            historical_table=historical_table_name,
            master_foreign_key=key_column_name,
            master_entity_id=master_entity_id)

    return session.query(historical_table_class) \
        .filter(text(filter_statement)) \
        .order_by(historical_table_class.valid_from.desc()) \
        .first()


def _get_primary_key_column_name(table):
    """Returns the name of the primary key column for |table|.

    NOTE: This returns the *column* name. This is not guaranteed to be equal to
    the *attribute* name on the ORM object, but this module assumes they will be
    equal. If this assumption is violated, callers of this method will not
    behave as expected.
    """

    # table.primary_key returns a PrimaryKeyConstraint containing a single
    # column
    return table.primary_key.columns.values()[0].name


def _does_entity_match_historical_snapshot(entity, historical_snapshot):
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


def _copy_entity_fields_to_historical_snapshot(entity, historical_snapshot):
    """Copies all column values present on |entity| to |historical_snapshot|.

    NOTE: This method *only* copies values for columns which are present on
    both the master and historical tables. Any column that is only present on
    one table will be ignored.
    """

    for column_property_name in _get_shared_column_property_names(
            type(entity), type(historical_snapshot)):
        entity_value = getattr(entity, column_property_name)
        setattr(historical_snapshot, column_property_name, entity_value)


def _get_shared_column_property_names(entity_class_a, entity_class_b):
    """Returns a set of all column property names shared between
    |entity_class_a| and |entity_class_b|.
    """
    return entity_class_a.get_column_property_names().intersection(
        entity_class_b.get_column_property_names())
