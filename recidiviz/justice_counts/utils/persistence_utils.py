# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Contains utilities for writing JusticeCountsDatabaseEntitites to the DB."""

from typing import Optional, Type

from sqlalchemy import cast
from sqlalchemy.orm import Session
from sqlalchemy.sql.schema import UniqueConstraint

from recidiviz.persistence.database.base_schema import JusticeCountsBase
from recidiviz.persistence.database.schema.justice_counts import schema


def get_existing_entity(
    ingested_entity: schema.JusticeCountsDatabaseEntity, session: Session
) -> Optional[JusticeCountsBase]:
    table = ingested_entity.__table__
    [unique_constraint] = [
        constraint
        for constraint in table.constraints
        if isinstance(constraint, UniqueConstraint)
    ]
    query = session.query(table)
    for column in unique_constraint:
        # TODO(#4477): Instead of making an assumption about how the property name is formed from the column name, use
        # an Entity method here to follow the foreign key relationship.
        if column.name.endswith("_id"):
            value = getattr(ingested_entity, column.name[: -len("_id")]).id
        else:
            value = getattr(ingested_entity, column.name)
        # Cast to the type because array types aren't deduced properly.
        query = query.filter(column == cast(value, column.type))
    table_entity: Optional[JusticeCountsBase] = query.first()
    return table_entity


def update_existing_or_create(
    ingested_entity: schema.JusticeCountsDatabaseEntity, session: Session
) -> schema.JusticeCountsDatabaseEntity:
    expunge_existing(session=session, ingested_entity=ingested_entity)
    # Note: Using on_conflict_do_update to resolve whether there is an existing entity could be more efficient as it
    # wouldn't incur multiple roundtrips. However for some entities we need to know whether there is an existing entity
    # (e.g. table instance) so we can clear child entities, so we probably wouldn't win much if anything.
    table_entity = get_existing_entity(ingested_entity, session)
    if table_entity is not None:
        # TODO(#4477): Instead of assuming the primary key field is named `id`, use an Entity method.
        ingested_entity.id = table_entity.id
        # TODO(#4477): Merging here doesn't seem perfect, although it should work so long as the given entity always has
        # all the properties set explicitly. To avoid the merge, the method could instead take in the entity class as
        # one parameter and the parameters to construct it separately and then query based on those parameters. However
        # this would likely make mypy less useful.
        merged_entity = session.merge(ingested_entity)
        return merged_entity
    session.add(ingested_entity)
    return ingested_entity


def delete_existing(
    session: Session,
    ingested_entity: schema.JusticeCountsDatabaseEntity,
    entity_cls: Type[schema.JusticeCountsDatabaseEntity],
) -> None:
    expunge_existing(session=session, ingested_entity=ingested_entity)
    table_entity = get_existing_entity(ingested_entity, session)
    if table_entity is not None:
        table = ingested_entity.__table__
        # TODO(#4477): need to have a better way to identify id since below method doesn't guarantee id is a valid attr
        delete_q = table.delete().where(entity_cls.id == table_entity.id)  # type: ignore[attr-defined]
        session.execute(delete_q)


def delete_existing_and_create(
    session: Session,
    ingested_entity: schema.JusticeCountsDatabaseEntity,
    entity_cls: Type[schema.JusticeCountsDatabaseEntity],
) -> schema.JusticeCountsDatabaseEntity:
    delete_existing(
        session=session, ingested_entity=ingested_entity, entity_cls=entity_cls
    )
    session.add(ingested_entity)
    return ingested_entity


def expunge_existing(
    session: Session,
    ingested_entity: schema.JusticeCountsDatabaseEntity,
) -> None:
    """In certain cases, the act of creating the `ingested_entity` will also add it to the session.
    Then, then we perform the query in `get_existing_entity`, this will perform an auto-flush,
    and we'll try to insert the `ingested_entity`, even if an existing one already exists,
    causing a constraint violation error. To avoid, we can remove the `ingested_entity` from the
    session here. It will be added back at the end of this method.
    See https://stackoverflow.com/questions/17116277/creating-sqlalchemy-object-outside-of-session
    """
    if ingested_entity not in session:
        return

    session.expunge(ingested_entity)
