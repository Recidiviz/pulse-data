# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
Resource CRUD operations
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, List, Optional, Sequence

from geoalchemy2 import WKBElement
from geoalchemy2.functions import ST_DWithin, ST_GeogFromWKB
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from recidiviz.persistence.database.schema.resource_search import schema
from recidiviz.resource_search.src.embedding.helpers import cosine_distance_comparator
from recidiviz.resource_search.src.exceptions import NotFoundError
from recidiviz.resource_search.src.external_apis.distance import miles_to_meters
from recidiviz.resource_search.src.models.resource import (  # ResourceUpdate,
    ResourceCreate,
)
from recidiviz.resource_search.src.typez.crud.resources import ResourceQueryParams


async def get_resources(
    session: AsyncSession, query: ResourceQueryParams
) -> Sequence[schema.Resource]:
    """Get all resources from the database"""
    target_geo = ST_GeogFromWKB(WKBElement(query.position.wkb))
    distance_meters = miles_to_meters(query.distance)

    stmt = (
        select(schema.Resource)
        .where(schema.Resource.category == query.category)
        .where(schema.Resource.subcategory == query.subcategory)
        .where(schema.Resource.banned.is_(False))
        .where(ST_DWithin(schema.Resource.location, target_geo, distance_meters))
        .order_by(cosine_distance_comparator(query.embedding))
        .limit(query.limit)
        .offset(query.offset)
    )

    result = await session.scalars(stmt)
    return result.all()


async def get_resource(
    session: AsyncSession, resource_id: uuid.UUID
) -> schema.Resource:
    """Get a resource from the database"""
    stmt = select(schema.Resource).where(schema.Resource.id == resource_id)
    result = await session.scalars(stmt)
    resource = result.first()
    if resource is None:
        raise NotFoundError(schema.Resource, resource_id)
    return resource


async def delete_resource(session: AsyncSession, resource_id: uuid.UUID) -> bool:
    """Delete a resource from the database"""
    resource = await get_resource(session, resource_id)
    await session.delete(resource)
    return True


async def upsert_resources(
    session: AsyncSession, data: List[ResourceCreate]
) -> List[schema.Resource]:
    """Batch insert resources using ON CONFLICT DO NOTHING on appropriate unique constraints, logging conflicts."""
    resources: List[schema.Resource] = []
    for resource_data in data:
        values = resource_data.model_dump()
        if not values.get("created_at"):
            values["created_at"] = datetime.now(timezone.utc)
        stmt = insert(schema.Resource).values(**values)
        if values.get("phone"):
            stmt = stmt.on_conflict_do_nothing(index_elements=["phone"])
            constraint_name = "phone"
        elif all(
            values.get(f) for f in ["normalized_name", "street", "city", "state", "zip"]
        ):
            stmt = stmt.on_conflict_do_nothing(
                index_elements=["normalized_name", "street", "city", "state", "zip"]
            )
            constraint_name = "normalized_name+street+city+state+zip"
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=["category", "uri"])
            constraint_name = "category+uri"
        try:
            result = await session.execute(stmt)
            if len(result.all()) == 0:
                logging.warning(
                    "Resource conflict on %s for values: %s. Skipping insert.",
                    constraint_name,
                    values,
                )
            else:
                # Fetch the inserted resource from the DB
                select_stmt = select(schema.Resource).where(
                    schema.Resource.uri == values.get("uri")
                )
                db_result = await session.scalars(select_stmt)
                db_resource = db_result.first()
                if db_resource:
                    resources.append(db_resource)
        except SQLAlchemyError as e:
            logging.error("Error inserting resource: %s | values: %s", e, values)
            continue
    return resources


async def save_with_unique_handling(
    session: AsyncSession,
    resource: ResourceCreate,
    get_existing_fn: Optional[Callable] = None,
) -> Optional[schema.Resource]:
    try:
        session.add(resource)
        await session.flush()
        return resource  # type: ignore
    except IntegrityError as e:
        await session.rollback()
        logging.warning("Unique constraint error for resource %s: %s", resource, e)
        if get_existing_fn is not None:
            return await get_existing_fn(session, resource)
        return None
    except Exception as e:
        await session.rollback()
        logging.error("Unexpected error saving resource %s: %s", resource, e)
        raise


async def save_resources(
    session: AsyncSession, resources: List[ResourceCreate]
) -> tuple[list[schema.Resource], list[ResourceCreate]]:
    saved: list[schema.Resource] = []
    skipped: list[ResourceCreate] = []
    for resource in resources:
        try:
            async with session.begin_nested():
                session.add(resource)
                await session.flush()
                # Fetch the inserted resource from the DB
                select_stmt = select(schema.Resource).where(
                    schema.Resource.uri == resource.uri
                )
                db_result = await session.scalars(select_stmt)
                db_resource = db_result.first()
                if db_resource:
                    saved.append(db_resource)
        except IntegrityError as e:
            skipped.append(resource)
            logging.warning("Unique constraint error for resource %s: %s", resource, e)
            await session.rollback()
        except Exception as e:
            skipped.append(resource)
            logging.error("Unexpected error for resource %s: %s", resource, e)
            await session.rollback()
    return saved, skipped


async def upsert_resource(session: AsyncSession, resource_data: ResourceCreate) -> None:
    stmt = insert(schema.Resource).values(**resource_data)
    stmt = stmt.on_conflict_do_update(
        index_elements=["unique_field"], set_=resource_data
    )
    await session.execute(stmt)
    await session.commit()


def handle_db_errors(func: Callable) -> Callable:
    async def wrapper(*args: Any, **kwargs: Any) -> None:
        session = args[0]
        try:
            return await func(*args, **kwargs)
        except IntegrityError as e:
            await session.rollback()
            logging.warning("Integrity error: %s", e)
            # handle or re-raise
        except Exception as e:
            await session.rollback()
            logging.error("Unexpected DB error: %s", e)
            raise

    return wrapper
