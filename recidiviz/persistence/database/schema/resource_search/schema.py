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
# ============================================================================

"""Define the ORM schema objects that map directly to the database
for the Research Search application.
"""

from geoalchemy2 import Geometry
from pgvector.sqlalchemy import Vector  # type: ignore
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeMeta, declarative_base, relationship
from sqlalchemy.sql.sqltypes import TIMESTAMP, Text

from recidiviz.persistence.database.database_entity import DatabaseEntity

# Defines the base class for all table classes in the resource search schema.
ResourceSearchBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="ResourceSearchBase"
)


class Resource(ResourceSearchBase):
    """
    Represents a transitional resource such as housing, employment services, or legal aid.

    Each resource entry includes identifying information, contact details, geographic location,
    and optional metadata from external sources like Google Places.
    """

    __tablename__ = "resource"

    id = Column(Integer, autoincrement=True, primary_key=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=True)

    uri = Column(String, nullable=False)
    # Stores geographic point in WGS 84 (SRID 4326), the standard used by GPS and Google Maps.
    # Enables location-aware querying (e.g., "resources within 10 miles").
    location = Column(Geometry("POINT", srid=4326), nullable=True)
    # OpenAI-generated embedding vector (1536-dim) used for semantic search.
    # Stored using the pgvector extension to enable similarity search with the <-> operator.
    embedding = Column(Vector(1536), nullable=True)
    category = Column(String, nullable=True)
    origin = Column(String, nullable=True)

    name = Column(String, nullable=True)
    normalized_name = Column(String, nullable=True)
    street = Column(String, nullable=True)
    city = Column(String, nullable=True)
    state = Column(String, nullable=True)
    zip = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    email = Column(String, nullable=True)
    website = Column(String, nullable=True)
    maps_url = Column(String, nullable=True)

    description = Column(String, nullable=True)
    tags = Column(ARRAY(String), nullable=True)

    banned = Column(Boolean, nullable=False, default=False)
    banned_reason = Column(String, nullable=True)
    llm_rank = Column(Integer, nullable=True)
    llm_valid = Column(Boolean, nullable=True)
    # Cached average score across all ResourceScore entries associated with this resource.
    # Automatically updated by a trigger whenever a ResourceScore is inserted, updated, or deleted.
    score = Column(Integer, nullable=False, default=5)

    embedding_text = Column(Text, nullable=True)
    extra_data = Column(
        JSONB,
        nullable=True,
    )
    subcategory = Column(String, nullable=True)

    google_places_rating = Column(Float, nullable=True)
    google_places_rating_count = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint("category", "uri"),
        Index(
            "unique_phone",
            "phone",
            unique=True,
            postgresql_where=text("phone IS NOT NULL"),
        ),
        Index(
            "unique_website",
            "website",
            unique=True,
            postgresql_where=text("website IS NOT NULL"),
        ),
        Index(
            "unique_name_address",
            "normalized_name",
            "street",
            "city",
            "state",
            "zip",
            unique=True,
            postgresql_where=text(
                "normalized_name IS NOT NULL AND street IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL AND zip IS NOT NULL"
            ),
        ),
    )

    scores = relationship("ResourceScore", back_populates="resource")


class ResourceScore(ResourceSearchBase):
    """
    Represents a single score assigned to a resource.

    This table supports a one-to-many relationship with the Resource table:
    each resource can have multiple associated scores.
    """

    __tablename__ = "resource_score"

    id = Column(Integer, autoincrement=True, primary_key=True)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), nullable=True)
    score = Column(Float, nullable=False, server_default=text("'5'::double precision"))

    resource_id = Column(Integer, nullable=False)
    resource = relationship("Resource", back_populates="scores")

    __table_args__ = (ForeignKeyConstraint(["resource_id"], ["resource.id"]),)


class ScrapeSource(ResourceSearchBase):
    """Tracks previously scraped pages to avoid reprocessing unchanged content."""

    __tablename__ = "scrape_source"

    # The URL of the scraped page (used as primary key)
    url = Column(String, primary_key=True)

    # Timestamp of when the page was last scraped
    last_scraped_at = Column(DateTime, nullable=False, server_default=text("now()"))

    # SHA-256 hash of the normalized page content (used to detect changes)
    content_hash = Column(String(64), nullable=False)
