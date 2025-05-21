# pylint: skip-file
"""init DBs

Revision ID: 4babcbd54dbd
Revises:
Create Date: 2025-05-15 22:43:36.851237

"""
import geoalchemy2
import pgvector  # type: ignore
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "4babcbd54dbd"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS postgis")
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "resource",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("uri", sa.String(), nullable=False),
        sa.Column(
            "location",
            geoalchemy2.types.Geometry(
                geometry_type="POINT",
                srid=4326,
                from_text="ST_GeomFromEWKT",
                name="geometry",
            ),
            nullable=True,
        ),
        sa.Column(
            "embedding",
            pgvector.sqlalchemy.vector.VECTOR(dim=1536),
            nullable=True,
        ),
        sa.Column("category", sa.String(), nullable=True),
        sa.Column("origin", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column("normalized_name", sa.String(), nullable=True),
        sa.Column("street", sa.String(), nullable=True),
        sa.Column("city", sa.String(), nullable=True),
        sa.Column("state", sa.String(), nullable=True),
        sa.Column("zip", sa.String(), nullable=True),
        sa.Column("phone", sa.String(), nullable=True),
        sa.Column("email", sa.String(), nullable=True),
        sa.Column("website", sa.String(), nullable=True),
        sa.Column("maps_url", sa.String(), nullable=True),
        sa.Column("description", sa.String(), nullable=True),
        sa.Column("tags", postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column("banned", sa.Boolean(), nullable=False),
        sa.Column("banned_reason", sa.String(), nullable=True),
        sa.Column("llm_rank", sa.Integer(), nullable=True),
        sa.Column("llm_valid", sa.Boolean(), nullable=True),
        sa.Column("score", sa.Integer(), nullable=False),
        sa.Column("embedding_text", sa.Text(), nullable=True),
        sa.Column(
            "extra_data",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("subcategory", sa.String(), nullable=True),
        sa.Column("google_places_rating", sa.Float(), nullable=True),
        sa.Column("google_places_rating_count", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("category", "uri"),
    )
    op.create_index(
        "unique_name_address",
        "resource",
        ["normalized_name", "street", "city", "state", "zip"],
        unique=True,
        postgresql_where=sa.text(
            "normalized_name IS NOT NULL AND street IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL AND zip IS NOT NULL"
        ),
    )
    op.create_index(
        "unique_phone",
        "resource",
        ["phone"],
        unique=True,
        postgresql_where=sa.text("phone IS NOT NULL"),
    )
    op.create_index(
        "unique_website",
        "resource",
        ["website"],
        unique=True,
        postgresql_where=sa.text("website IS NOT NULL"),
    )
    op.create_table(
        "scrape_source",
        sa.Column("url", sa.String(), nullable=False),
        sa.Column(
            "last_scraped_at",
            sa.DateTime(),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.PrimaryKeyConstraint("url"),
    )
    op.create_table(
        "resource_score",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column(
            "score",
            sa.Float(),
            server_default=sa.text("'5'::double precision"),
            nullable=False,
        ),
        sa.Column("resource_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(
            ["resource_id"],
            ["resource.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    op.execute("DROP EXTENSION IF NOT EXISTS postgis")
    op.execute("DROP EXTENSION IF NOT EXISTS vector")

    op.drop_table("resource_score")
    op.drop_table("scrape_source")
    op.drop_index(
        "unique_website",
        table_name="resource",
        postgresql_where=sa.text("website IS NOT NULL"),
    )
    op.drop_index(
        "unique_phone",
        table_name="resource",
        postgresql_where=sa.text("phone IS NOT NULL"),
    )
    op.drop_index(
        "unique_name_address",
        table_name="resource",
        postgresql_where=sa.text(
            "normalized_name IS NOT NULL AND street IS NOT NULL AND city IS NOT NULL AND state IS NOT NULL AND zip IS NOT NULL"
        ),
    )
    op.drop_index(
        "idx_resource_location", table_name="resource", postgresql_using="gist"
    )
    op.drop_table("resource")
    # ### end Alembic commands ###
