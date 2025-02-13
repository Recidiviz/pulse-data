# pylint: skip-file
"""initial_migration

Revision ID: 757e4457a1c7
Revises: 
Create Date: 2020-05-14 20:41:44.758364

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "757e4457a1c7"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "direct_ingest_ingest_file_metadata",
        sa.Column("file_id", sa.Integer(), nullable=False),
        sa.Column("region_code", sa.String(length=255), nullable=False),
        sa.Column("file_tag", sa.String(length=255), nullable=False),
        sa.Column("normalized_file_name", sa.String(length=255), nullable=True),
        sa.Column("discovery_time", sa.DateTime(), nullable=True),
        sa.Column("processed_time", sa.DateTime(), nullable=True),
        sa.Column("is_invalidated", sa.Boolean(), nullable=False),
        sa.Column("job_creation_time", sa.DateTime(), nullable=False),
        sa.Column(
            "datetimes_contained_lower_bound_exclusive", sa.DateTime(), nullable=True
        ),
        sa.Column(
            "datetimes_contained_upper_bound_inclusive", sa.DateTime(), nullable=False
        ),
        sa.Column("export_time", sa.DateTime(), nullable=True),
        sa.CheckConstraint(
            "(normalized_file_name IS NULL AND export_time IS NULL) OR (normalized_file_name IS NOT NULL AND export_time IS NOT NULL)",
            name="normalized_file_with_export",
        ),
        sa.CheckConstraint(
            "datetimes_contained_lower_bound_exclusive IS NULL OR datetimes_contained_lower_bound_exclusive < datetimes_contained_upper_bound_inclusive",
            name="datetimes_contained_ordering",
        ),
        sa.CheckConstraint(
            "discovery_time IS NULL OR export_time IS NOT NULL",
            name="discovery_after_export",
        ),
        sa.CheckConstraint(
            "processed_time IS NULL OR discovery_time IS NOT NULL",
            name="processed_after_discovery",
        ),
        sa.PrimaryKeyConstraint("file_id"),
    )
    op.create_index(
        op.f("ix_direct_ingest_ingest_file_metadata_file_tag"),
        "direct_ingest_ingest_file_metadata",
        ["file_tag"],
        unique=False,
    )
    op.create_index(
        op.f("ix_direct_ingest_ingest_file_metadata_normalized_file_name"),
        "direct_ingest_ingest_file_metadata",
        ["normalized_file_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_direct_ingest_ingest_file_metadata_region_code"),
        "direct_ingest_ingest_file_metadata",
        ["region_code"],
        unique=False,
    )
    op.create_table(
        "direct_ingest_raw_file_metadata",
        sa.Column("file_id", sa.Integer(), nullable=False),
        sa.Column("region_code", sa.String(length=255), nullable=False),
        sa.Column("file_tag", sa.String(length=255), nullable=False),
        sa.Column("normalized_file_name", sa.String(length=255), nullable=True),
        sa.Column("discovery_time", sa.DateTime(), nullable=True),
        sa.Column("processed_time", sa.DateTime(), nullable=True),
        sa.Column(
            "datetimes_contained_upper_bound_inclusive", sa.DateTime(), nullable=False
        ),
        sa.CheckConstraint(
            "discovery_time IS NOT NULL", name="nonnull_raw_file_discovery_time"
        ),
        sa.CheckConstraint(
            "normalized_file_name IS NOT NULL", name="nonnull_raw_normalized_file_name"
        ),
        sa.PrimaryKeyConstraint("file_id"),
        sa.UniqueConstraint(
            "region_code", "normalized_file_name", name="one_normalized_name_per_region"
        ),
    )
    op.create_index(
        op.f("ix_direct_ingest_raw_file_metadata_file_tag"),
        "direct_ingest_raw_file_metadata",
        ["file_tag"],
        unique=False,
    )
    op.create_index(
        op.f("ix_direct_ingest_raw_file_metadata_normalized_file_name"),
        "direct_ingest_raw_file_metadata",
        ["normalized_file_name"],
        unique=False,
    )
    op.create_index(
        op.f("ix_direct_ingest_raw_file_metadata_region_code"),
        "direct_ingest_raw_file_metadata",
        ["region_code"],
        unique=False,
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(
        op.f("ix_direct_ingest_raw_file_metadata_region_code"),
        table_name="direct_ingest_raw_file_metadata",
    )
    op.drop_index(
        op.f("ix_direct_ingest_raw_file_metadata_normalized_file_name"),
        table_name="direct_ingest_raw_file_metadata",
    )
    op.drop_index(
        op.f("ix_direct_ingest_raw_file_metadata_file_tag"),
        table_name="direct_ingest_raw_file_metadata",
    )
    op.drop_table("direct_ingest_raw_file_metadata")
    op.drop_index(
        op.f("ix_direct_ingest_ingest_file_metadata_region_code"),
        table_name="direct_ingest_ingest_file_metadata",
    )
    op.drop_index(
        op.f("ix_direct_ingest_ingest_file_metadata_normalized_file_name"),
        table_name="direct_ingest_ingest_file_metadata",
    )
    op.drop_index(
        op.f("ix_direct_ingest_ingest_file_metadata_file_tag"),
        table_name="direct_ingest_ingest_file_metadata",
    )
    op.drop_table("direct_ingest_ingest_file_metadata")
    # ### end Alembic commands ###
