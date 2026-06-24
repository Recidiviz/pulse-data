# pylint: skip-file
"""add_pipeline_type_to_dataflow_tables

Revision ID: a91841509ed8
Revises: 7c8f2a4d9b13
Create Date: 2026-06-23 12:30:00.000000

Adds a `pipeline_type` column (`ingest_pipeline_type` enum) to both
`direct_ingest_dataflow_job` and `direct_ingest_dataflow_raw_table_upper_bounds`.

Sets `server_default='ACTIVITY'` on the new column to make Postgres backfill every
existing row with `ACTIVITY` as it adds the column, and ensure that every future
`INSERT` that omits `pipeline_type` also gets `ACTIVITY`.

A future PR will parameterize every read/write path by `IngestPipelineType` and drop
the `server_default`.
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "a91841509ed8"
down_revision = "7c8f2a4d9b13"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ingest_pipeline_type_enum = postgresql.ENUM(
        "ACTIVITY", "IDENTITY", name="ingest_pipeline_type"
    )
    ingest_pipeline_type_enum.create(op.get_bind())

    op.add_column(
        "direct_ingest_dataflow_job",
        sa.Column(
            "pipeline_type",
            postgresql.ENUM(
                "ACTIVITY", "IDENTITY", name="ingest_pipeline_type", create_type=False
            ),
            nullable=False,
            server_default="ACTIVITY",
        ),
    )
    op.create_index(
        op.f("ix_direct_ingest_dataflow_job_pipeline_type"),
        "direct_ingest_dataflow_job",
        ["pipeline_type"],
        unique=False,
    )

    op.add_column(
        "direct_ingest_dataflow_raw_table_upper_bounds",
        sa.Column(
            "pipeline_type",
            postgresql.ENUM(
                "ACTIVITY", "IDENTITY", name="ingest_pipeline_type", create_type=False
            ),
            nullable=False,
            server_default="ACTIVITY",
        ),
    )
    op.create_index(
        op.f("ix_direct_ingest_dataflow_raw_table_upper_bounds_pipeline_type"),
        "direct_ingest_dataflow_raw_table_upper_bounds",
        ["pipeline_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_direct_ingest_dataflow_raw_table_upper_bounds_pipeline_type"),
        table_name="direct_ingest_dataflow_raw_table_upper_bounds",
    )
    op.drop_column("direct_ingest_dataflow_raw_table_upper_bounds", "pipeline_type")
    op.drop_index(
        op.f("ix_direct_ingest_dataflow_job_pipeline_type"),
        table_name="direct_ingest_dataflow_job",
    )
    op.drop_column("direct_ingest_dataflow_job", "pipeline_type")
    postgresql.ENUM("ACTIVITY", "IDENTITY", name="ingest_pipeline_type").drop(
        op.get_bind()
    )
