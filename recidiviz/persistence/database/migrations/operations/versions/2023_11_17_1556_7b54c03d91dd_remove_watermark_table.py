# pylint: skip-file
"""remove_watermark_table

Revision ID: 7b54c03d91dd
Revises: 10527c023d57
Create Date: 2023-11-17 15:56:17.848366

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "7b54c03d91dd"
down_revision = "10527c023d57"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "ix_direct_ingest_dataflow_raw_table_upper_bounds_job_id",
        table_name="direct_ingest_dataflow_raw_table_upper_bounds",
    )
    op.drop_index(
        "ix_direct_ingest_dataflow_raw_table_upper_bounds_raw_da_0f75",
        table_name="direct_ingest_dataflow_raw_table_upper_bounds",
    )
    op.drop_index(
        "ix_direct_ingest_dataflow_raw_table_upper_bounds_region_code",
        table_name="direct_ingest_dataflow_raw_table_upper_bounds",
    )
    op.drop_table("direct_ingest_dataflow_raw_table_upper_bounds")
    op.drop_index(
        "ix_direct_ingest_dataflow_job_ingest_instance",
        table_name="direct_ingest_dataflow_job",
    )
    op.drop_index(
        "ix_direct_ingest_dataflow_job_region_code",
        table_name="direct_ingest_dataflow_job",
    )
    op.drop_table("direct_ingest_dataflow_job")


def downgrade() -> None:
    op.create_table(
        "direct_ingest_dataflow_job",
        sa.Column(
            "job_id", sa.VARCHAR(length=255), autoincrement=False, nullable=False
        ),
        sa.Column(
            "region_code", sa.VARCHAR(length=255), autoincrement=False, nullable=False
        ),
        sa.Column(
            "ingest_instance",
            postgresql.ENUM(
                "PRIMARY", "SECONDARY", name="direct_ingest_instance", create_type=False
            ),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column(
            "completion_time",
            postgresql.TIMESTAMP(),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("is_invalidated", sa.BOOLEAN(), autoincrement=False, nullable=False),
        sa.PrimaryKeyConstraint("job_id", name="direct_ingest_dataflow_job_pkey"),
    )
    op.create_index(
        "ix_direct_ingest_dataflow_job_region_code",
        "direct_ingest_dataflow_job",
        ["region_code"],
        unique=False,
    )
    op.create_index(
        "ix_direct_ingest_dataflow_job_ingest_instance",
        "direct_ingest_dataflow_job",
        ["ingest_instance"],
        unique=False,
    )

    op.create_table(
        "direct_ingest_dataflow_raw_table_upper_bounds",
        sa.Column(
            "region_code", sa.VARCHAR(length=255), autoincrement=False, nullable=False
        ),
        sa.Column(
            "raw_data_file_tag",
            sa.VARCHAR(length=255),
            autoincrement=False,
            nullable=False,
        ),
        sa.Column("watermark_id", sa.INTEGER(), autoincrement=False, nullable=False),
        sa.Column(
            "job_id", sa.VARCHAR(length=255), autoincrement=False, nullable=False
        ),
        sa.Column(
            "watermark_datetime",
            postgresql.TIMESTAMP(timezone=True),
            autoincrement=False,
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["direct_ingest_dataflow_job.job_id"],
            name="direct_ingest_dataflow_raw_table_upper_bounds_job_id_fkey",
            initially="DEFERRED",
            deferrable=True,
        ),
        sa.PrimaryKeyConstraint(
            "watermark_id", name="direct_ingest_dataflow_raw_table_upper_bounds_pkey"
        ),
        sa.UniqueConstraint(
            "job_id", "raw_data_file_tag", name="file_tags_unique_within_pipeline"
        ),
    )
    op.create_index(
        "ix_direct_ingest_dataflow_raw_table_upper_bounds_region_code",
        "direct_ingest_dataflow_raw_table_upper_bounds",
        ["region_code"],
        unique=False,
    )
    op.create_index(
        "ix_direct_ingest_dataflow_raw_table_upper_bounds_raw_da_0f75",
        "direct_ingest_dataflow_raw_table_upper_bounds",
        ["raw_data_file_tag"],
        unique=False,
    )
    op.create_index(
        "ix_direct_ingest_dataflow_raw_table_upper_bounds_job_id",
        "direct_ingest_dataflow_raw_table_upper_bounds",
        ["job_id"],
        unique=False,
    )
