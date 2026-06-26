# pylint: skip-file
"""drop_pipeline_type_server_default

Revision ID: b51842610fe9
Revises: 9ac715eedb6f
Create Date: 2026-06-23 13:00:00.000000

Drops the `server_default='ACTIVITY'` from the `pipeline_type` column on both
`direct_ingest_dataflow_job` and `direct_ingest_dataflow_raw_table_upper_bounds`
now that every write path explicitly specifies pipeline_type.
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "b51842610fe9"
down_revision = "9ac715eedb6f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("direct_ingest_dataflow_job", "pipeline_type", server_default=None)
    op.alter_column(
        "direct_ingest_dataflow_raw_table_upper_bounds",
        "pipeline_type",
        server_default=None,
    )


def downgrade() -> None:
    op.alter_column(
        "direct_ingest_dataflow_job", "pipeline_type", server_default="ACTIVITY"
    )
    op.alter_column(
        "direct_ingest_dataflow_raw_table_upper_bounds",
        "pipeline_type",
        server_default="ACTIVITY",
    )
