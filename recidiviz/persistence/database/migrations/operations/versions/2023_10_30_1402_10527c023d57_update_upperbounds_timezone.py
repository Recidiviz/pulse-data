# pylint: skip-file
"""update_upperbounds_timezone

Revision ID: 10527c023d57
Revises: d06dc4a420ce
Create Date: 2023-10-30 14:02:09.564737

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "10527c023d57"
down_revision = "d06dc4a420ce"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column(
        "direct_ingest_dataflow_raw_table_upper_bounds", "watermark_datetime"
    )
    op.add_column(
        "direct_ingest_dataflow_raw_table_upper_bounds",
        sa.Column(
            "watermark_datetime", postgresql.TIMESTAMP(timezone=True), nullable=False
        ),
    )


def downgrade() -> None:
    op.drop_column(
        "direct_ingest_dataflow_raw_table_upper_bounds", "watermark_datetime"
    )
    op.add_column(
        "direct_ingest_dataflow_raw_table_upper_bounds",
        sa.Column("watermark_datetime", postgresql.TIMESTAMP(), nullable=False),
    )
