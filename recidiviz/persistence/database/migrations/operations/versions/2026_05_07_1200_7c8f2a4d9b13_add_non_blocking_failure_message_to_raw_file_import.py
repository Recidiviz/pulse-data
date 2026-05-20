# pylint: skip-file
"""add_non_blocking_failure_message_to_raw_file_import

Revision ID: 7c8f2a4d9b13
Revises: 53076122bfd5
Create Date: 2026-05-07 12:00:00.000000

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7c8f2a4d9b13"
down_revision = "53076122bfd5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "direct_ingest_raw_file_import",
        sa.Column("non_blocking_failure_message", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("direct_ingest_raw_file_import", "non_blocking_failure_message")
