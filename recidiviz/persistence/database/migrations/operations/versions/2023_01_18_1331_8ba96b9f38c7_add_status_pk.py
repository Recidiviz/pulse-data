# pylint: skip-file
"""add_status_pk

Revision ID: 8ba96b9f38c7
Revises: f9d5785088fc
Create Date: 2023-01-18 13:31:15.261337

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8ba96b9f38c7"
down_revision = "f9d5785088fc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_primary_key(
        "direct_ingest_instance_status_pkey",
        "direct_ingest_instance_status",
        ["region_code", "status_timestamp", "instance"],
    )


def downgrade() -> None:
    # DO NOTHING - the PK will get dropped when the table gets dropped in
    # a preceding downgrade() call.
    pass
