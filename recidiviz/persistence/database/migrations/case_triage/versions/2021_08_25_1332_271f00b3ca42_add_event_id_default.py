# pylint: skip-file
"""add_event_id_default

Revision ID: 271f00b3ca42
Revises: 2e0398d9283d
Create Date: 2021-08-25 13:32:58.924590

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "271f00b3ca42"
down_revision = "2e0398d9283d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "etl_client_events",
        "event_id",
        server_default=sa.text("gen_random_uuid()"),
    )


def downgrade() -> None:
    op.alter_column(
        "etl_client_events",
        "event_id",
        server_default=None,
    )
