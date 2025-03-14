# pylint: skip-file
"""Drop blocked field

Revision ID: f095a0b3b95b
Revises: 753d0dab5a29
Create Date: 2025-03-06 13:55:19.023484

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "f095a0b3b95b"
down_revision = "753d0dab5a29"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("user_override", "blocked")


def downgrade() -> None:
    op.add_column(
        "user_override",
        sa.Column("blocked", sa.BOOLEAN, nullable=True),
    )
