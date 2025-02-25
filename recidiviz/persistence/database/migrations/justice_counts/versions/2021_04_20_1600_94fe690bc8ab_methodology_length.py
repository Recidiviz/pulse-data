# pylint: skip-file
"""methodology_length

Revision ID: 94fe690bc8ab
Revises: d4940e906def
Create Date: 2021-04-20 16:00:27.401375

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "94fe690bc8ab"
down_revision = "d4940e906def"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "report_table_instance",
        "methodology",
        existing_type=sa.String(length=255),
        type_=sa.String(),
    )


def downgrade() -> None:
    # Leaving this blank because some values will be longer than 255 so the migration
    # will just fail.
    pass
