# pylint: skip-file
"""add subscribed column

Revision ID: d77dcb2be80d
Revises: c021bab6ae8f
Create Date: 2023-08-04 10:00:54.782692

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d77dcb2be80d"
down_revision = "c021bab6ae8f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "agency_user_account_association",
        sa.Column("subscribed", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("agency_user_account_association", "subscribed")
