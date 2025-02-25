# pylint: skip-file
"""rename user_email to email_address in PermissionsOverride

Revision ID: d00c4f6c83d0
Revises: b068757e3897
Create Date: 2023-03-22 10:05:30.778878

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d00c4f6c83d0"
down_revision = "b068757e3897"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "permissions_override", "user_email", new_column_name="email_address"
    )


def downgrade() -> None:
    op.alter_column(
        "permissions_override", "email_address", new_column_name="user_email"
    )
