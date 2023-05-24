# pylint: skip-file
"""add readonly role

Revision ID: b85b2f2d012b
Revises: 4bfb2734352b
Create Date: 2023-05-24 11:12:42.937567

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b85b2f2d012b"
down_revision = "4bfb2734352b"
branch_labels = None
depends_on = None

# Without new value
old_values = ["AGENCY_ADMIN", "JUSTICE_COUNTS_ADMIN", "CONTRIBUTOR"]

# With new value
new_values = ["AGENCY_ADMIN", "JUSTICE_COUNTS_ADMIN", "CONTRIBUTOR", "READ_ONLY"]


def upgrade() -> None:
    op.execute("ALTER TYPE useraccountrole RENAME TO useraccountrole_old;")
    sa.Enum(*new_values, name="useraccountrole").create(bind=op.get_bind())
    op.alter_column(
        "agency_user_account_association",
        column_name="role",
        type_=sa.Enum(*new_values, name="useraccountrole"),
        postgresql_using="role::text::useraccountrole",
    )
    op.execute("DROP TYPE useraccountrole_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE useraccountrole RENAME TO useraccountrole_old;")
    sa.Enum(*old_values, name="useraccountrole").create(bind=op.get_bind())
    op.alter_column(
        "agency_user_account_association",
        column_name="role",
        type_=sa.Enum(*old_values, name="useraccountrole"),
        postgresql_using="role::text::useraccountrole",
    )
    op.execute("DROP TYPE useraccountrole_old;")
