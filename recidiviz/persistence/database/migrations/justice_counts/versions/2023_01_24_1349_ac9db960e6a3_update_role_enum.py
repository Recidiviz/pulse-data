# pylint: skip-file
"""update role enum

Revision ID: ac9db960e6a3
Revises: 35a5f3e339f2
Create Date: 2023-01-24 13:49:09.122909

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ac9db960e6a3"
down_revision = "35a5f3e339f2"
branch_labels = None
depends_on = None


# Without new value
old_values = ["ADMIN"]

# With new value
new_values = ["AGENCY_ADMIN", "JUSTICE_COUNTS_ADMIN", "CONTRIBUTOR"]


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
