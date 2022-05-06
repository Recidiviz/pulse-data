# pylint: skip-file
"""drop-unused-residency-status

Revision ID: 711c8575e63c
Revises: 09a00a11ebcb
Create Date: 2022-05-06 16:47:42.690322

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "711c8575e63c"
down_revision = "09a00a11ebcb"
branch_labels = None
depends_on = None

# With TRANSIENT
old_values = [
    "HOMELESS",
    "PERMANENT",
    "TRANSIENT",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Without TRANSIENT
new_values = ["HOMELESS", "PERMANENT", "EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN"]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_residency_status RENAME TO state_residency_status_old;"
    )
    sa.Enum(*new_values, name="state_residency_status").create(bind=op.get_bind())
    op.alter_column(
        "state_person",
        column_name="residency_status",
        type_=sa.Enum(*new_values, name="state_residency_status"),
        postgresql_using="residency_status::text::state_residency_status",
    )
    op.alter_column(
        "state_person_history",
        column_name="residency_status",
        type_=sa.Enum(*new_values, name="state_residency_status"),
        postgresql_using="residency_status::text::state_residency_status",
    )
    op.execute("DROP TYPE state_residency_status_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_residency_status RENAME TO state_residency_status_old;"
    )
    sa.Enum(*old_values, name="state_residency_status").create(bind=op.get_bind())
    op.alter_column(
        "state_person",
        column_name="residency_status",
        type_=sa.Enum(*old_values, name="state_residency_status"),
        postgresql_using="residency_status::text::state_residency_status",
    )
    op.alter_column(
        "state_person_history",
        column_name="residency_status",
        type_=sa.Enum(*old_values, name="state_residency_status"),
        postgresql_using="residency_status::text::state_residency_status",
    )
    op.execute("DROP TYPE state_residency_status_old;")
