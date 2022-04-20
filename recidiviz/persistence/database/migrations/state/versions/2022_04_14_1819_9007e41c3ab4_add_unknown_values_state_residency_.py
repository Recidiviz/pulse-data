# pylint: skip-file
"""add_unknown_values_state_residency_status

Revision ID: 9007e41c3ab4
Revises: 45f0ce7c8b03
Create Date: 2022-04-14 18:19:06.730356

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9007e41c3ab4"
down_revision = "45f0ce7c8b03"
branch_labels = None
depends_on = None

# Without new value
old_values = ["HOMELESS", "PERMANENT", "TRANSIENT"]

# With new value
new_values = [
    "HOMELESS",
    "PERMANENT",
    "TRANSIENT",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


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
