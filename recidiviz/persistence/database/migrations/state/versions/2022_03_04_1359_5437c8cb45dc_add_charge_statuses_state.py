# pylint: skip-file
"""add_charge_statuses_state

Revision ID: 5437c8cb45dc
Revises: 0da6ee9769e1
Create Date: 2022-03-04 13:59:06.689190

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5437c8cb45dc"
down_revision = "0da6ee9769e1"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "ACQUITTED",
    "COMPLETED_SENTENCE",
    "CONVICTED",
    "DROPPED",
    "INFERRED_DROPPED",
    "EXTERNAL_UNKNOWN",
    "PENDING",
    "PRETRIAL",
    "SENTENCED",
    "PRESENT_WITHOUT_INFO",
    "REMOVED_WITHOUT_INFO",
]

# With new value
new_values = [
    "ACQUITTED",
    "COMPLETED_SENTENCE",
    "CONVICTED",
    "DROPPED",
    "INFERRED_DROPPED",
    "EXTERNAL_UNKNOWN",
    "PENDING",
    "PRETRIAL",
    "SENTENCED",
    "PRESENT_WITHOUT_INFO",
    "REMOVED_WITHOUT_INFO",
    "ADJUDICATED",
    "TRANSFERRED_AWAY",
]


def upgrade() -> None:
    op.execute("ALTER TYPE charge_status RENAME TO charge_status_old;")
    sa.Enum(*new_values, name="charge_status").create(bind=op.get_bind())
    op.alter_column(
        "state_charge",
        column_name="status",
        type_=sa.Enum(*new_values, name="charge_status"),
        postgresql_using="status::text::charge_status",
    )
    op.alter_column(
        "state_charge_history",
        column_name="status",
        type_=sa.Enum(*new_values, name="charge_status"),
        postgresql_using="status::text::charge_status",
    )
    op.execute("DROP TYPE charge_status_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE charge_status RENAME TO charge_status_old;")
    sa.Enum(*old_values, name="charge_status").create(bind=op.get_bind())
    op.alter_column(
        "state_charge",
        column_name="status",
        type_=sa.Enum(*old_values, name="charge_status"),
        postgresql_using="status::text::charge_status",
    )
    op.alter_column(
        "state_charge_history",
        column_name="status",
        type_=sa.Enum(*old_values, name="charge_status"),
        postgresql_using="status::text::charge_status",
    )
    op.execute("DROP TYPE charge_status_old;")
