# pylint: skip-file
"""drop-unused-charge-statuses

Revision ID: 0d5975d9b897
Revises: 6d2226525eb4
Create Date: 2022-05-06 17:23:26.887675

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0d5975d9b897"
down_revision = "f0b4f8724f7d"
branch_labels = None
depends_on = None

# With deprecated values
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
    "ADJUDICATED",
    "TRANSFERRED_AWAY",
    "INTERNAL_UNKNOWN",
]

# Without deprecated values
new_values = [
    "ACQUITTED",
    "CONVICTED",
    "DROPPED",
    "EXTERNAL_UNKNOWN",
    "PENDING",
    "SENTENCED",
    "PRESENT_WITHOUT_INFO",
    "ADJUDICATED",
    "TRANSFERRED_AWAY",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_charge_status RENAME TO state_charge_status_old;")
    sa.Enum(*new_values, name="state_charge_status").create(bind=op.get_bind())
    op.alter_column(
        "state_charge",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_charge_status"),
        postgresql_using="status::text::state_charge_status",
    )
    op.execute("DROP TYPE state_charge_status_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_charge_status RENAME TO state_charge_status_old;")
    sa.Enum(*old_values, name="state_charge_status").create(bind=op.get_bind())
    op.alter_column(
        "state_charge",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_charge_status"),
        postgresql_using="status::text::state_charge_status",
    )
    op.execute("DROP TYPE state_charge_status_old;")
