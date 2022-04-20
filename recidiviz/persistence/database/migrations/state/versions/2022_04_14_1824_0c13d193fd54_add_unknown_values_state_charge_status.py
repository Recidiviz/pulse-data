# pylint: skip-file
"""add_unknown_values_state_charge_status

Revision ID: 0c13d193fd54
Revises: fb182bd2257e
Create Date: 2022-04-14 18:24:40.584619

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0c13d193fd54"
down_revision = "fb182bd2257e"
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
    "ADJUDICATED",
    "TRANSFERRED_AWAY",
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
    op.alter_column(
        "state_charge_history",
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
    op.alter_column(
        "state_charge_history",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_charge_status"),
        postgresql_using="status::text::state_charge_status",
    )
    op.execute("DROP TYPE state_charge_status_old;")
