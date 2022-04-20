# pylint: skip-file
"""remove_charge_status_removed_without_info

Revision ID: 68f7c5087963
Revises: 1f5a8b579bc0
Create Date: 2022-04-13 18:04:45.203925

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "68f7c5087963"
down_revision = "1f5a8b579bc0"
branch_labels = None
depends_on = None

# With REMOVED_WITHOUT_INFO
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
    "ADJUDICATED",
    "TRANSFERRED_AWAY",
]

# Without REMOVED_WITHOUT_INFO
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
