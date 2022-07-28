# pylint: skip-file
"""delete_sentenced_status

Revision ID: ddaf417ade56
Revises: d6fed693b593
Create Date: 2022-07-27 21:00:01.918301

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ddaf417ade56"
down_revision = "d6fed693b593"
branch_labels = None
depends_on = None


# Without SENTENCED
new_values = [
    "ACQUITTED",
    "ADJUDICATED",
    "CONVICTED",
    "DROPPED",
    "PENDING",
    "TRANSFERRED_AWAY",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
]

# WIth SENTENCED
old_values = [
    "ACQUITTED",
    "ADJUDICATED",
    "CONVICTED",
    "DROPPED",
    "PENDING",
    "SENTENCED",
    "TRANSFERRED_AWAY",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
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
