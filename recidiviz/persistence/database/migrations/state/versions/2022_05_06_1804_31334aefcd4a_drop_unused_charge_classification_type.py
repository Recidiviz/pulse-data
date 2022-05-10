# pylint: skip-file
"""drop-unused-charge-classification-type

Revision ID: 31334aefcd4a
Revises: 0d5975d9b897
Create Date: 2022-05-06 17:24:15.245047

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "31334aefcd4a"
down_revision = "0d5975d9b897"
branch_labels = None
depends_on = None

# With OTHER
old_values = [
    "CIVIL",
    "EXTERNAL_UNKNOWN",
    "FELONY",
    "INFRACTION",
    "MISDEMEANOR",
    "OTHER",
    "INTERNAL_UNKNOWN",
]

# Without OTHER
new_values = [
    "CIVIL",
    "EXTERNAL_UNKNOWN",
    "FELONY",
    "INFRACTION",
    "MISDEMEANOR",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_charge_classification_type RENAME TO state_charge_classification_type_old;"
    )
    sa.Enum(*new_values, name="state_charge_classification_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_charge",
        column_name="classification_type",
        type_=sa.Enum(*new_values, name="state_charge_classification_type"),
        postgresql_using="classification_type::text::state_charge_classification_type",
    )
    op.execute("DROP TYPE state_charge_classification_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_charge_classification_type RENAME TO state_charge_classification_type_old;"
    )
    sa.Enum(*old_values, name="state_charge_classification_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_charge",
        column_name="classification_type",
        type_=sa.Enum(*old_values, name="state_charge_classification_type"),
        postgresql_using="classification_type::text::state_charge_classification_type",
    )
    op.execute("DROP TYPE state_charge_classification_type_old;")
