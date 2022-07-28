# pylint: skip-file
"""delete_infraction

Revision ID: 8830f60d82a6
Revises: 3c725da8f8cf
Create Date: 2022-07-28 09:29:19.240821

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8830f60d82a6"
down_revision = "3c725da8f8cf"
branch_labels = None
depends_on = None

# Without INFRACTION
new_values = ["CIVIL", "FELONY", "MISDEMEANOR", "INTERNAL_UNKNOWN", "EXTERNAL_UNKNOWN"]

# With INFRACTION
old_values = [
    "CIVIL",
    "FELONY",
    "INFRACTION",
    "MISDEMEANOR",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
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
