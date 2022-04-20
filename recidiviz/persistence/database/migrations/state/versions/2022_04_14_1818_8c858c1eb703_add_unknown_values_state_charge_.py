# pylint: skip-file
"""add_unknown_values_state_charge_classification_type

Revision ID: 8c858c1eb703
Revises: 6fa63084ef17
Create Date: 2022-04-14 18:18:44.327900

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8c858c1eb703"
down_revision = "6fa63084ef17"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CIVIL",
    "EXTERNAL_UNKNOWN",
    "FELONY",
    "INFRACTION",
    "MISDEMEANOR",
    "OTHER",
]

# With new value
new_values = [
    "CIVIL",
    "EXTERNAL_UNKNOWN",
    "FELONY",
    "INFRACTION",
    "MISDEMEANOR",
    "OTHER",
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
    op.alter_column(
        "state_charge_history",
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
    op.alter_column(
        "state_charge_history",
        column_name="classification_type",
        type_=sa.Enum(*old_values, name="state_charge_classification_type"),
        postgresql_using="classification_type::text::state_charge_classification_type",
    )
    op.execute("DROP TYPE state_charge_classification_type_old;")
