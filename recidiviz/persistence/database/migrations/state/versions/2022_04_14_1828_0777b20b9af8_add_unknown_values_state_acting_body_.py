# pylint: skip-file
"""add_unknown_values_state_acting_body_type

Revision ID: 0777b20b9af8
Revises: ef114fe1374c
Create Date: 2022-04-14 18:28:17.724106

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0777b20b9af8"
down_revision = "ef114fe1374c"
branch_labels = None
depends_on = None

# Without new value
old_values = ["COURT", "PAROLE_BOARD", "SENTENCED_PERSON", "SUPERVISION_OFFICER"]

# With new value
new_values = [
    "COURT",
    "PAROLE_BOARD",
    "SENTENCED_PERSON",
    "SUPERVISION_OFFICER",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_acting_body_type RENAME TO state_acting_body_type_old;"
    )
    sa.Enum(*new_values, name="state_acting_body_type").create(bind=op.get_bind())
    op.alter_column(
        "state_early_discharge",
        column_name="requesting_body_type",
        type_=sa.Enum(*new_values, name="state_acting_body_type"),
        postgresql_using="requesting_body_type::text::state_acting_body_type",
    )
    op.alter_column(
        "state_early_discharge_history",
        column_name="requesting_body_type",
        type_=sa.Enum(*new_values, name="state_acting_body_type"),
        postgresql_using="requesting_body_type::text::state_acting_body_type",
    )
    op.alter_column(
        "state_early_discharge",
        column_name="deciding_body_type",
        type_=sa.Enum(*new_values, name="state_acting_body_type"),
        postgresql_using="deciding_body_type::text::state_acting_body_type",
    )
    op.alter_column(
        "state_early_discharge_history",
        column_name="deciding_body_type",
        type_=sa.Enum(*new_values, name="state_acting_body_type"),
        postgresql_using="deciding_body_type::text::state_acting_body_type",
    )
    op.execute("DROP TYPE state_acting_body_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_acting_body_type RENAME TO state_acting_body_type_old;"
    )
    sa.Enum(*old_values, name="state_acting_body_type").create(bind=op.get_bind())
    op.alter_column(
        "state_early_discharge",
        column_name="requesting_body_type",
        type_=sa.Enum(*old_values, name="state_acting_body_type"),
        postgresql_using="requesting_body_type::text::state_acting_body_type",
    )
    op.alter_column(
        "state_early_discharge_history",
        column_name="requesting_body_type",
        type_=sa.Enum(*old_values, name="state_acting_body_type"),
        postgresql_using="requesting_body_type::text::state_acting_body_type",
    )
    op.alter_column(
        "state_early_discharge",
        column_name="deciding_body_type",
        type_=sa.Enum(*old_values, name="state_acting_body_type"),
        postgresql_using="deciding_body_type::text::state_acting_body_type",
    )
    op.alter_column(
        "state_early_discharge_history",
        column_name="deciding_body_type",
        type_=sa.Enum(*old_values, name="state_acting_body_type"),
        postgresql_using="deciding_body_type::text::state_acting_body_type",
    )
    op.execute("DROP TYPE state_acting_body_type_old;")
