# pylint: skip-file
"""add_unknown_values_state_incarceration_type

Revision ID: 15fc233812ac
Revises: 9007e41c3ab4
Create Date: 2022-04-14 18:22:20.563682

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "15fc233812ac"
down_revision = "9007e41c3ab4"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "COUNTY_JAIL",
    "FEDERAL_PRISON",
    "OUT_OF_STATE",
    "STATE_PRISON",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "COUNTY_JAIL",
    "FEDERAL_PRISON",
    "OUT_OF_STATE",
    "STATE_PRISON",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_type RENAME TO state_incarceration_type_old;"
    )
    sa.Enum(*new_values, name="state_incarceration_type").create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_sentence",
        column_name="incarceration_type",
        type_=sa.Enum(*new_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="incarceration_type",
        type_=sa.Enum(*new_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="incarceration_type",
        type_=sa.Enum(*new_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="incarceration_type",
        type_=sa.Enum(*new_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.execute("DROP TYPE state_incarceration_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_type RENAME TO state_incarceration_type_old;"
    )
    sa.Enum(*old_values, name="state_incarceration_type").create(bind=op.get_bind())
    op.alter_column(
        "state_incarceration_sentence",
        column_name="incarceration_type",
        type_=sa.Enum(*old_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="incarceration_type",
        type_=sa.Enum(*old_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="incarceration_type",
        type_=sa.Enum(*old_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="incarceration_type",
        type_=sa.Enum(*old_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.execute("DROP TYPE state_incarceration_type_old;")
