# pylint: skip-file
"""add_unknown_values_state_race

Revision ID: c1f0f439e75c
Revises: 512d63c3fd4f
Create Date: 2022-04-14 18:18:53.630644

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c1f0f439e75c"
down_revision = "512d63c3fd4f"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "AMERICAN_INDIAN_ALASKAN_NATIVE",
    "ASIAN",
    "BLACK",
    "EXTERNAL_UNKNOWN",
    "NATIVE_HAWAIIAN_PACIFIC_ISLANDER",
    "OTHER",
    "WHITE",
]

# With new value
new_values = [
    "AMERICAN_INDIAN_ALASKAN_NATIVE",
    "ASIAN",
    "BLACK",
    "EXTERNAL_UNKNOWN",
    "NATIVE_HAWAIIAN_PACIFIC_ISLANDER",
    "OTHER",
    "WHITE",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_race RENAME TO state_race_old;")
    sa.Enum(*new_values, name="state_race").create(bind=op.get_bind())
    op.alter_column(
        "state_person_race",
        column_name="race",
        type_=sa.Enum(*new_values, name="state_race"),
        postgresql_using="race::text::state_race",
    )
    op.alter_column(
        "state_person_race_history",
        column_name="race",
        type_=sa.Enum(*new_values, name="state_race"),
        postgresql_using="race::text::state_race",
    )
    op.execute("DROP TYPE state_race_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_race RENAME TO state_race_old;")
    sa.Enum(*old_values, name="state_race").create(bind=op.get_bind())
    op.alter_column(
        "state_person_race",
        column_name="race",
        type_=sa.Enum(*old_values, name="state_race"),
        postgresql_using="race::text::state_race",
    )
    op.alter_column(
        "state_person_race_history",
        column_name="race",
        type_=sa.Enum(*old_values, name="state_race"),
        postgresql_using="race::text::state_race",
    )
    op.execute("DROP TYPE state_race_old;")
