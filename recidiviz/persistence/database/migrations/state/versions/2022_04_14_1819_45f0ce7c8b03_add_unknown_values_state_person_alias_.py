# pylint: skip-file
"""add_unknown_values_state_person_alias_type

Revision ID: 45f0ce7c8b03
Revises: 6f676c302974
Create Date: 2022-04-14 18:19:00.297304

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "45f0ce7c8b03"
down_revision = "6f676c302974"
branch_labels = None
depends_on = None

# Without new value
old_values = ["AFFILIATION_NAME", "ALIAS", "GIVEN_NAME", "MAIDEN_NAME", "NICKNAME"]

# With new value
new_values = [
    "AFFILIATION_NAME",
    "ALIAS",
    "GIVEN_NAME",
    "MAIDEN_NAME",
    "NICKNAME",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_person_alias_type RENAME TO state_person_alias_type_old;"
    )
    sa.Enum(*new_values, name="state_person_alias_type").create(bind=op.get_bind())
    op.alter_column(
        "state_person_alias",
        column_name="alias_type",
        type_=sa.Enum(*new_values, name="state_person_alias_type"),
        postgresql_using="alias_type::text::state_person_alias_type",
    )
    op.alter_column(
        "state_person_alias_history",
        column_name="alias_type",
        type_=sa.Enum(*new_values, name="state_person_alias_type"),
        postgresql_using="alias_type::text::state_person_alias_type",
    )
    op.execute("DROP TYPE state_person_alias_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_person_alias_type RENAME TO state_person_alias_type_old;"
    )
    sa.Enum(*old_values, name="state_person_alias_type").create(bind=op.get_bind())
    op.alter_column(
        "state_person_alias",
        column_name="alias_type",
        type_=sa.Enum(*old_values, name="state_person_alias_type"),
        postgresql_using="alias_type::text::state_person_alias_type",
    )
    op.alter_column(
        "state_person_alias_history",
        column_name="alias_type",
        type_=sa.Enum(*old_values, name="state_person_alias_type"),
        postgresql_using="alias_type::text::state_person_alias_type",
    )
    op.execute("DROP TYPE state_person_alias_type_old;")
