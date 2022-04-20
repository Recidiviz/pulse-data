# pylint: skip-file
"""add_unknown_values_state_ethnicity

Revision ID: 523623e0bdb3
Revises: 15188f35f7d1
Create Date: 2022-04-14 18:24:33.232142

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "523623e0bdb3"
down_revision = "15188f35f7d1"
branch_labels = None
depends_on = None

# Without new value
old_values = ["EXTERNAL_UNKNOWN", "HISPANIC", "NOT_HISPANIC"]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "HISPANIC",
    "NOT_HISPANIC",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_ethnicity RENAME TO state_ethnicity_old;")
    sa.Enum(*new_values, name="state_ethnicity").create(bind=op.get_bind())
    op.alter_column(
        "state_person_ethnicity",
        column_name="ethnicity",
        type_=sa.Enum(*new_values, name="state_ethnicity"),
        postgresql_using="ethnicity::text::state_ethnicity",
    )
    op.alter_column(
        "state_person_ethnicity_history",
        column_name="ethnicity",
        type_=sa.Enum(*new_values, name="state_ethnicity"),
        postgresql_using="ethnicity::text::state_ethnicity",
    )
    op.execute("DROP TYPE state_ethnicity_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_ethnicity RENAME TO state_ethnicity_old;")
    sa.Enum(*old_values, name="state_ethnicity").create(bind=op.get_bind())
    op.alter_column(
        "state_person_ethnicity",
        column_name="ethnicity",
        type_=sa.Enum(*old_values, name="state_ethnicity"),
        postgresql_using="ethnicity::text::state_ethnicity",
    )
    op.alter_column(
        "state_person_ethnicity_history",
        column_name="ethnicity",
        type_=sa.Enum(*old_values, name="state_ethnicity"),
        postgresql_using="ethnicity::text::state_ethnicity",
    )
    op.execute("DROP TYPE state_ethnicity_old;")
