# pylint: skip-file
"""add_unknown_values_state_gender

Revision ID: ad7b1b5b087d
Revises: 689749353c15
Create Date: 2022-04-14 18:24:01.954249

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ad7b1b5b087d"
down_revision = "689749353c15"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "FEMALE",
    "MALE",
    "OTHER",
    "TRANS",
    "TRANS_FEMALE",
    "TRANS_MALE",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "FEMALE",
    "MALE",
    "OTHER",
    "TRANS",
    "TRANS_FEMALE",
    "TRANS_MALE",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_gender RENAME TO state_gender_old;")
    sa.Enum(*new_values, name="state_gender").create(bind=op.get_bind())
    op.alter_column(
        "state_person",
        column_name="gender",
        type_=sa.Enum(*new_values, name="state_gender"),
        postgresql_using="gender::text::state_gender",
    )
    op.alter_column(
        "state_person_history",
        column_name="gender",
        type_=sa.Enum(*new_values, name="state_gender"),
        postgresql_using="gender::text::state_gender",
    )
    op.execute("DROP TYPE state_gender_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_gender RENAME TO state_gender_old;")
    sa.Enum(*old_values, name="state_gender").create(bind=op.get_bind())
    op.alter_column(
        "state_person",
        column_name="gender",
        type_=sa.Enum(*old_values, name="state_gender"),
        postgresql_using="gender::text::state_gender",
    )
    op.alter_column(
        "state_person_history",
        column_name="gender",
        type_=sa.Enum(*old_values, name="state_gender"),
        postgresql_using="gender::text::state_gender",
    )
    op.execute("DROP TYPE state_gender_old;")
