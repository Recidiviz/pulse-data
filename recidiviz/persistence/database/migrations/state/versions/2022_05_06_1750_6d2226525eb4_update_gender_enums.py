# pylint: skip-file
"""update-gender-enums

Revision ID: 6d2226525eb4
Revises: 711c8575e63c
Create Date: 2022-05-06 16:50:12.763081

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "6d2226525eb4"
down_revision = "a6a86d0718db"
branch_labels = None
depends_on = None

# Without NON_BINARY, with OTHER
old_values = [
    "EXTERNAL_UNKNOWN",
    "FEMALE",
    "MALE",
    "OTHER",
    "TRANS",
    "TRANS_FEMALE",
    "TRANS_MALE",
    "INTERNAL_UNKNOWN",
]

# With NON_BINARY, without OTHER
new_values = [
    "EXTERNAL_UNKNOWN",
    "FEMALE",
    "MALE",
    "TRANS",
    "TRANS_FEMALE",
    "TRANS_MALE",
    "INTERNAL_UNKNOWN",
    "NON_BINARY",
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
