# pylint: skip-file
"""add_amended_status

Revision ID: 366599c208e9
Revises: 4cfe5e4cf0d5
Create Date: 2022-10-04 10:36:47.059607

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "366599c208e9"
down_revision = "4cfe5e4cf0d5"
branch_labels = None
depends_on = None


# Without the added values
old_values = [
    "COMMUTED",
    "COMPLETED",
    "PARDONED",
    "PENDING",
    "REVOKED",
    "SANCTIONED",
    "SERVING",
    "SUSPENDED",
    "VACATED",
    "PRESENT_WITHOUT_INFO",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]

# With added values
new_values = [
    "AMENDED",
    "COMMUTED",
    "COMPLETED",
    "PARDONED",
    "PENDING",
    "REVOKED",
    "SANCTIONED",
    "SERVING",
    "SUSPENDED",
    "VACATED",
    "PRESENT_WITHOUT_INFO",
    "INTERNAL_UNKNOWN",
    "EXTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_sentence_status RENAME TO state_sentence_status_old;")
    sa.Enum(*new_values, name="state_sentence_status").create(bind=op.get_bind())

    op.alter_column(
        "state_incarceration_sentence",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_supervision_sentence",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    op.execute("DROP TYPE state_sentence_status_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_sentence_status RENAME TO state_sentence_status_old;")
    sa.Enum(*old_values, name="state_sentence_status").create(bind=op.get_bind())

    op.alter_column(
        "state_incarceration_sentence",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_supervision_sentence",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    op.execute("DROP TYPE state_sentence_status_old;")
