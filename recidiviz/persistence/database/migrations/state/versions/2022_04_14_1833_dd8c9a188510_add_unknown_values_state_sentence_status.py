# pylint: skip-file
"""add_unknown_values_state_sentence_status

Revision ID: dd8c9a188510
Revises: 0777b20b9af8
Create Date: 2022-04-14 18:33:20.877651

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "dd8c9a188510"
down_revision = "0777b20b9af8"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "COMMUTED",
    "COMPLETED",
    "EXTERNAL_UNKNOWN",
    "PARDONED",
    "PRESENT_WITHOUT_INFO",
    "SERVING",
    "SUSPENDED",
    "REVOKED",
    "VACATED",
    "PENDING",
    "SANCTIONED",
]

# With new value
new_values = [
    "COMMUTED",
    "COMPLETED",
    "EXTERNAL_UNKNOWN",
    "PARDONED",
    "PRESENT_WITHOUT_INFO",
    "SERVING",
    "SUSPENDED",
    "REVOKED",
    "VACATED",
    "PENDING",
    "SANCTIONED",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_sentence_status RENAME TO state_sentence_status_old;")
    sa.Enum(*new_values, name="state_sentence_status").create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_sentence",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_supervision_sentence_history",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_incarceration_sentence",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.execute("DROP TYPE state_sentence_status_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_sentence_status RENAME TO state_sentence_status_old;")
    sa.Enum(*old_values, name="state_sentence_status").create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_sentence",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_supervision_sentence_history",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_incarceration_sentence",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.execute("DROP TYPE state_sentence_status_old;")
