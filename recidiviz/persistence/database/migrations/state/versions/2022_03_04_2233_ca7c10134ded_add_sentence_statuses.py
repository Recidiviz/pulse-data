# pylint: skip-file
"""add_sentence_statuses

Revision ID: ca7c10134ded
Revises: 5437c8cb45dc
Create Date: 2022-03-04 22:33:03.688573

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "ca7c10134ded"
down_revision = "5437c8cb45dc"
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
        "state_incarceration_sentence_history",
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
    op.alter_column(
        "state_supervision_sentence_history",
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
        "state_incarceration_sentence_history",
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
    op.alter_column(
        "state_supervision_sentence_history",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.execute("DROP TYPE state_sentence_status_old;")
