"""sentence_status_revoked
  
Revision ID: 055777a0433a
Revises: 9832e1ccfaac
Create Date: 2020-02-21 22:03:44.786354

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "055777a0433a"
down_revision = "9832e1ccfaac"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "COMMUTED",
    "COMPLETED",
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "SERVING",
    "SUSPENDED",
]

# With new value
new_values = [
    "COMMUTED",
    "COMPLETED",
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "REVOKED",
    "SERVING",
    "SUSPENDED",
]


def upgrade():
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

    op.alter_column(
        "state_sentence_group",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_sentence_group_history",
        column_name="status",
        type_=sa.Enum(*new_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    op.execute("DROP TYPE state_sentence_status_old;")


def downgrade():
    op.execute("ALTER TYPE state_sentence_status RENAME TO state_sentence_status_old;")
    sa.Enum(*old_values, name="state_sentence_status").create(bind=op.get_bind())

    op.alter_column(
        "state_sentence_group",
        column_name="status",
        type_=sa.Enum(*old_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_sentence_group_history",
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
