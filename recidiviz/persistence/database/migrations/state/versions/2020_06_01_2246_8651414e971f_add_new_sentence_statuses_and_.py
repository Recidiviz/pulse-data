# pylint: skip-file
"""add_new_sentence_statuses_and_incarceration_types

Revision ID: 8651414e971f
Revises: 8b4feda0caa1
Create Date: 2020-06-01 22:46:46.987709

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "8651414e971f"
down_revision = "8b4feda0caa1"
branch_labels = None
depends_on = None

# Without new value
old_status_values = [
    "COMMUTED",
    "COMPLETED",
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "REVOKED",
    "SERVING",
    "SUSPENDED",
]
old_incarceration_values = ["COUNTY_JAIL", "EXTERNAL_UNKNOWN", "STATE_PRISON"]

# With new value
new_status_values = [
    "COMMUTED",
    "COMPLETED",
    "EXTERNAL_UNKNOWN",
    "PARDONED",
    "PRESENT_WITHOUT_INFO",
    "REVOKED",
    "SERVING",
    "SUSPENDED",
    "VACATED",
]
new_incarceration_values = [
    "COUNTY_JAIL",
    "EXTERNAL_UNKNOWN",
    "FEDERAL_PRISON",
    "OUT_OF_STATE",
    "STATE_PRISON",
]


def upgrade():
    # Sentence status
    op.execute("ALTER TYPE state_sentence_status RENAME TO state_sentence_status_old;")
    sa.Enum(*new_status_values, name="state_sentence_status").create(bind=op.get_bind())

    # Sentence status in state_sentence_group
    op.alter_column(
        "state_sentence_group",
        column_name="status",
        type_=sa.Enum(*new_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_sentence_group_history",
        column_name="status",
        type_=sa.Enum(*new_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    # Sentence status in state_incarceration_sentence
    op.alter_column(
        "state_incarceration_sentence",
        column_name="status",
        type_=sa.Enum(*new_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="status",
        type_=sa.Enum(*new_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    # Sentence status in state_supervision_sentence
    op.alter_column(
        "state_supervision_sentence",
        column_name="status",
        type_=sa.Enum(*new_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_supervision_sentence_history",
        column_name="status",
        type_=sa.Enum(*new_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    op.execute("DROP TYPE state_sentence_status_old;")

    # Incarceration type
    op.execute(
        "ALTER TYPE state_incarceration_type RENAME TO state_incarceration_type_old;"
    )
    sa.Enum(*new_incarceration_values, name="state_incarceration_type").create(
        bind=op.get_bind()
    )

    # Incarceration type in state_incarceration_sentence
    op.alter_column(
        "state_incarceration_sentence",
        column_name="incarceration_type",
        type_=sa.Enum(*new_incarceration_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="incarceration_type",
        type_=sa.Enum(*new_incarceration_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )

    # Incarceration type in state_incarceration_period
    op.alter_column(
        "state_incarceration_period",
        column_name="incarceration_type",
        type_=sa.Enum(*new_incarceration_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="incarceration_type",
        type_=sa.Enum(*new_incarceration_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )

    op.execute("DROP TYPE state_incarceration_type_old;")


def downgrade():
    # Sentence status
    op.execute("ALTER TYPE state_sentence_status RENAME TO state_sentence_status_old;")
    sa.Enum(*old_status_values, name="state_sentence_status").create(bind=op.get_bind())

    # Sentence status in state_sentence_group
    op.alter_column(
        "state_sentence_group",
        column_name="status",
        type_=sa.Enum(*old_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_sentence_group_history",
        column_name="status",
        type_=sa.Enum(*old_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    # Sentence status in state_incarceration_sentence
    op.alter_column(
        "state_incarceration_sentence",
        column_name="status",
        type_=sa.Enum(*old_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="status",
        type_=sa.Enum(*old_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    # Sentence status in state_supervision_sentence
    op.alter_column(
        "state_supervision_sentence",
        column_name="status",
        type_=sa.Enum(*old_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )
    op.alter_column(
        "state_supervision_sentence_history",
        column_name="status",
        type_=sa.Enum(*old_status_values, name="state_sentence_status"),
        postgresql_using="status::text::state_sentence_status",
    )

    op.execute("DROP TYPE state_sentence_status_old;")

    # Incarceration type
    op.execute(
        "ALTER TYPE state_incarceration_type RENAME TO state_incarceration_type_old;"
    )
    sa.Enum(*old_incarceration_values, name="state_incarceration_type").create(
        bind=op.get_bind()
    )

    # Incarceration type in state_incarceration_sentence
    op.alter_column(
        "state_incarceration_sentence",
        column_name="incarceration_type",
        type_=sa.Enum(*old_incarceration_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_sentence_history",
        column_name="incarceration_type",
        type_=sa.Enum(*old_incarceration_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )

    # Incarceration type in state_incarceration_period
    op.alter_column(
        "state_incarceration_period",
        column_name="incarceration_type",
        type_=sa.Enum(*old_incarceration_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="incarceration_type",
        type_=sa.Enum(*old_incarceration_values, name="state_incarceration_type"),
        postgresql_using="incarceration_type::text::state_incarceration_type",
    )

    op.execute("DROP TYPE state_incarceration_type_old;")
