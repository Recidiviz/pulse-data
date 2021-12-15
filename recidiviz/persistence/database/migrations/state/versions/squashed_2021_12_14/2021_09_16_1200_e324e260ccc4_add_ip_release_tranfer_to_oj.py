# pylint: skip-file
"""add_ip_release_tranfer_to_oj

Revision ID: e324e260ccc4
Revises: 5f167eaafa84
Create Date: 2021-09-16 12:00:38.857771

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "e324e260ccc4"
down_revision = "5f167eaafa84"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "ESCAPE",
    "EXECUTION",
    "PARDONED",
    "RELEASED_FROM_ERRONEOUS_ADMISSION",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "RELEASED_TO_SUPERVISION",
    "SENTENCE_SERVED",
    "STATUS_CHANGE",
    "TRANSFER",
    "VACATED",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TEMPORARY_RELEASE",
]

# With new value
new_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "ESCAPE",
    "EXECUTION",
    "PARDONED",
    "RELEASED_FROM_ERRONEOUS_ADMISSION",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "RELEASED_TO_SUPERVISION",
    "SENTENCE_SERVED",
    "STATUS_CHANGE",
    "TRANSFER",
    "VACATED",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
    "TEMPORARY_RELEASE",
    "TRANSFER_TO_OTHER_JURISDICTION",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(*new_values, name="state_incarceration_period_release_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_release_reason"),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="release_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_release_reason"),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    # Update projected release reason.
    op.alter_column(
        "state_incarceration_period",
        column_name="projected_release_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_release_reason"),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="projected_release_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_release_reason"),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(*old_values, name="state_incarceration_period_release_reason").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_release_reason"),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="release_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_release_reason"),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    # Update projected release reason.
    op.alter_column(
        "state_incarceration_period",
        column_name="projected_release_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_release_reason"),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="projected_release_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_release_reason"),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")
