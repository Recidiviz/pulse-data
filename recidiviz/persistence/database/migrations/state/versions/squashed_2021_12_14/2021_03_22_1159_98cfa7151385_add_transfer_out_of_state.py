# pylint: skip-file
"""add_transfer_out_of_state

Revision ID: 98cfa7151385
Revises: 2d283bfe5d7a
Create Date: 2021-03-22 11:59:14.427998

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "98cfa7151385"
down_revision = "2d283bfe5d7a"
branch_labels = None
depends_on = None

# Without`TRANSFER_OUT_OF_STATE`
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
    "SENTENCE_SERVED",
    "STATUS_CHANGE",
    "TRANSFER",
    "TRANSFERRED_OUT_OF_STATE",
    "VACATED",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# With `TRANSFER_OUT_OF_STATE`
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
    "SENTENCE_SERVED",
    "STATUS_CHANGE",
    "TRANSFER",
    "TRANSFERRED_OUT_OF_STATE",
    "VACATED",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "TRANSFER_OUT_OF_STATE",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(*new_values, name="state_incarceration_period_release_reason").create(
        bind=op.get_bind()
    )

    # Update release reason.
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

    # Update release reason.
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
