# pylint: skip-file
"""add_new_release_reasons

Revision ID: c7cc7e007e2f
Revises: dfcb0a5afd65
Create Date: 2020-07-01 16:43:15.649972

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c7cc7e007e2f"
down_revision = "dfcb0a5afd65"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "EXECUTION",
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "SENTENCE_SERVED",
    "TRANSFER",
    "TRANSFERRED_OUT_OF_STATE",
]

# With new value
new_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "EXECUTION",
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "PARDONED",
    "RELEASED_FROM_ERRONEOUS_ADMISSION",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "SENTENCE_SERVED",
    "TRANSFER",
    "TRANSFERRED_OUT_OF_STATE",
    "VACATED",
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
