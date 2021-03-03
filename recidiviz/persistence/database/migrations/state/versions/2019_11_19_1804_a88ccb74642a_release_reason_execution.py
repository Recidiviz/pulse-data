"""release_reason_execution

Revision ID: a88ccb74642a
Revises: a1b99c7cc151
Create Date: 2019-11-19 18:04:01.515271

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a88ccb74642a"
down_revision = "a1b99c7cc151"
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
    "EXTERNAL_UNKNOWN",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "SENTENCE_SERVED",
    "TRANSFER",
]

# With new value
new_values = [
    "COMMUTED",
    "COMPASSIONATE",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "EXECUTED",
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
    "RELEASED_FROM_TEMPORARY_CUSTODY",
    "RELEASED_IN_ERROR",
    "SENTENCE_SERVED",
    "TRANSFER",
]


def upgrade():
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
        "state_incarceration_period",
        column_name="projected_release_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_release_reason"),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )

    op.alter_column(
        "state_incarceration_period_history",
        column_name="release_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_release_reason"),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="projected_release_reason",
        type_=sa.Enum(*new_values, name="state_incarceration_period_release_reason"),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )

    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")


def downgrade():
    op.execute(
        "ALTER TYPE state_incarceration_period_release_reason RENAME TO state_incarceration_period_release_reason_old;"
    )
    sa.Enum(*old_values, name="state_incarceration_period_release_reason").create(
        bind=op.get_bind()
    )

    op.alter_column(
        "state_incarceration_period",
        column_name="projected_release_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_release_reason"),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period",
        column_name="release_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_release_reason"),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )

    op.alter_column(
        "state_incarceration_period_history",
        column_name="projected_release_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_release_reason"),
        postgresql_using="projected_release_reason::text::state_incarceration_period_release_reason",
    )
    op.alter_column(
        "state_incarceration_period_history",
        column_name="release_reason",
        type_=sa.Enum(*old_values, name="state_incarceration_period_release_reason"),
        postgresql_using="release_reason::text::state_incarceration_period_release_reason",
    )

    op.execute("DROP TYPE state_incarceration_period_release_reason_old;")
