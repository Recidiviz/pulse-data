"""add_compassionate_release_reason

Revision ID: dc9834453209
Revises: 964cfd6c3d6c
Create Date: 2019-09-03 20:28:07.029716

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "dc9834453209"
down_revision = "964cfd6c3d6c"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "COMMUTED",
    "CONDITIONAL_RELEASE",
    "COURT_ORDER",
    "DEATH",
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
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
    "ESCAPE",
    "EXTERNAL_UNKNOWN",
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
