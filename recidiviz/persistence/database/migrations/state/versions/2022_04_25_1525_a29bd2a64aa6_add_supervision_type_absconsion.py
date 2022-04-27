# pylint: skip-file
"""add_supervision_type_absconsion

Revision ID: a29bd2a64aa6
Revises: 1142b91484ed
Create Date: 2022-04-25 15:25:22.651383

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a29bd2a64aa6"
down_revision = "1142b91484ed"
branch_labels = None
depends_on = None

# Without ABSCONSION
old_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "INFORMAL_PROBATION",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "DUAL",
    "COMMUNITY_CONFINEMENT",
    "BENCH_WARRANT",
]

# With ABSCONSION
new_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "INFORMAL_PROBATION",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "DUAL",
    "COMMUNITY_CONFINEMENT",
    "BENCH_WARRANT",
    "ABSCONSION",
]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_supervision_type RENAME TO state_supervision_period_supervision_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_period_supervision_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_period_supervision_type",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_period_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_period_supervision_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_period_supervision_type RENAME TO state_supervision_period_supervision_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_period_supervision_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_period_supervision_type",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_type::text::state_supervision_period_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_period_supervision_type_old;")
