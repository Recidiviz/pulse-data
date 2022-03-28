# pylint: skip-file
"""add_bench_warrant_supervision_type
Revision ID: b024ac1a811d
Revises: ca7c10134ded
Create Date: 2022-03-23 14:21:51.889020
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "b024ac1a811d"
down_revision = "e5f5c74569b6"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "INFORMAL_PROBATION",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "DUAL",
    "COMMUNITY_CONFINEMENT",
]

# With new value
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
