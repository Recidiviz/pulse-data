# pylint: skip-file
"""add_warrant_status_enum

Revision ID: a2eafc022f5e
Revises: 7f28f8767137
Create Date: 2023-12-11 11:41:15.855812

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a2eafc022f5e"
down_revision = "7f28f8767137"
branch_labels = None
depends_on = None


# Supervision period supervision types, without WARRANT_STATUS
old_supervision_types = [
    "ABSCONSION",
    "INFORMAL_PROBATION",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "DUAL",
    "COMMUNITY_CONFINEMENT",
    "BENCH_WARRANT",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]

# Supervision period supervision types, with WARRANT_STATUS
new_supervision_types = [
    "ABSCONSION",
    "INFORMAL_PROBATION",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "DUAL",
    "COMMUNITY_CONFINEMENT",
    "BENCH_WARRANT",
    "WARRANT_STATUS",
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
]


def upgrade() -> None:
    # Supervision period supervision type
    op.execute(
        "ALTER TYPE state_supervision_period_supervision_type RENAME TO state_supervision_period_supervision_type_old;"
    )
    sa.Enum(
        *new_supervision_types,
        name="state_supervision_period_supervision_type",
    ).create(op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_type",
        type_=sa.Enum(
            *new_supervision_types,
            name="state_supervision_period_supervision_type",
        ),
        postgresql_using="supervision_type::text::state_supervision_period_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_period_supervision_type_old;")


def downgrade() -> None:
    # Supervision period supervision type
    op.execute(
        "ALTER TYPE state_supervision_period_supervision_type RENAME TO state_supervision_period_supervision_type_old;"
    )
    sa.Enum(
        *old_supervision_types,
        name="state_supervision_period_supervision_type",
    ).create(op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_type",
        type_=sa.Enum(
            *old_supervision_types,
            name="state_supervision_period_supervision_type",
        ),
        postgresql_using="supervision_type::text::state_supervision_period_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_period_supervision_type_old;")
