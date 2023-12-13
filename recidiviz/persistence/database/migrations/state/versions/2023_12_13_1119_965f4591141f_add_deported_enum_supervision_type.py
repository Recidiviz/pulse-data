# pylint: skip-file
"""add_deported_enum_supervision_type

Revision ID: 965f4591141f
Revises: 159c8a262bc6
Create Date: 2023-12-13 11:19:57.231345

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "965f4591141f"
down_revision = "15f9c8e64103"
branch_labels = None
depends_on = None


# Supervision period supervision types, without DEPORTED
old_supervision_types = [
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

# Supervision period supervision types, with DEPORTED
new_supervision_types = [
    "ABSCONSION",
    "INFORMAL_PROBATION",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "DUAL",
    "DEPORTED",
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
