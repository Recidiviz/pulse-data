# pylint: skip-file
"""add_community_confinement_supervision_type

Revision ID: 581ca2c69255
Revises: c5e0b7ec2b9a
Create Date: 2021-09-28 16:15:14.846181

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "581ca2c69255"
down_revision = "bcd42781c590"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "DUAL",
    "EXTERNAL_UNKNOWN",
    "INFORMAL_PROBATION",
    "INTERNAL_UNKNOWN",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
]

# With new value
new_values = [
    "DUAL",
    "EXTERNAL_UNKNOWN",
    "INFORMAL_PROBATION",
    "INTERNAL_UNKNOWN",
    "INVESTIGATION",
    "PAROLE",
    "PROBATION",
    "COMMUNITY_CONFINEMENT",
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
        column_name="supervision_period_supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_period_supervision_type::text::state_supervision_period_supervision_type",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_period_supervision_type",
        type_=sa.Enum(*new_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_period_supervision_type::text::state_supervision_period_supervision_type",
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
        column_name="supervision_period_supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_period_supervision_type::text::state_supervision_period_supervision_type",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_period_supervision_type",
        type_=sa.Enum(*old_values, name="state_supervision_period_supervision_type"),
        postgresql_using="supervision_period_supervision_type::text::state_supervision_period_supervision_type",
    )
    op.execute("DROP TYPE state_supervision_period_supervision_type_old;")
