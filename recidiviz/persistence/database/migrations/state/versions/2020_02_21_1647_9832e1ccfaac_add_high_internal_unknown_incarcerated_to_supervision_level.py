# pylint: skip-file
"""add_high_internal_unknown_incarcerated_to_supervision_level

Revision ID: 9832e1ccfaac
Revises: 1378682f4712
Create Date: 2020-02-21 16:47:07.603696

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "9832e1ccfaac"
down_revision = "1378682f4712"
branch_labels = None
depends_on = None

# Without new value
old_values = {
    "EXTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "MINIMUM",
    "MEDIUM",
    "MAXIMUM",
    "DIVERSION",
    "INTERSTATE_COMPACT",
}

# With new value
new_values = {
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "MINIMUM",
    "MEDIUM",
    "HIGH",
    "MAXIMUM",
    "INCARCERATED",
    "DIVERSION",
    "INTERSTATE_COMPACT",
}


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_level RENAME TO state_supervision_level_old;"
    )
    sa.Enum(*new_values, name="state_supervision_level").create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_level",
        type_=sa.Enum(*new_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_level",
        type_=sa.Enum(*new_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.execute("DROP TYPE state_supervision_level_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_level RENAME TO state_supervision_level_old;"
    )
    sa.Enum(*old_values, name="state_supervision_level").create(bind=op.get_bind())
    op.alter_column(
        "state_supervision_period",
        column_name="supervision_level",
        type_=sa.Enum(*old_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.alter_column(
        "state_supervision_period_history",
        column_name="supervision_level",
        type_=sa.Enum(*old_values, name="state_supervision_level"),
        postgresql_using="supervision_level::text::state_supervision_level",
    )
    op.execute("DROP TYPE state_supervision_level_old;")
