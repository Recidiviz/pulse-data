# pylint: skip-file
"""add_unassigned_supervision_level

Revision ID: bb9fd77aa9cb
Revises: 8a77e86728e9
Create Date: 2021-06-30 11:32:16.765723

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bb9fd77aa9cb"
down_revision = "8a77e86728e9"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "DIVERSION",
    "INCARCERATED",
    "INTERSTATE_COMPACT",
    "IN_CUSTODY",
    "ELECTRONIC_MONITORING_ONLY",
    "LIMITED",
    "MINIMUM",
    "MEDIUM",
    "HIGH",
    "MAXIMUM",
    "UNSUPERVISED",
]

# With new value
new_values = [
    "EXTERNAL_UNKNOWN",
    "INTERNAL_UNKNOWN",
    "PRESENT_WITHOUT_INFO",
    "DIVERSION",
    "INCARCERATED",
    "INTERSTATE_COMPACT",
    "IN_CUSTODY",
    "ELECTRONIC_MONITORING_ONLY",
    "LIMITED",
    "MINIMUM",
    "MEDIUM",
    "HIGH",
    "MAXIMUM",
    "UNSUPERVISED",
    "UNASSIGNED",
]


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
