# pylint: skip-file
"""supervision_violation_type_escaped

Revision ID: 3e56b61df4a9
Revises: a88ccb74642a
Create Date: 2019-11-19 18:16:08.038651

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "3e56b61df4a9"
down_revision = "a88ccb74642a"
branch_labels = None
depends_on = None

# Without new value
old_values = ["ABSCONDED", "FELONY", "MISDEMEANOR", "MUNICIPAL", "TECHNICAL"]

# With new value
new_values = ["ABSCONDED", "ESCAPED", "FELONY", "MISDEMEANOR", "MUNICIPAL", "TECHNICAL"]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_type RENAME TO state_supervision_violation_type_old;"
    )
    sa.Enum(*new_values, name="state_supervision_violation_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_violation",
        column_name="violation_type",
        type_=sa.Enum(*new_values, name="state_supervision_violation_type"),
        postgresql_using="violation_type::text::state_supervision_violation_type",
    )
    op.alter_column(
        "state_supervision_violation_history",
        column_name="violation_type",
        type_=sa.Enum(*new_values, name="state_supervision_violation_type"),
        postgresql_using="violation_type::text::state_supervision_violation_type",
    )
    op.execute("DROP TYPE state_supervision_violation_type_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_supervision_violation_type RENAME TO state_supervision_violation_type_old;"
    )
    sa.Enum(*old_values, name="state_supervision_violation_type").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_supervision_violation",
        column_name="violation_type",
        type_=sa.Enum(*old_values, name="state_supervision_violation_type"),
        postgresql_using="violation_type::text::state_supervision_violation_type",
    )
    op.alter_column(
        "state_supervision_violation_history",
        column_name="violation_type",
        type_=sa.Enum(*old_values, name="state_supervision_violation_type"),
        postgresql_using="violation_type::text::state_supervision_violation_type",
    )
    op.execute("DROP TYPE state_supervision_violation_type_old;")
