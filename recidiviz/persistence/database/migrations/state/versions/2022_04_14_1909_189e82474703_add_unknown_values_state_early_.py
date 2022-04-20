# pylint: skip-file
"""add_unknown_values_state_early_discharge_decision_status

Revision ID: 189e82474703
Revises: dd8c9a188510
Create Date: 2022-04-14 19:09:27.222149

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "189e82474703"
down_revision = "dd8c9a188510"
branch_labels = None
depends_on = None

# Without new value
old_values = ["PENDING", "DECIDED", "INVALID"]

# With new value
new_values = ["PENDING", "DECIDED", "INVALID", "EXTERNAL_UNKNOWN", "INTERNAL_UNKNOWN"]


def upgrade() -> None:
    op.execute(
        "ALTER TYPE state_early_discharge_decision_status RENAME TO state_early_discharge_decision_status_old;"
    )
    sa.Enum(*new_values, name="state_early_discharge_decision_status").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_early_discharge",
        column_name="decision_status",
        type_=sa.Enum(*new_values, name="state_early_discharge_decision_status"),
        postgresql_using="decision_status::text::state_early_discharge_decision_status",
    )
    op.alter_column(
        "state_early_discharge_history",
        column_name="decision_status",
        type_=sa.Enum(*new_values, name="state_early_discharge_decision_status"),
        postgresql_using="decision_status::text::state_early_discharge_decision_status",
    )
    op.execute("DROP TYPE state_early_discharge_decision_status_old;")


def downgrade() -> None:
    op.execute(
        "ALTER TYPE state_early_discharge_decision_status RENAME TO state_early_discharge_decision_status_old;"
    )
    sa.Enum(*old_values, name="state_early_discharge_decision_status").create(
        bind=op.get_bind()
    )
    op.alter_column(
        "state_early_discharge",
        column_name="decision_status",
        type_=sa.Enum(*old_values, name="state_early_discharge_decision_status"),
        postgresql_using="decision_status::text::state_early_discharge_decision_status",
    )
    op.alter_column(
        "state_early_discharge_history",
        column_name="decision_status",
        type_=sa.Enum(*old_values, name="state_early_discharge_decision_status"),
        postgresql_using="decision_status::text::state_early_discharge_decision_status",
    )
    op.execute("DROP TYPE state_early_discharge_decision_status_old;")
