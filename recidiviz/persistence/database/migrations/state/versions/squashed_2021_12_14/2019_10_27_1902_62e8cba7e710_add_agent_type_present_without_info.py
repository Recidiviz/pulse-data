# pylint: skip-file
"""add_agent_type_present_without_info

Revision ID: 62e8cba7e710
Revises: 4616432309a9
Create Date: 2019-10-27 19:02:56.713286

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "62e8cba7e710"
down_revision = "4616432309a9"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "PAROLE_BOARD_MEMBER",
    "SUPERVISION_OFFICER",
    "UNIT_SUPERVISOR",
]

# With new value
new_values = [
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "PAROLE_BOARD_MEMBER",
    "SUPERVISION_OFFICER",
    "UNIT_SUPERVISOR",
    "PRESENT_WITHOUT_INFO",
]


def upgrade() -> None:
    op.execute("ALTER TYPE state_agent_type RENAME TO state_agent_type_old;")
    sa.Enum(*new_values, name="state_agent_type").create(bind=op.get_bind())
    op.alter_column(
        "state_agent",
        column_name="agent_type",
        type_=sa.Enum(*new_values, name="state_agent_type"),
        postgresql_using="agent_type::text::state_agent_type",
    )
    op.alter_column(
        "state_agent_history",
        column_name="agent_type",
        type_=sa.Enum(*new_values, name="state_agent_type"),
        postgresql_using="agent_type::text::state_agent_type",
    )
    op.execute("DROP TYPE state_agent_type_old;")


def downgrade() -> None:
    op.execute("ALTER TYPE state_agent_type RENAME TO state_agent_type_old;")
    sa.Enum(*old_values, name="state_agent_type").create(bind=op.get_bind())
    op.alter_column(
        "state_agent",
        column_name="agent_type",
        type_=sa.Enum(*old_values, name="state_agent_type"),
        postgresql_using="agent_type::text::state_agent_type",
    )
    op.alter_column(
        "state_agent_history",
        column_name="agent_type",
        type_=sa.Enum(*old_values, name="state_agent_type"),
        postgresql_using="agent_type::text::state_agent_type",
    )
    op.execute("DROP TYPE state_agent_type_old;")
