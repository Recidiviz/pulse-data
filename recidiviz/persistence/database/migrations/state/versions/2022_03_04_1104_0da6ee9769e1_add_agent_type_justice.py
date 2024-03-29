# pylint: skip-file
"""add_agent_type_justice

Revision ID: 0da6ee9769e1
Revises: 42f3b1facfdd
Create Date: 2022-03-04 11:04:04.546619

"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0da6ee9769e1"
down_revision = "42f3b1facfdd"
branch_labels = None
depends_on = None

# Without new value
old_values = [
    "PRESENT_WITHOUT_INFO",
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "PAROLE_BOARD_MEMBER",
    "SUPERVISION_OFFICER",
    "UNIT_SUPERVISOR",
    "INTERNAL_UNKNOWN",
]

# With new value
new_values = [
    "PRESENT_WITHOUT_INFO",
    "CORRECTIONAL_OFFICER",
    "JUDGE",
    "PAROLE_BOARD_MEMBER",
    "SUPERVISION_OFFICER",
    "UNIT_SUPERVISOR",
    "INTERNAL_UNKNOWN",
    "JUSTICE",
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
